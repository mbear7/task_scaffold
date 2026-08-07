# -*- coding: utf-8 -*-
"""
Level 2: output preparation (Excel export, DB payload). Depends on
types.py (PipelineSpec, get_pipeline_spec, PipelineContractError) and
table_adapters.py (get_table_adapter -- a lateral, acyclic level-2
import; table_adapters.py sits below export.py within level 2 and must
never import anything back from here). Must NEVER import runner.py.

export_excel()/build_db_payload() keep their original public signatures
permanently, for standalone/external callers -- task_core/__init__.py
already commits to resolving every facade name exactly as before.
run_pipelines() does not call these public wrappers: it calls the
private, spec-aware helpers below with the PipelineSpec captured before
task_cls.run(), not re-derived after. This preserves facade compatibility
while preventing runtime cls.spec reassignment from changing output
targets or any other static publish configuration -- db_table,
excel_name, db_type_overrides, db_not_null_columns, output_schema,
db_table_id_pix, and the resolved adapter
are all governed exclusively by the spec captured before .run() executes,
enforced structurally, not by convention. The one deliberately dynamic
exception is db_contract, via the get_dynamic_db_contract hook below.
"""

from dataclasses import replace
from datetime import datetime, timezone
import logging
import os

from task_core.types import OutputColumn, PipelineContractError, get_pipeline_spec
from task_core.table_adapters import get_table_adapter


log = logging.getLogger(__name__)


def _build_framework_columns(spec):
    """Framework-column tuple implied by ``spec`` -- currently just the
    single ``db_updated_at`` column when enabled, empty otherwise.

    Shared between the INSERT path (``apply_db_updated_at``) and the
    COPY path (``_prepare_copy_source_for_pipeline``) so the two derive
    the exact same tuple from the same spec. Duplicating the "read
    ``spec.db_updated_at``, pick the name" rule is a documented
    one-source-of-truth boundary, so a single producer here is required
    -- not merely nice.
    """
    if not spec.db_updated_at:
        return ()
    column_name = spec.db_updated_at if isinstance(spec.db_updated_at, str) else 'etl_updated_at'
    return (OutputColumn(column_name, 'TIMESTAMPTZ', nullable=False),)


def apply_db_updated_at(payload, spec, run_started_at=None):
    # Technical-column logic stays outside the adapters, here in the
    # orchestration layer, operating on the already-built,
    # already-engine-neutral DbPayload (.columns/.rows/.type_overrides are
    # plain lists and dicts regardless of which adapter produced them).
    # Never needed engine-specific knowledge in the first place -- it was
    # always operating on task_core's own generic payload shape.
    framework = _build_framework_columns(spec)
    if not framework:
        return
    # Only one framework column today; kept as a tuple so a future
    # addition passes through _build_framework_columns without a new
    # mechanism. Destructure here to keep the append/append/append
    # sequence readable.
    (framework_column,) = framework
    column_name = framework_column.name

    # Added after db_contract is applied (to_db_payload already ran it),
    # since db_contract's cut(*source_cols) would otherwise silently drop
    # a column added upstream that it doesn't know about. This also keeps
    # Excel export (which uses tbl directly, before this point) unaffected.
    if column_name in payload.columns:
        raise PipelineContractError(
            f'{spec.db_table!r}: db_updated_at column {column_name!r} already exists in the '
            'output; rename the column or disable db_updated_at to avoid a silent overwrite'
        )

    if run_started_at is None:
        run_started_at = datetime.now(timezone.utc)

    payload.columns.append(column_name)
    for row in payload.rows:
        row[column_name] = run_started_at

    payload.framework_columns = tuple(payload.framework_columns) + (framework_column,)


def _export_excel_with_spec(tbl, spec):
    if not spec.excel_name:
        return None
    adapter = get_table_adapter(spec.table_adapter)
    adapter.to_excel(tbl, spec.excel_name)
    return spec.excel_name


def export_excel(task_cls, tbl):
    return _export_excel_with_spec(tbl, get_pipeline_spec(task_cls))


def _build_db_payload_with_spec(task_cls, tbl, spec, pg_schema, *, run_started_at=None):
    if not spec.db_table:
        return None
    adapter = get_table_adapter(spec.table_adapter)

    # get_dynamic_db_contract is the only supported dynamic publish-contract
    # mechanism -- resolved here, against the pipeline's actual output,
    # while every other field used below comes from spec, captured before
    # .run() executed. An explicit getattr check for one named method, not
    # a full spec re-fetch that could pick up an unrelated cls.spec
    # reassignment.
    dynamic_contract_fn = getattr(task_cls, 'get_dynamic_db_contract', None)
    if spec.output_schema is not None and callable(dynamic_contract_fn):
        raise PipelineContractError(
            f'{spec.db_table!r}: output_schema cannot be combined with '
            'get_dynamic_db_contract(); declared schemas require a static final column contract'
        )
    db_contract = dynamic_contract_fn(tbl) if callable(dynamic_contract_fn) else spec.db_contract

    payload = adapter.to_db_payload(
        tbl,
        table_name=spec.db_table,
        schema=pg_schema,
        type_overrides=spec.db_type_overrides,
        db_contract=db_contract,
        not_null_columns=spec.db_not_null_columns,
        output_schema=spec.output_schema,
        publication_strategy=spec.db_publication_strategy or 'replace',
        db_loader=spec.db_loader,
        db_table_id_pix=spec.db_table_id_pix,
    )
    apply_db_updated_at(payload, spec, run_started_at)
    return payload


def build_db_payload(task_cls, tbl, pg_schema, *, run_started_at=None):
    spec = get_pipeline_spec(task_cls)
    if spec.db_loader == 'copy':
        return _build_copy_payload_with_spec(
            task_cls, tbl, spec, pg_schema, run_started_at=run_started_at,
        )
    return _build_db_payload_with_spec(
        task_cls, tbl, spec, pg_schema, run_started_at=run_started_at,
    )


def _compose_copy_row_source(task_cls, tbl, spec, *, run_started_at):
    """Build the final logical one-shot row source used by COPY tests and
    the publisher path.

    COPY deliberately supports only the static ``db_contract`` from the
    captured PipelineSpec. ``get_dynamic_db_contract()`` may execute
    arbitrary task code and traverse the lazy table before the one-shot
    source is claimed, so the combination is rejected structurally and
    repeated defensively here for direct helper callers.
    """
    from task_core.db.payload import (
        RowProjection,
        _ProjectedRowSource,
    )

    dynamic_contract_fn = getattr(task_cls, 'get_dynamic_db_contract', None)
    if callable(dynamic_contract_fn):
        raise PipelineContractError(
            f'{getattr(task_cls, "__name__", repr(task_cls))}: '
            f"db_loader='copy' cannot be combined with "
            f'get_dynamic_db_contract(); COPY resolves its output columns '
            f'once before consuming the one-shot source'
        )

    adapter = get_table_adapter(spec.table_adapter)
    source_columns, raw_source = adapter.to_row_source(tbl)
    framework_columns = _build_framework_columns(spec)
    projection = RowProjection.build(
        source_columns,
        db_contract=spec.db_contract,
        framework_columns=framework_columns,
        run_started_at=run_started_at,
    )
    return projection, _ProjectedRowSource(raw_source, projection), framework_columns


def _build_copy_payload_with_spec(
    task_cls, tbl, spec, pg_schema, *, run_started_at=None,
):
    """Build a COPY DbPayload without materializing rows.

    Spool preparation remains inside ``DbPublisher.publish()`` so the
    publisher owns the complete preparation lifecycle: schema resolution,
    staging DDL, selected transport, verification, comment, commit, and
    cleanup of the final spool on every exit path.
    """
    from task_core.db.payload import DbPayload

    if not spec.db_table:
        return None
    if run_started_at is None:
        run_started_at = datetime.now(timezone.utc)

    projection, projected_source, framework_columns = _compose_copy_row_source(
        task_cls, tbl, spec, run_started_at=run_started_at,
    )
    return DbPayload(
        table_name=spec.db_table,
        schema=pg_schema,
        columns=list(projection.output_columns),
        rows=None,
        type_overrides=spec.db_type_overrides,
        db_table_id_pix=spec.db_table_id_pix,
        not_null_columns=tuple(spec.db_not_null_columns or ()),
        output_schema=(
            tuple(spec.output_schema) if spec.output_schema is not None else None
        ),
        framework_columns=framework_columns,
        publication_strategy=spec.db_publication_strategy or 'replace',
        db_loader='copy',
        row_source=projected_source,
        copy_spool_encryption=spec.db_copy_spool_encryption,
    )


def _prepare_copy_source_for_pipeline(
    task_cls, tbl, spec, pg_schema, *, task_name, run_started_at=None, policy=None,
):
    """Compose the direct COPY preparation chain for one pipeline: adapter
    row-source
    reshaped by db_contract + framework columns, spooled through
    prepare_copy_source() into a target-aware spool container whose plaintext
    body is PostgreSQL COPY text.

    Sits at level 2 alongside _build_db_payload_with_spec because both
    turn (task_cls, out_tbl, spec, pg_schema) into a publish-ready
    artefact; runner.py stays engine-neutral by delegating both.

    This remains a direct preparation helper for tests and diagnostics.
    The runner path builds a one-shot DbPayload and lets
    ``DbPublisher.publish()`` own spool preparation and database transport.

    Returns a PreparedCopySource. The caller owns its spool path from
    the moment this returns.
    """
    # Deferred imports keep export.py's module-level dependency surface
    # minimal (db/copy is level 2 alongside export; the imports are
    # acyclic but noisy at the top of the file) and mirror the
    # dependency direction the tests enforce: nothing at level 2 pulls
    # in db/copy unconditionally.
    from task_core.db.copy import prepare_copy_source
    from task_core.db.policies import CopyLoadPolicy
    from task_core.db.spool_io import SpoolIdentity
    from task_core.db.spool_format import resolve_spool_directory
    from task_core.db.values import ResolvedColumn, _resolve_declared_type

    if not spec.db_table:
        raise PipelineContractError(
            '_prepare_copy_source_for_pipeline requires spec.db_table'
        )

    if policy is None:
        policy = CopyLoadPolicy()
    if spec.db_copy_spool_encryption is not None:
        policy = replace(
            policy, encrypt_spools=spec.db_copy_spool_encryption,
        )
        if not policy.encrypt_spools:
            log.warning(
                'COPY spool encryption disabled by PipelineSpec for pipeline %s',
                getattr(task_cls, '__name__', repr(task_cls)),
            )
    if run_started_at is None:
        run_started_at = datetime.now(timezone.utc)

    projection, projected_source, framework_columns = _compose_copy_row_source(
        task_cls, tbl, spec, run_started_at=run_started_at,
    )

    # Framework columns as ResolvedColumn using the same OutputColumn
    # -> concrete TypeEngine converter db/values uses on the INSERT
    # path, so aware timestamps stay aware regardless of loader. Built
    # unconditionally: declared mode folds them into declared_columns
    # (below), inferred mode hands them to prepare_copy_source as the
    # type-pin argument.
    resolved_framework_columns = tuple(
        ResolvedColumn(fw.name, _resolve_declared_type(fw.type), fw.nullable)
        for fw in framework_columns
    )

    if spec.output_schema is not None:
        # Declared mode: convert OutputColumn (which accepts a SA type
        # instance, class or string alias) into ResolvedColumn
        # (concrete TypeEngine) exactly the way db/values does on the
        # INSERT path -- same converter, so declared-mode value
        # validation stays byte-identical between the two loaders.
        # Framework columns are appended in projection order.
        declared_columns = tuple(
            ResolvedColumn(c.name, _resolve_declared_type(c.type), c.nullable)
            for c in spec.output_schema
        ) + resolved_framework_columns
    else:
        # Inferred mode: prepare_copy_source runs its own accumulator
        # on the neutral pass and returns the resolved types. Framework
        # column types are pinned by the framework_columns kwarg below,
        # not by declared_columns.
        declared_columns = None

    identity = SpoolIdentity(
        task=task_name,
        target_schema=pg_schema or '<default-schema>',
        target_table=spec.db_table,
        run_start_utc=run_started_at,
        pid=os.getpid(),
    )
    directory = resolve_spool_directory(policy)

    # prepare_copy_source iterates row_source with a plain `for`, so
    # it needs an iterable; _ProjectedRowSource exposes iteration via
    # its explicit iter_rows() method (DbRowSource protocol), not
    # __iter__. Handing the generator over rather than the wrapper
    # keeps prepare_copy_source unaware of the protocol.
    return prepare_copy_source(
        row_source=projected_source.iter_rows(),
        columns=projection.output_columns,
        declared_schema=declared_columns,
        identity=identity,
        directory=directory,
        policy=policy,
        framework_columns=resolved_framework_columns,
        type_overrides=spec.db_type_overrides,
        not_null_columns=spec.db_not_null_columns or (),
    )
