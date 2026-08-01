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

from datetime import datetime, timezone

from task_core.types import OutputColumn, PipelineContractError, get_pipeline_spec
from task_core.table_adapters import get_table_adapter


def apply_db_updated_at(payload, spec, run_started_at=None):
    # Technical-column logic stays outside the adapters, here in the
    # orchestration layer, operating on the already-built,
    # already-engine-neutral DbPayload (.columns/.rows/.type_overrides are
    # plain lists and dicts regardless of which adapter produced them).
    # Never needed engine-specific knowledge in the first place -- it was
    # always operating on task_core's own generic payload shape.
    if not spec.db_updated_at:
        return

    # Added after db_contract is applied (to_db_payload already ran it),
    # since db_contract's cut(*source_cols) would otherwise silently drop
    # a column added upstream that it doesn't know about. This also keeps
    # Excel export (which uses tbl directly, before this point) unaffected.
    column_name = spec.db_updated_at if isinstance(spec.db_updated_at, str) else 'etl_updated_at'

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

    payload.framework_columns = tuple(payload.framework_columns) + (
        OutputColumn(column_name, 'TIMESTAMPTZ', nullable=False),
    )


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
    return _build_db_payload_with_spec(
        task_cls, tbl, get_pipeline_spec(task_cls), pg_schema, run_started_at=run_started_at,
    )
