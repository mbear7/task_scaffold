"""COPY loader: spool preparation and DBAPI transport.

Layering (ADR 0011 §Implementation sequence):

    db/publish  ->  db/copy  ->  db/spool_io   -> db/spool_format
                                 db/copytext   -> db/values

`db/copy` is one level *below* `db/publish`. It knows nothing about
live-table publication, advisory locks, transaction boundaries, or the
`DbPublisher` class. It cannot import `db/publish`, begin/commit/roll back
transactions, or create an engine or connection. Its only database operation
opens a cursor on the SQLAlchemy connection supplied by the publisher and
streams a prepared spool through psycopg2 `copy_expert()` into an already
created staging table.

The module is deliberately name-clean of the forbidden transaction and
engine operations so the architecture tests can enforce that ownership
boundary.

What was one file until 0.7.4 is now four. This one orchestrates: it decides
one-pass versus two-pass preparation, drives the source through the
serializers, and performs the transport. The pieces it drives live in
`spool_format.py` (what a spool is), `copytext.py` (how values become COPY
text) and `spool_io.py` (handles, encryption, cleanup).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from task_core.db.copytext import (
    _compile_declared_copy_field_writers,
    _compile_inferred_copy_field_serializers,
    _write_compiled_declared_copytext_row,
    _write_compiled_inferred_copytext_row,
)
from task_core.db.policies import CopyLoadPolicy
from task_core.db.spool_format import (
    read_neutral_preamble,
    read_neutral_row,
    write_neutral_preamble,
    write_neutral_row,
    write_neutral_terminator,
)
from task_core.db.spool_io import (
    PreparedCopySource,
    SpoolIdentity,
    _close_preserving_primary,
    cleanup_spool_paths,
    open_spool_for_read,
    open_spool_for_write,
)
from task_core.db.values import (
    DbPublishError,
    ResolvedColumn,
    _InferenceStreamState,
    _normalize_value,
    _resolve_override,
)
from task_core.types import find_duplicates

log = logging.getLogger(__name__)


# --- DBAPI COPY transport ----------------------------------------------


def _build_copy_sql(conn, staging_table, columns: Sequence[ResolvedColumn]) -> str:
    """Build one defensively quoted PostgreSQL COPY FROM STDIN statement.

    The serializer writes text rows with tab delimiters and ``\\N`` as the
    NULL marker. The SQL must describe that exact grammar; changing one side
    without the other would silently corrupt values rather than merely reduce
    performance.
    """
    dialect = getattr(conn, 'dialect', None)
    if dialect is None or getattr(dialect, 'name', None) != 'postgresql':
        raise DbPublishError(
            'COPY loader requires a SQLAlchemy PostgreSQL connection'
        )
    if getattr(dialect, 'driver', None) not in (None, 'psycopg2'):
        raise DbPublishError(
            f"COPY loader requires SQLAlchemy's psycopg2 dialect, got "
            f"{getattr(dialect, 'driver', None)!r}"
        )

    preparer = dialect.identifier_preparer
    table_sql = preparer.format_table(staging_table)
    column_sql = ', '.join(preparer.quote(column.name) for column in columns)
    # SQL text E'\\\\N' represents the two-character COPY NULL marker
    # backslash + N. The input serializer emits that marker only for None;
    # literal text ``\\N`` is escaped as ``\\\\N`` in the row body.
    return (
        f'COPY {table_sql} ({column_sql}) FROM STDIN '
        "WITH (FORMAT text, DELIMITER E'\\t', NULL E'\\\\N', ENCODING 'UTF8')"
    )


def _get_driver_connection(conn):
    """Return the existing psycopg2 connection behind SQLAlchemy.

    No engine or second connection is created. SQLAlchemy 2.x exposes the
    DBAPI connection as ``Connection.connection.driver_connection``. The
    legacy ``.connection`` fallback keeps the helper usable with older test
    doubles and SQLAlchemy 2.0 proxy shapes without importing psycopg2.
    """
    proxy = getattr(conn, 'connection', None)
    if proxy is None:
        raise DbPublishError(
            'COPY loader could not access the SQLAlchemy DBAPI connection'
        )
    raw = getattr(proxy, 'driver_connection', None)
    if raw is None:
        raw = getattr(proxy, 'connection', None)
    if raw is None or not callable(getattr(raw, 'cursor', None)):
        raise DbPublishError(
            'COPY loader could not access the existing psycopg2 connection'
        )
    return raw


def load_copy_into_staging(conn, staging_table, prepared, _chunk_size=None) -> int:
    """Stream one prepared COPY-text spool into an existing staging table.

    The caller owns the SQLAlchemy transaction, staging DDL, verification,
    commit/rollback and spool cleanup. This function opens only a cursor on
    the existing DBAPI connection and returns the exact logical row count
    captured during spool preparation.
    """
    if not isinstance(prepared, PreparedCopySource):
        raise DbPublishError(
            f'COPY loader requires PreparedCopySource, got '
            f'{type(prepared).__name__}'
        )
    copy_sql = _build_copy_sql(conn, staging_table, prepared.columns)
    raw = _get_driver_connection(conn)
    cursor = raw.cursor()
    primary_error = None
    try:
        with prepared.open_reader() as reader:
            # psycopg2 copy_expert() pulls from the file-like object in bounded
            # chunks. Reading through authenticated EOF is what verifies the
            # final AES-GCM tag; no decrypted temporary file is created.
            cursor.copy_expert(copy_sql, reader, size=prepared.buffer_bytes)
        return prepared.row_count
    except BaseException as exc:
        primary_error = exc
        # Bypassing SQLAlchemy's execute() layer means a psycopg2 exception is
        # not automatically classified by the dialect or used to invalidate
        # the SQLAlchemy Connection. Ask the dialect as well as checking the
        # driver's closed flag: psycopg2 does not set ``closed`` for every
        # disconnect shape. Invalidating here preserves DbPublisher's fatal
        # no-reconnect rule for the advisory-lock-owning session.
        disconnected = bool(getattr(raw, 'closed', 0))
        if not disconnected and isinstance(exc, Exception):
            is_disconnect = getattr(conn.dialect, 'is_disconnect', None)
            if callable(is_disconnect):
                try:
                    disconnected = bool(is_disconnect(exc, raw, cursor))
                except BaseException:
                    log.exception(
                        'secondary error while classifying a COPY connection '
                        'failure; preserving primary exception'
                    )
        if disconnected:
            invalidate = getattr(conn, 'invalidate', None)
            if callable(invalidate):
                try:
                    invalidate(exc)
                except BaseException:
                    log.exception(
                        'secondary error while invalidating a lost COPY '
                        'connection; preserving primary exception'
                    )
        raise
    finally:
        try:
            cursor.close()
        except BaseException:
            if primary_error is None:
                raise
            log.exception(
                'secondary error while closing psycopg2 COPY cursor; '
                'preserving primary exception'
            )


def _normalize_copy_row(
    row: Sequence[Any],
    *,
    expected_width: int,
) -> tuple[Any, ...]:
    if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
        raise DbPublishError(
            f'row source yielded non-sequence {type(row).__name__}'
        )
    normalized = tuple(_normalize_value(value) for value in row)
    if len(normalized) != expected_width:
        raise DbPublishError(
            f'row width {len(normalized)} does not match column '
            f'count {expected_width}'
        )
    return normalized


def _prepare_declared_copy_source_one_pass(
    *,
    row_source: Iterable[Sequence[Any]],
    source_columns: tuple[str, ...],
    resolved_columns: tuple[ResolvedColumn, ...],
    identity: SpoolIdentity,
    directory: Path,
    policy: CopyLoadPolicy,
) -> PreparedCopySource:
    """Validate and serialize a declared COPY payload in one traversal.

    A declared schema already supplies the target types and wire order, so a
    type-neutral spool would only add a full write/read cycle. The final
    COPY-text spool is therefore written directly while each normalized row
    is validated against the shared declared-value kernel.
    """
    copytext_path: Path | None = None
    try:
        serializers = _compile_declared_copy_field_writers(
            source_columns,
            resolved_columns,
            identity.target_table,
        )
        handle = open_spool_for_write(
            directory,
            stage='copytext',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            encrypt=policy.encrypt_spools,
        )
        copytext_path = handle.path
        row_count = 0
        output_buffer = bytearray()
        with _close_preserving_primary(
            handle.stream,
            description=f'COPY-text spool {copytext_path}',
        ) as copytext_fp:
            for row in row_source:
                row_count += 1
                _write_compiled_declared_copytext_row(
                    copytext_fp,
                    row,
                    serializers,
                    row_count,
                    output_buffer,
                    expected_width=len(source_columns),
                )
        return PreparedCopySource(
            path=copytext_path,
            columns=resolved_columns,
            row_count=row_count,
            spool_bytes=copytext_path.stat().st_size,
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            protection=handle.protection,
            _key=handle.key,
        )
    except BaseException:
        if copytext_path is not None:
            cleanup_spool_paths([copytext_path])
        raise


# --- Orchestrator ------------------------------------------------------

def prepare_copy_source(
    *,
    row_source: Iterable[Sequence[Any]],
    columns: Sequence[str],
    declared_schema: Sequence[ResolvedColumn] | None,
    identity: SpoolIdentity,
    directory: Path,
    policy: CopyLoadPolicy | None = None,
    framework_columns: Sequence[ResolvedColumn] = (),
    type_overrides: Mapping[str, Any] | None = None,
    not_null_columns: Sequence[str] = (),
) -> PreparedCopySource:
    """Prepare a final COPY-text spool from a positional row source.

    Declared mode knows the target schema before traversal and therefore
    validates and serializes directly into the final spool in one pass.
    Inferred mode writes a type-neutral spool while accumulating schema state,
    then replays it once through the resolved target-aware serializer.

    `row_source` must yield sequences whose positional order matches
    `columns`. Each source value is normalized once through the same kernel as
    INSERT. Declared and source column sets must match, but their order may
    differ; a positional serializer compiled once before the row loop emits
    fields in resolved-schema order without rebuilding per-row dictionaries.

    `framework_columns` pins the resolved type and nullability of technical
    columns whose value is caller-supplied and constant (e.g.
    `etl_updated_at`) after inference completes. Inferred user columns now
    distinguish naive from timezone-aware datetimes directly; framework
    columns remain pinned because their contract is framework-owned rather
    than inferred from task data. In declared mode this override is a no-op
    (declared columns already carry their pinned type), but the same
    framework tuple is accepted and validated for symmetry with the caller.

    On success: returns an immutable `PreparedCopySource` carrying the final
    spool, resolved columns, exact row count and on-disk byte count. An
    inferred-mode neutral spool is removed before success is returned. On any
    exception, deletion of every current-run spool is attempted with bounded
    retries; a cleanup failure is logged without replacing the primary
    exception and may be reaped by a later positively-owned cleanup pass.

    Since 0.6.6 this preparation result is consumed by the same-connection
    psycopg2 COPY transport. The caller still owns transaction boundaries,
    staging DDL, publication and final spool cleanup.
    """
    # --- Input validation (all before any file is created) ------------
    if policy is None:
        policy = CopyLoadPolicy()
    if not isinstance(policy, CopyLoadPolicy):
        raise DbPublishError(
            f'policy must be a CopyLoadPolicy or None, got {type(policy).__name__}'
        )
    if not isinstance(identity, SpoolIdentity):
        raise DbPublishError(
            f'identity must be a SpoolIdentity, got {type(identity).__name__}'
        )
    if not isinstance(directory, Path):
        raise DbPublishError(
            f'directory must be a pathlib.Path, got {type(directory).__name__}'
        )
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
        raise DbPublishError(
            f'columns must be a Sequence of str, got {type(columns).__name__}'
        )
    columns_tuple = tuple(columns)
    if not columns_tuple:
        raise DbPublishError('columns must be non-empty')
    for name in columns_tuple:
        if not isinstance(name, str) or not name:
            raise DbPublishError(
                f'column names must be non-empty strings, got {name!r}'
            )
    # Duplicate names would make source-position lookup ambiguous and could
    # route a resolved output column to the wrong positional value. The
    # caller-side RowProjection already blocks duplicates upstream, so
    # this is defensive symmetry rather than the primary guard.
    duplicates = find_duplicates(columns_tuple)
    if duplicates:
        raise DbPublishError(
            f'columns must not contain duplicate names, got duplicates: {duplicates!r}'
        )
    if declared_schema is not None:
        declared_tuple = tuple(declared_schema)
        for col in declared_tuple:
            if not isinstance(col, ResolvedColumn):
                raise DbPublishError(
                    f'declared_schema entries must be ResolvedColumn, '
                    f'got {type(col).__name__}'
                )
        declared_names = [c.name for c in declared_tuple]
        declared_duplicates = find_duplicates(declared_names)
        if declared_duplicates:
            raise DbPublishError(
                f'declared_schema contains duplicate column names: {declared_duplicates!r}'
            )
        missing = [name for name in declared_names if name not in columns_tuple]
        unexpected = [name for name in columns_tuple if name not in set(declared_names)]
        if missing or unexpected:
            raise DbPublishError(
                f'declared_schema columns do not match source columns; '
                f'missing={missing!r}, unexpected={unexpected!r}'
            )
    framework_tuple = tuple(framework_columns)
    for col in framework_tuple:
        if not isinstance(col, ResolvedColumn):
            raise DbPublishError(
                f'framework_columns entries must be ResolvedColumn, '
                f'got {type(col).__name__}'
            )
    if framework_tuple:
        column_set = set(columns_tuple)
        unknown = [c.name for c in framework_tuple if c.name not in column_set]
        if unknown:
            raise DbPublishError(
                f'framework_columns include names not present in columns: {unknown!r}'
            )
    if declared_schema is not None and type_overrides is not None:
        raise DbPublishError('declared_schema cannot be combined with type_overrides')
    if declared_schema is not None and tuple(not_null_columns):
        raise DbPublishError('declared_schema cannot be combined with not_null_columns')

    if type_overrides is not None and not isinstance(type_overrides, Mapping):
        raise DbPublishError(
            f'type_overrides must be a mapping or None, got {type(type_overrides).__name__}'
        )
    overrides = dict(type_overrides or {})
    unknown_overrides = [name for name in overrides if name not in columns_tuple]
    if unknown_overrides:
        raise DbPublishError(
            f'type_overrides contains column(s) not present in the output: {unknown_overrides!r}'
        )

    if not isinstance(not_null_columns, Sequence) or isinstance(
        not_null_columns, (str, bytes)
    ):
        raise DbPublishError('not_null_columns must be a sequence of strings')
    not_null = tuple(not_null_columns)
    if not all(isinstance(name, str) for name in not_null):
        raise DbPublishError('not_null_columns must contain only strings')
    not_null_duplicates = find_duplicates(not_null)
    if not_null_duplicates:
        raise DbPublishError(
            f'not_null_columns contains duplicate column(s): {not_null_duplicates!r}'
        )
    unknown_not_null = [name for name in not_null if name not in columns_tuple]
    if unknown_not_null:
        raise DbPublishError(
            f'not_null_columns contains column(s) not present in the output: {unknown_not_null!r}'
        )

    if row_source is None:
        raise DbPublishError('row_source must not be None')

    # --- Execute the selected preparation path -----------------------

    if declared_schema is not None:
        return _prepare_declared_copy_source_one_pass(
            row_source=row_source,
            source_columns=columns_tuple,
            resolved_columns=declared_tuple,
            identity=identity,
            directory=directory,
            policy=policy,
        )

    # Inferred mode still needs two passes: the target schema is unknown
    # until every sampled/observed family has been resolved. Pass 1 writes
    # normalized values to the neutral spool while feeding inference. Pass 2
    # uses a positional serializer compiled once from the resolved schema.
    neutral_path: Path | None = None
    copytext_path: Path | None = None
    try:
        state = _InferenceStreamState(len(columns_tuple))
        source_row_count = 0
        neutral_handle = open_spool_for_write(
            directory,
            stage='neutral',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            encrypt=policy.encrypt_spools,
        )
        neutral_path = neutral_handle.path
        with _close_preserving_primary(
            neutral_handle.stream,
            description=f'neutral COPY spool {neutral_path}',
        ) as neutral_fp:
            write_neutral_preamble(neutral_fp, columns=columns_tuple)
            for row in row_source:
                values = _normalize_copy_row(
                    row,
                    expected_width=len(columns_tuple),
                )
                source_row_count += 1
                state.feed_row(values)
                write_neutral_row(
                    neutral_fp,
                    values,
                    expected_width=len(columns_tuple),
                )
            write_neutral_terminator(neutral_fp)

        resolved_types = state.resolve()
        framework_by_name = {c.name: c for c in framework_tuple}
        resolved_list = []
        for index, name in enumerate(columns_tuple):
            framework = framework_by_name.get(name)
            if framework is not None:
                resolved_list.append(framework)
                continue
            override = _resolve_override(overrides.get(name))
            resolved_list.append(ResolvedColumn(
                name=name,
                type=override if override is not None else resolved_types[index],
                nullable=name not in not_null,
            ))
        resolved_columns = tuple(resolved_list)
        serializers = _compile_inferred_copy_field_serializers(
            columns_tuple,
            resolved_columns,
            identity.target_table,
        )

        copytext_handle = open_spool_for_write(
            directory,
            stage='copytext',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            encrypt=policy.encrypt_spools,
        )
        copytext_path = copytext_handle.path
        output_buffer = bytearray()
        with _close_preserving_primary(
            copytext_handle.stream,
            description=f'COPY-text spool {copytext_path}',
        ) as copytext_fp:
            neutral_read = open_spool_for_read(
                neutral_path,
                identity=identity,
                stage='neutral',
                buffer_bytes=policy.buffer_bytes,
                key=neutral_handle.key,
            )
            with _close_preserving_primary(
                neutral_read,
                description=f'neutral COPY spool reader {neutral_path}',
            ):
                preamble = read_neutral_preamble(neutral_read)
                if preamble != columns_tuple:
                    raise DbPublishError(
                        f'neutral spool preamble columns {preamble!r} do '
                        f'not match expected {columns_tuple!r}'
                    )
                row_number = 0
                while True:
                    values = read_neutral_row(
                        neutral_read,
                        len(columns_tuple),
                    )
                    if values is None:
                        break
                    row_number += 1
                    _write_compiled_inferred_copytext_row(
                        copytext_fp,
                        values,
                        serializers,
                        row_number,
                        output_buffer,
                    )

        failed_neutral_cleanup = cleanup_spool_paths([neutral_path])
        if failed_neutral_cleanup:
            raise DbPublishError(
                f'could not remove completed neutral COPY spool(s): '
                f'{failed_neutral_cleanup!r}'
            )
        return PreparedCopySource(
            path=copytext_path,
            columns=resolved_columns,
            row_count=source_row_count,
            spool_bytes=copytext_path.stat().st_size,
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            protection=copytext_handle.protection,
            _key=copytext_handle.key,
        )
    except BaseException:
        to_cleanup = [p for p in (neutral_path, copytext_path) if p is not None]
        if to_cleanup:
            cleanup_spool_paths(to_cleanup)
        raise


__all__ = [
    'prepare_copy_source',
    'load_copy_into_staging',
]
