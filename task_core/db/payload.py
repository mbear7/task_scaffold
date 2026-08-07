# -*- coding: utf-8 -*-
"""What flows into publication: the payload shape and its constructors.

Split out of publish.py in 0.7.4. `DbPayload` is the one representation the
publisher accepts, `from_petl()` and `from_pandas()` are the two supported
ways to build it, and `RowProjection` + `_ProjectedRowSource` compose
db_contract renaming, column projection and framework columns into the final
logical row shape.

The projection is a row-source transformation rather than a dictionary-first
mutation, which ADR 0011 §Row-source contract requires so that COPY never
needs a whole-table `list[dict]`. A parity test asserts it produces
byte-identical output to the INSERT path.

Nothing here connects, publishes or locks.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import pandas as pd

from task_core.db.values import (
    DbPublishError,
    DbPublishInvariantError,
    _apply_db_contract_columns,
    _normalize_value,
    _validate_unique_columns,
)
from task_core.types import (
    DbRowSource,
    OutputColumn,
    find_duplicates,
    validate_db_loader,
    validate_payload_source_state,
    validate_publication_strategy,
)


@dataclass(frozen=True)
class DbTableResult:
    schema: str
    table_name: str
    full_name: str
    rows: int
    db_table_id_pix: Any | None = None


@dataclass
class DbPayload:
    table_name: str
    schema: str
    columns: list[str]
    # None means "this payload's rows are carried by row_source, not
    # materialized". Public COPY payloads use this state; INSERT payloads keep
    # the materialized mapping list. See ADR 0011 §Row-source contract.
    rows: list[dict[str, Any]] | None
    type_overrides: dict[str, Any] | None = None
    db_table_id_pix: Any | None = None
    not_null_columns: tuple[str, ...] = ()
    output_schema: tuple[OutputColumn, ...] | None = None
    framework_columns: tuple[OutputColumn, ...] = ()
    # 'replace' or 'refill'. Carried from PipelineSpec so publication can
    # select the mechanism without re-deriving it from the schema source.
    publication_strategy: str = 'replace'
    # 'insert' or 'copy'. Repeated on the payload so direct construction
    # cannot bypass loader/source-state validation. See ADR 0011.
    db_loader: str = 'insert'
    # Positional-stability rule (same one that added db_publication_strategy
    # after every 0.5.0 field): appended AFTER every 0.6.0 field so any
    # caller constructing DbPayload positionally keeps its previous
    # meaning. Only meaningful when db_loader='copy'; None on the insert
    # path. See ADR 0011 §Row-source contract for the state matrix.
    row_source: DbRowSource | None = None
    # Per-task override for CopyLoadPolicy.encrypt_spools. Appended after
    # every existing payload field so older positional construction keeps
    # its meaning. None inherits the publisher policy.
    copy_spool_encryption: bool | None = None

    def __post_init__(self):
        validate_publication_strategy(
            self.publication_strategy,
            output_schema=self.output_schema,
            field_name='publication_strategy',
            error_type=DbPublishError,
        )
        validate_db_loader(
            self.db_loader,
            field_name='db_loader',
            error_type=DbPublishError,
        )
        if self.copy_spool_encryption is not None and type(
            self.copy_spool_encryption
        ) is not bool:
            raise DbPublishError('copy_spool_encryption must be bool or None')
        # Enforced here, not only per-field, because the invariant is on
        # the pair (loader, rows/row_source) and no single field carries
        # enough context to check it alone. Runs after validate_db_loader
        # so an unknown loader is reported with its specific message
        # rather than the generic state-matrix one.
        validate_payload_source_state(
            self.db_loader, self.rows, self.row_source,
            error_type=DbPublishError,
        )


@dataclass(frozen=True)
class RowProjection:
    """A plan for turning positional rows from a DbRowSource into the
    final logical rows of a DbPayload -- db_contract renaming/projection
    plus framework columns (currently just the run-started-at timestamp),
    composed in one place.

    Immutable by construction: source_columns/output_columns are tuples,
    source_indices is a tuple, constants is a MappingProxyType. Built via
    ``RowProjection.build``; the constructor is intentionally low-level
    so tests can assemble one directly.

    Composition order matches the current INSERT path exactly (from_petl
    / from_pandas apply the contract inline, then export.apply_db_updated_at
    appends framework columns after) so an INSERT/COPY parity test can
    hold both to the same expected output.
    """

    source_columns: tuple[str, ...]
    output_columns: tuple[str, ...]
    # Per output column: index into the source row, or -1 for a column
    # whose value comes from ``constants`` instead. Stored as a plain
    # tuple of ints -- looked up once per row per column in the hot
    # iteration path, so cheap.
    source_indices: tuple[int, ...]
    # Output-column-index -> constant value. Used for framework columns
    # (the timestamp is computed once per payload/run and injected at
    # the right position). MappingProxyType so an accidental mutation
    # after construction raises rather than silently drifting.
    constants: Mapping[int, Any]

    def __post_init__(self):
        if len(self.output_columns) != len(self.source_indices):
            raise DbPublishInvariantError(
                f'RowProjection: output_columns has {len(self.output_columns)} '
                f'entries but source_indices has {len(self.source_indices)}'
            )
        source_width = len(self.source_columns)
        for out_idx, src_idx in enumerate(self.source_indices):
            if src_idx == -1:
                if out_idx not in self.constants:
                    raise DbPublishInvariantError(
                        f'RowProjection: output column {self.output_columns[out_idx]!r} '
                        f'at position {out_idx} has source_index=-1 but no constant'
                    )
            elif not (0 <= src_idx < source_width):
                raise DbPublishInvariantError(
                    f'RowProjection: output column {self.output_columns[out_idx]!r} '
                    f'at position {out_idx} points at source index {src_idx}, '
                    f'outside [0, {source_width})'
                )
        for const_idx in self.constants:
            if not (0 <= const_idx < len(self.output_columns)):
                raise DbPublishInvariantError(
                    f'RowProjection: constant at position {const_idx} is '
                    f'outside output range [0, {len(self.output_columns)})'
                )
            if self.source_indices[const_idx] != -1:
                # A constant sitting at a source-backed position would
                # be silently ignored by iter_rows() (the ternary picks
                # row[src_idx], never touching constants[out_idx]) --
                # exactly the class of silent projection drift these
                # checks exist to prevent.
                raise DbPublishInvariantError(
                    f'RowProjection: constant at position {const_idx} '
                    f'coincides with a source-backed position '
                    f'(source_indices[{const_idx}]='
                    f'{self.source_indices[const_idx]}); constants may '
                    f'only appear at positions where source_indices == -1'
                )

    @classmethod
    def build(cls, source_columns, *, db_contract, framework_columns, run_started_at):
        """Compose a projection from the same inputs the INSERT path uses.

        ``db_contract`` -- mapping source column names to output column
        names, or None/empty for identity. Applied first: the projected
        column list is either ``list(db_contract.values())`` in mapping
        order or ``list(source_columns)``.

        ``framework_columns`` -- tuple of ``OutputColumn`` for framework
        columns to append AFTER the contract projection, in order. The
        only framework column today is db_updated_at, so this is a
        one-element tuple in practice; kept general so a future framework
        column does not need a new mechanism. Each framework column
        currently receives the same ``run_started_at`` value (a single
        datetime), matching what apply_db_updated_at writes on the
        INSERT path.

        Framework column position is derived as
        ``len(contract_projected_columns)``, not hardcoded to "last" --
        it resolves to last today, but that fact is encoded rather than
        assumed, so a future non-terminal framework column would just
        pass a different position.
        """
        src_cols = tuple(str(c) for c in source_columns)

        # Collision validation mirrors the checks the INSERT path
        # already performs at db/values._stringify_and_reject_duplicate_columns
        # (source-name duplicates) and _apply_db_contract_columns
        # (target-name duplicates), plus PipelineSpec.__post_init__'s
        # rejection of framework-name collisions with the declared
        # schema. Without this, RowProjection.build would silently
        # accept configurations INSERT rejects, which is exactly the
        # semantic-drift class the parity test exists to catch.
        src_dupes = find_duplicates(src_cols)
        if src_dupes:
            raise DbPublishError(
                f'RowProjection.build: duplicate source column names: '
                f'{src_dupes!r}'
            )

        src_index = {name: i for i, name in enumerate(src_cols)}

        if db_contract:
            contract_pairs = list(db_contract.items())
            projected_cols = [target for _src, target in contract_pairs]
            projected_indices = []
            for src_name, _target in contract_pairs:
                if src_name not in src_index:
                    raise DbPublishError(
                        f'RowProjection.build: db_contract references source '
                        f'column {src_name!r} not in source_columns'
                    )
                projected_indices.append(src_index[src_name])

            target_dupes = find_duplicates(projected_cols)
            if target_dupes:
                raise DbPublishError(
                    f'RowProjection.build: db_contract maps multiple source '
                    f'columns to the same target name: {target_dupes!r}'
                )
        else:
            projected_cols = list(src_cols)
            projected_indices = list(range(len(src_cols)))

        fw_names = [fw.name for fw in framework_columns]
        fw_dupes = find_duplicates(fw_names)
        if fw_dupes:
            raise DbPublishError(
                f'RowProjection.build: duplicate framework column names: '
                f'{fw_dupes!r}'
            )

        projected_set = set(projected_cols)
        colliding = [name for name in fw_names if name in projected_set]
        if colliding:
            raise DbPublishError(
                f'RowProjection.build: framework column(s) collide with '
                f'projected column(s): {colliding!r}'
            )

        output_cols = list(projected_cols)
        source_indices = list(projected_indices)
        constants = {}
        for fw in framework_columns:
            # Position derived, not hardcoded -- see docstring.
            fw_position = len(output_cols)
            output_cols.append(fw.name)
            source_indices.append(-1)
            constants[fw_position] = run_started_at

        return cls(
            source_columns=src_cols,
            output_columns=tuple(output_cols),
            source_indices=tuple(source_indices),
            constants=MappingProxyType(dict(constants)),
        )


class _ProjectedRowSource:
    """DbRowSource decorator: wraps another DbRowSource and yields rows
    reshaped by a RowProjection (renamed, projected, framework-augmented).

    One-shot at its own layer, not merely by delegation. Delegating to
    the wrapped source's own one-shot guard would give a correct answer
    only if that guard existed and fired -- a hand-rolled DbRowSource
    that re-iterates would otherwise be silently accepted here.
    Row width from the underlying source is checked exactly against the
    projection's declared ``source_columns`` width -- an under- or
    over-wide row is a broken source, and the ADR's row-source contract
    requires exact width.
    """

    def __init__(self, source, projection):
        self._source = source
        self._projection = projection
        self._claimed = False

    def iter_rows(self):
        if self._claimed:
            raise DbPublishError(
                'projected row source already consumed -- one-shot per ADR 0011'
            )
        self._claimed = True
        projection = self._projection
        expected_width = len(projection.source_columns)
        source_indices = projection.source_indices
        constants = projection.constants
        for row_number, row in enumerate(self._source.iter_rows(), start=1):
            row = tuple(row)  # defensive: some sources yield generators/iterators
            if len(row) != expected_width:
                raise DbPublishError(
                    f'row {row_number} has width {len(row)}, expected '
                    f'{expected_width} to match declared source_columns'
                )
            yield tuple(
                constants[out_idx] if src_idx == -1 else row[src_idx]
                for out_idx, src_idx in enumerate(source_indices)
            )

def from_petl(
    tbl, *, table_name, schema, type_overrides=None, db_contract=None,
    not_null_columns=(), output_schema=None, db_table_id_pix=None,
    publication_strategy='replace', db_loader='insert',
):
    if isinstance(tbl, pd.DataFrame):
        raise DbPublishError(
            f'{table_name!r}: from_petl() received a pandas DataFrame, '
            'not a petl table -- use from_pandas() instead'
        )

    iterator = iter(tbl)

    try:
        header = next(iterator)
    except StopIteration:
        raise DbPublishError(f'PETL table for {table_name!r} is empty and has no header row')

    columns = [str(col) for col in header]
    if not columns:
        raise DbPublishError(f'{table_name!r}: no columns to publish -- the source table has no header')
    _validate_unique_columns(columns, table_name=table_name)

    rows = [
        {col: _normalize_value(value) for col, value in zip(columns, row, strict=True)}
        for row in iterator
    ]
    columns, rows = _apply_db_contract_columns(columns, rows, db_contract, table_name=table_name)

    return DbPayload(
        table_name=table_name,
        schema=schema,
        columns=columns,
        rows=rows,
        type_overrides=type_overrides,
        not_null_columns=tuple(not_null_columns or ()),
        output_schema=tuple(output_schema) if output_schema is not None else None,
        publication_strategy=publication_strategy,
        db_loader=db_loader,
        db_table_id_pix=db_table_id_pix,
    )



def from_pandas(
    df: pd.DataFrame, *, table_name, schema, type_overrides=None, db_contract=None,
    not_null_columns=(), output_schema=None, db_table_id_pix=None,
    publication_strategy='replace', db_loader='insert',
):
    if not isinstance(df, pd.DataFrame):
        raise DbPublishError(
            f'{table_name!r}: from_pandas() received a {type(df).__name__!r}, '
            'not a pandas DataFrame -- use from_petl() instead'
        )

    columns = [str(col) for col in df.columns]
    if not columns:
        raise DbPublishError(f'{table_name!r}: no columns to publish -- the source DataFrame has no columns')
    _validate_unique_columns(columns, table_name=table_name)

    prepared = df.copy()
    prepared = prepared.astype(object).where(pd.notna(prepared), None)

    rows = [
        {col: _normalize_value(value) for col, value in zip(columns, row, strict=True)}
        for row in prepared.itertuples(index=False, name=None)
    ]
    columns, rows = _apply_db_contract_columns(columns, rows, db_contract, table_name=table_name)

    return DbPayload(
        table_name=table_name,
        schema=schema,
        columns=columns,
        rows=rows,
        type_overrides=type_overrides,
        not_null_columns=tuple(not_null_columns or ()),
        output_schema=tuple(output_schema) if output_schema is not None else None,
        publication_strategy=publication_strategy,
        db_loader=db_loader,
        db_table_id_pix=db_table_id_pix,
    )
