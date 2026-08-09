"""
Level 0: shared vocabulary for the rest of the package. Zero imports from
anywhere else in task_core, or from db/publish.py -- not even under
TYPE_CHECKING. RunResult.source_fingerprints and
DbRunResult.committed_tables/published_tables are typed list[Any] rather
than list[SourceFingerprint]/list[DbTableResult] specifically to keep
this module import-free.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

# Runtime floor for the whole package, enforced at import time -- not just
# documented in the README. cleanup.py and runner.py use e.add_note() and
# the ExceptionGroup builtins (3.11+); on 3.10 nothing would fail until a
# cleanup error actually occurred, at which point add_note() would raise
# AttributeError *inside the exception handler*, masking the real failure
# -- precisely the failure mode the cleanup redesign exists to eliminate.
# The check lives here, not in __init__.py, because the facade is pure
# re-exports by standing rule, and types.py is the first module every
# import path through the facade loads anyway.
if sys.version_info < (3, 11):
    raise RuntimeError(
        f'task_core requires Python 3.11 or newer (found {sys.version.split()[0]}): '
        'cleanup-failure handling uses ExceptionGroup and BaseException.add_note(), '
        'which do not exist before 3.11.'
    )


def find_duplicates(items):
    """Values appearing more than once in items, in first-occurrence order,
    each listed once. The one shared implementation of an idiom previously
    hand-rolled in runner.py, binding.py, and db/publish.py -- order matters
    (error messages should report duplicates in the order the caller's data
    presents them), which is why this isn't a set operation."""
    seen = set()
    duplicates = []
    for item in items:
        if item in seen and item not in duplicates:
            duplicates.append(item)
        seen.add(item)
    return duplicates


# Closed set for PipelineSpec.table_adapter. None is legacy-only, kept
# solely so pre-existing specs (written before this field existed) never
# break. New pipelines -- petl or pandas -- should set this explicitly to
# 'petl' or 'pandas'; both are equally first-class choices, not a default
# plus an opt-in exception. Single source of truth: table_adapters.py
# imports this rather than re-declaring its own copy, so the two can't
# drift if a third engine ever lands. types.py's zero-imports rule is
# one-directional -- other modules importing *from* it is exactly how
# the layering already works.
VALID_TABLE_ADAPTERS = frozenset({None, 'petl', 'pandas'})


# The scaffold's portable identifier convention. Deliberately NOT a
# PostgreSQL rule -- this module is level 0 and engine-neutral, and every
# PostgreSQL-specific fact (the 63-byte limit, staging-name generation,
# normalization and collision rules) lives in db/publish.py instead.
#
# Lower case only, not [A-Za-z_]. Uppercase is exactly what makes an
# identifier case-fragile: SQLAlchemy quotes a mixed-case name to preserve
# it, quoting defeats PostgreSQL's folding, and 'Sales' then becomes a
# genuinely different table from 'sales'. Confirmed directly against the
# real postgresql dialect's identifier preparer:
#
#     'sales' -> sales          CREATE TABLE bsr.sales
#     'Sales' -> "Sales"        CREATE TABLE bsr."Sales"
#
# So this pattern means something worth the name 'portable': an identifier
# that behaves identically whether it is quoted or not, and therefore never
# needs quoting in hand-written SQL downstream. Confirmed directly that all
# 159 identifiers this project currently publishes -- 13 table names, 145
# column names, 1 schema -- already satisfy it, as do the source-state
# schema/table ('bsr', 'task_scaffold_meta'), so tightening from the
# previous [A-Za-z_] form broke nothing.
PORTABLE_IDENTIFIER_RE = re.compile(r'^[a-z_][a-z0-9_]*$')

# Published column names follow a wider convention than relation names, and
# deliberately do NOT claim portability in the sense defined above.
#
# Relation names are ours to choose. Column names usually are not: they come
# from analytical vocabulary, where `lev.1` and `metric.plan_2026` are
# ordinary rather than accidental. Rejecting them forced a rename of data the
# scaffold did not own.
#
# The dot costs the property the name 'portable' stands for. This:
#
#     select lev.1 from hr_ssch
#
# does not select the column -- it parses as a qualified reference. Reading a
# dotted column in hand-written SQL requires quoting it:
#
#     select "lev.1" from hr_ssch
#
# That is why the dot is added here instead of to PORTABLE_IDENTIFIER_RE:
# schemas, table names and generated relation names keep the stronger
# guarantee, and only columns take the weaker one. Every path where a column
# name reaches SQL already quotes it -- SQLAlchemy for DDL and INSERT, the
# dialect's identifier preparer for COPY, _quote_identifier() for refill's
# INSERT ... SELECT. See decisions/0014.
#
# Written as a repeated dotted segment rather than [a-z0-9_.]* so that '.lev',
# 'lev.' and 'lev..1' stay rejected: a dot separates parts, it is not just
# another permitted character.
PUBLISHED_COLUMN_RE = re.compile(r'^[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*$')

# How new data replaces old in a published table. Engine-neutral
# vocabulary, like PORTABLE_IDENTIFIER_RE above; the mechanics live in
# db/publish.py.
#
#   replace   drop the live table, rename staging into its place. One
#             database write, catalog-time lock. The target is a new
#             relation each run, so views, grants, indexes, ownership and
#             triggers do not survive.
#   refill    truncate the live table and insert from staging. Two database
#             writes and a lock held for a window proportional to row
#             count, in exchange for a stable OID -- so everything attached
#             to the table survives.
#
# 'partition' is deliberately absent rather than reserved-and-rejected: an
# accepted vocabulary value that raises NotImplementedError is its own
# small lie. It is added when it is built.
PUBLICATION_STRATEGIES = ('replace', 'refill')


def validate_publication_strategy(
    value, *, output_schema, allow_none=False,
    field_name='publication_strategy', error_type=ValueError,
):
    """Validate one publication-strategy value at any public boundary.

    ``PipelineSpec`` and direct ``DbPayload`` callers must enforce the same
    legal matrix. Keeping the rule here prevents the spec path and the direct
    publisher path from drifting apart.
    """
    if value is None:
        if allow_none:
            return None
        raise error_type(
            f'{field_name} must be one of {PUBLICATION_STRATEGIES}, got None'
        )

    if not isinstance(value, str) or value not in PUBLICATION_STRATEGIES:
        raise error_type(
            f'{field_name} must be one of {PUBLICATION_STRATEGIES}'
            f'{" or None" if allow_none else ""}, got {value!r}'
        )

    if value == 'refill' and output_schema is None:
        raise error_type(
            f"{field_name}='refill' requires output_schema. Refill truncates "
            "the live table and inserts into it, so the target's physical "
            "schema must be stable across runs; inferred schemas may change "
            "with the data."
        )

    return value


# How rows enter the staging table. Same engine-neutral vocabulary rule
# as PUBLICATION_STRATEGIES: only values backed by a complete transport
# appear here. INSERT consumes materialized mappings; COPY consumes a
# one-shot positional row source, prepares a bounded local spool, and
# streams it through PostgreSQL COPY FROM STDIN.
DB_LOADERS = ('insert', 'copy')


def validate_db_loader(
    value, *, field_name='db_loader', error_type=ValueError,
):
    """Validate one db-loader value at any public boundary.

    ``PipelineSpec`` and direct ``DbPayload`` callers enforce the same
    rule so the spec path and the direct publisher path cannot drift
    apart -- the same discipline validate_publication_strategy() applies
    above.
    """
    if not isinstance(value, str):
        raise error_type(
            f'{field_name} must be a str, got {type(value).__name__}'
        )
    if value not in DB_LOADERS:
        raise error_type(
            f'{field_name} must be one of {DB_LOADERS}, got {value!r}'
        )
    return value


# What a CSV reader does with a record whose field count is not the
# expected width. Same engine-neutral vocabulary rule as the two above.
#
#   strict            short -> error   long -> error
#   pad               short -> ''      long -> error
#   truncate          short -> error   long -> drop surplus
#   pad_or_truncate   short -> ''      long -> drop surplus
#
# 'pad' and 'truncate' are one-sided on purpose, and the asymmetry is the
# whole point of having four values rather than three: a task that expects
# ragged short rows still wants to hear about a row that is too long,
# because a surplus field usually means an unescaped delimiter rather than
# a sloppy writer. Making 'truncate' also pad would leave it indistinguish-
# able from 'pad_or_truncate'. See decisions/0015.
ROW_WIDTH_MODES = ('strict', 'pad', 'truncate', 'pad_or_truncate')


def validate_row_width(
    value, *, field_name='row_width', error_type=ValueError,
):
    """Validate one row-width mode at any public boundary.

    Lives here rather than in resources/csv.py for the same reason the two
    validators above do: the vocabulary is the contract, and a second copy
    of it inside the resource layer is how the spec path and the direct
    path drift apart.
    """
    if not isinstance(value, str):
        raise error_type(
            f'{field_name} must be a str, got {type(value).__name__}'
        )
    if value not in ROW_WIDTH_MODES:
        raise error_type(
            f'{field_name} must be one of {ROW_WIDTH_MODES}, got {value!r}'
        )
    return value


@runtime_checkable
class DbRowSource(Protocol):
    """One-shot iterator over positional rows for a DbPayload.

    Yields sequences of column values in the order declared by the
    payload's ``columns``. Consumed exactly once -- callers that need
    a second traversal must materialize themselves. Positional (not
    dictionary) so a COPY transport can spool values straight to
    PostgreSQL without walking a dict per row. See ADR 0011
    (Row-source contract).
    """

    def iter_rows(self) -> Iterator[Sequence[Any]]:
        ...


def validate_payload_source_state(
    loader, rows, row_source, *, error_type=ValueError,
):
    """Enforce the exact (db_loader, rows, row_source) state matrix from
    ADR 0011 §Row-source contract:

        loader=insert  ->  rows present, row_source absent
        loader=copy    ->  rows absent,  row_source present

    Any other combination is a configuration error before source
    execution or staging DDL. Called from ``DbPayload.__post_init__``
    after ``validate_db_loader`` so ``loader`` is already known to be a
    supported value.

    Both legs are public in 0.6.6. COPY callers must supply the one-shot
    row source and must not also materialize a row list.
    """
    if loader == 'insert':
        if rows is None:
            raise error_type(
                "db_loader='insert' requires rows to be present, got None"
            )
        if row_source is not None:
            raise error_type(
                "db_loader='insert' must not carry a row_source; the "
                'materialized rows are the source'
            )
        return

    if loader == 'copy':
        if row_source is None:
            raise error_type(
                "db_loader='copy' requires row_source to be present, got None"
            )
        if rows is not None:
            raise error_type(
                "db_loader='copy' must not carry materialized rows; the "
                'row_source is the source'
            )
        return

    # Any loader value not covered above should already have been
    # rejected by validate_db_loader before this runs. Reaching this
    # branch means the two functions have drifted -- an invariant
    # violation, not a task-author mistake.
    raise error_type(
        f'internal invariant violated -- validate_payload_source_state '
        f'reached unknown loader {loader!r}'
    )


@dataclass(frozen=True)
class OutputColumn:
    """One column in a fully declared database output schema.

    ``type`` accepts the same SQLAlchemy type instance, SQLAlchemy type
    class, or supported string alias as ``db_type_overrides``. Columns are
    nullable by default; task authors opt into ``NOT NULL`` explicitly.
    """

    name: str
    type: Any
    nullable: bool = field(default=True, kw_only=True)

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name:
            raise TypeError('OutputColumn.name must be a non-empty str')
        if self.type is None:
            raise TypeError('OutputColumn.type must not be None')
        if type(self.nullable) is not bool:
            raise TypeError('OutputColumn.nullable must be bool')


@dataclass(frozen=True, kw_only=True)
class PipelineSpec:
    excel_name: str | None = None
    db_table: str | None = None
    db_output: list[str] | tuple[str, ...] | None = None
    db_contract: dict[str, str] | None = None
    db_type_overrides: dict[str, Any] | None = None
    db_table_id_pix: Any | None = None
    db_updated_at: bool | str = False
    publish_result: bool = False
    debug_display: bool = False
    table_adapter: str | None = None
    db_not_null_columns: list[str] | tuple[str, ...] | None = None
    output_schema: list[OutputColumn] | tuple[OutputColumn, ...] | None = None
    # None resolves to 'replace' for BOTH schema modes.
    #
    # 'refill' requires output_schema: it needs the target's physical schema
    # to remain stable across runs, and only a declaration can promise that.
    db_publication_strategy: str | None = None
    # Implemented values are 'insert' and 'copy'.
    db_loader: str = 'insert'
    # Secure default is inherited from PublisherConfig.copy_load_policy.
    # A task may explicitly opt out for controlled, non-sensitive workloads.
    db_copy_spool_encryption: bool | None = None

    def __post_init__(self):
        if self.excel_name is not None and not isinstance(self.excel_name, str):
            raise TypeError('excel_name must be str or None')

        if self.db_table is not None and not isinstance(self.db_table, str):
            raise TypeError('db_table must be str or None')

        validate_publication_strategy(
            self.db_publication_strategy,
            output_schema=self.output_schema,
            allow_none=True,
            field_name='db_publication_strategy',
        )

        validate_db_loader(self.db_loader, field_name='db_loader')

        if self.db_copy_spool_encryption is not None and type(
            self.db_copy_spool_encryption
        ) is not bool:
            raise TypeError('db_copy_spool_encryption must be bool or None')

        if self.db_output is not None:
            # Require the declared contract (list[str] | tuple[str, ...] |
            # None) exactly, not any Iterable -- a generator would pass
            # isinstance(..., Iterable), get silently consumed by the
            # all(isinstance(item, str) ...) check below, and leave
            # self.db_output holding an exhausted generator forever after
            # (list(spec.db_output) == [] on every later read, with no
            # error anywhere). Sets are also excluded despite being
            # Iterable and non-string/Mapping -- db_output's order is
            # meaningful (it's a column projection/order), and a set
            # doesn't preserve one.
            if not isinstance(self.db_output, (list, tuple)):
                raise TypeError('db_output must be a list or tuple of strings, or None')
            if not all(isinstance(item, str) for item in self.db_output):
                raise TypeError('db_output must contain only strings')
            object.__setattr__(self, 'db_output', tuple(self.db_output))

        if self.db_contract is not None:
            if not isinstance(self.db_contract, dict):
                raise TypeError('db_contract must be dict or None')
            if not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in self.db_contract.items()
            ):
                raise TypeError('db_contract must map strings to strings')
            # frozen=True only ever blocked reassigning self.db_contract
            # itself, never mutating the dict it points to -- confirmed
            # directly: spec.db_contract['x'] = 'y' worked fine despite
            # the dataclass being frozen, contradicting export.py's
            # stated guarantee that publish configuration is captured
            # before run() and cannot change during execution.
            # MappingProxyType actually closes that, matching the same
            # treatment already given to PipelineBinding.resources.
            object.__setattr__(self, 'db_contract', MappingProxyType(dict(self.db_contract)))

        if self.db_type_overrides is not None:
            if not isinstance(self.db_type_overrides, dict):
                raise TypeError('db_type_overrides must be dict or None')
            object.__setattr__(self, 'db_type_overrides', MappingProxyType(dict(self.db_type_overrides)))

        if self.db_not_null_columns is not None:
            if not isinstance(self.db_not_null_columns, (list, tuple)):
                raise TypeError(
                    'db_not_null_columns must be a list or tuple of strings, or None'
                )
            if not all(isinstance(item, str) for item in self.db_not_null_columns):
                raise TypeError('db_not_null_columns must contain only strings')
            duplicates = find_duplicates(self.db_not_null_columns)
            if duplicates:
                raise PipelineContractError(
                    f'db_not_null_columns contains duplicate column(s): {duplicates}'
                )
            object.__setattr__(
                self, 'db_not_null_columns', tuple(self.db_not_null_columns)
            )

        if self.output_schema is not None:
            if not isinstance(self.output_schema, (list, tuple)):
                raise TypeError(
                    'output_schema must be a list or tuple of OutputColumn values, or None'
                )
            if not self.output_schema:
                raise PipelineContractError('output_schema must contain at least one column')
            if not all(isinstance(item, OutputColumn) for item in self.output_schema):
                raise TypeError('output_schema must contain only OutputColumn values')
            duplicates = find_duplicates(column.name for column in self.output_schema)
            if duplicates:
                raise PipelineContractError(
                    f'output_schema contains duplicate column name(s): {duplicates}'
                )
            object.__setattr__(self, 'output_schema', tuple(self.output_schema))

        if self.output_schema is not None:
            incompatible = []
            if self.db_output is not None:
                incompatible.append('db_output')
            if self.db_type_overrides is not None:
                incompatible.append('db_type_overrides')
            if self.db_not_null_columns is not None:
                incompatible.append('db_not_null_columns')
            if incompatible:
                raise PipelineContractError(
                    'output_schema cannot be combined with ' + ', '.join(incompatible)
                )

        if not isinstance(self.db_updated_at, (bool, str)):
            raise TypeError('db_updated_at must be bool or str')
        if isinstance(self.db_updated_at, str) and not self.db_updated_at:
            raise TypeError('db_updated_at must be a non-empty str when used as a column name')

        updated_at_name = (
            self.db_updated_at if isinstance(self.db_updated_at, str) else 'etl_updated_at'
        )
        if self.db_updated_at:
            if self.output_schema is not None and any(
                column.name == updated_at_name for column in self.output_schema
            ):
                raise PipelineContractError(
                    f'output_schema must not declare framework column {updated_at_name!r}'
                )
            if self.db_not_null_columns and updated_at_name in self.db_not_null_columns:
                raise PipelineContractError(
                    f'db_not_null_columns must not include framework column {updated_at_name!r}; '
                    'it is always NOT NULL'
                )
            if self.db_type_overrides and updated_at_name in self.db_type_overrides:
                raise PipelineContractError(
                    f'db_type_overrides must not override framework column {updated_at_name!r}'
                )

        if not isinstance(self.publish_result, bool):
            raise TypeError('publish_result must be bool')

        if not isinstance(self.debug_display, bool):
            raise TypeError('debug_display must be bool')

        if self.table_adapter not in VALID_TABLE_ADAPTERS:
            raise PipelineContractError(
                f'table_adapter must be one of {sorted(a for a in VALID_TABLE_ADAPTERS if a)} '
                f'or None, got {self.table_adapter!r}'
            )


@dataclass(frozen=True)
class DbRunResult:
    requested: bool
    had_outputs: bool
    committed: bool
    # Typed list[Any], not list[DbTableResult]: DbTableResult is defined in
    # db/publish.py. Even though db/publish.py now lives inside task_core
    # (task_core/db/publish.py), it's still an implementation module one
    # level up from types.py -- the same reasoning as
    # RunResult.source_fingerprints below applies: types.py stays
    # stdlib-only, full stop, not "stdlib-only except peer modules that
    # happen to be convenient."
    committed_tables: list[Any]
    published_tables: list[Any]
    row_counts: dict[str, int]

    @property
    def status(self):
        if not self.requested:
            return 'not_requested'
        if not self.had_outputs:
            return 'no_tables'
        return 'committed' if self.committed else 'not_committed'


@dataclass(frozen=True)
class RunResult:
    task_name: str
    pipeline_rows: dict[str, int]
    excel_outputs: list[str]
    db: DbRunResult
    skipped: bool = False
    skip_reason: str | None = None
    source_check_enabled: bool = False
    source_changed: bool | None = None
    source_fingerprints: list[Any] = field(default_factory=list)

    @property
    def db_committed(self):
        return self.db.committed

    @property
    def db_committed_tables(self):
        return list(self.db.committed_tables)

    @property
    def db_committed_table_names(self):
        return [item.full_name for item in self.db.committed_tables]

    @property
    def db_committed_table_ids_pix(self):
        if not self.db.requested or not self.db.committed:
            return None

        return [
            item.db_table_id_pix
            for item in self.db.committed_tables
            if item.db_table_id_pix is not None
        ]


class PipelineContractError(ValueError):
    pass


class PipelineError(RuntimeError):
    def __init__(self, task_name, pipeline, step, message):
        self.task_name = task_name
        self.pipeline = pipeline
        self.step = step
        self.message = message
        super().__init__(message)

    def __str__(self):
        return f'{self.task_name}: pipeline {self.pipeline!r} step {self.step!r}: {self.message}'


class SourceCheckError(PipelineContractError):
    pass


def get_pipeline_spec(task_cls):
    spec = getattr(task_cls, 'spec', None)
    if not isinstance(spec, PipelineSpec):
        raise PipelineContractError(
            f'{task_cls.__name__}: missing class attribute spec = PipelineSpec(...)'
        )
    return spec
