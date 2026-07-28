# -*- coding: utf-8 -*-
"""
Level 0: shared vocabulary for the rest of the package. Zero imports from
anywhere else in task_core, or from db_publish.py -- not even under
TYPE_CHECKING. RunResult.source_fingerprints and
DbRunResult.committed_tables/published_tables are typed list[Any] rather
than list[SourceFingerprint]/list[DbTableResult] specifically to keep
this module import-free.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

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
    hand-rolled in runner.py, binding.py, and db_publish.py -- order matters
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
# normalization and collision rules) lives in db_publish.py instead.
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



@dataclass(frozen=True)
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
    def __post_init__(self):
        if self.excel_name is not None and not isinstance(self.excel_name, str):
            raise TypeError('excel_name must be str or None')

        if self.db_table is not None and not isinstance(self.db_table, str):
            raise TypeError('db_table must be str or None')

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

        if not isinstance(self.db_updated_at, (bool, str)):
            raise TypeError('db_updated_at must be bool or str')
        if isinstance(self.db_updated_at, str) and not self.db_updated_at:
            raise TypeError('db_updated_at must be a non-empty str when used as a column name')

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
    # db_publish.py. Even though db_publish.py now lives inside task_core
    # (task_core/db_publish.py), it's still an implementation module one
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
