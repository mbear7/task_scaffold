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

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

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
            if (
                isinstance(self.db_output, (str, Mapping))
                or not isinstance(self.db_output, Iterable)
            ):
                raise TypeError('db_output must be a sequence of strings or None')
            if not all(isinstance(item, str) for item in self.db_output):
                raise TypeError('db_output must contain only strings')

        if self.db_contract is not None:
            if not isinstance(self.db_contract, dict):
                raise TypeError('db_contract must be dict or None')
            if not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in self.db_contract.items()
            ):
                raise TypeError('db_contract must map strings to strings')

        if self.db_type_overrides is not None and not isinstance(self.db_type_overrides, dict):
            raise TypeError('db_type_overrides must be dict or None')

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
    # db_publish.py. Even though db_publish.py sits outside the task_core
    # package, it's still an implementation module -- the same reasoning as
    # RunResult.source_fingerprints (section 2a) applies: types.py stays
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
