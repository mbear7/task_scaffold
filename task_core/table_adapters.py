# -*- coding: utf-8 -*-
"""
Level 2: engine adapters for pipeline output. Sits below export.py within
level 2 -- export.py imports get_table_adapter from here (lateral,
acyclic); this module must never import anything back from export.py.
Depends on types.py (VALID_TABLE_ADAPTERS, PipelineContractError),
task_core.db_publish (from_petl/from_pandas -- internal to task_core, not
an external peer module), petl (etl, real package, imported directly),
pandas, and openpyxl_compat.

Adapters are engine mechanics only -- turning a pipeline's output table
into a DbPayload or an Excel file, counting its rows, previewing it.
Anything that operates on task_core's own already-engine-neutral
representations after that point (db_updated_at, the dynamic-contract
hook) is orchestration, lives in export.py, and is written once, not
duplicated per adapter.
"""

from __future__ import annotations

import pandas as pd
import petl as etl

from task_core.db_publish import from_pandas, from_petl

from task_core.types import PipelineContractError, VALID_TABLE_ADAPTERS
from task_core.openpyxl_compat import suppress_openpyxl_data_validation_warning


def normalize_for_excel(value):
    # Single dispatch point for coercing values petl/openpyxl/pandas can't
    # write to .xlsx as-is. Starts with the one quirk actually seen in
    # practice: openpyxl refuses any tz-aware datetime/time outright
    # ("Excel does not support timezones in datetimes"), which surfaces
    # whenever a pipeline's source table (e.g. a psycopg2-backed
    # db_resource reading a timestamptz column) flows into Excel export.
    # Deliberately conservative: checks for a meaningful .tzinfo value
    # first, never calls .replace() just because some unrelated object
    # happens to expose a same-named method. Deliberately not pre-emptively
    # handling other hypothetical quirks (Decimal, bytes, NaN, ...) that
    # haven't actually been hit -- add them here, one confirmed case at a
    # time, without needing to touch either adapter's to_excel().
    if getattr(value, 'tzinfo', None) is not None:
        return value.replace(tzinfo=None)

    return value


class _PetlAdapter:
    def validate(self, tbl):
        if tbl is None:
            raise PipelineContractError(
                'expected a petl table, got None -- did .run() forget a return?'
            )
        if isinstance(tbl, pd.DataFrame):
            raise PipelineContractError(
                "expected a petl table, got a pandas DataFrame -- "
                "set table_adapter='pandas' if this pipeline should use pandas"
            )

    def nrows(self, tbl):
        with suppress_openpyxl_data_validation_warning():
            return etl.nrows(tbl)

    def display(self, tbl):
        with suppress_openpyxl_data_validation_warning():
            tbl.display(limit=1000, encoding='utf-8')

    def to_excel(self, tbl, path):
        with suppress_openpyxl_data_validation_warning():
            etl.convertall(tbl, normalize_for_excel).toxlsx(path)

    def to_db_payload(self, tbl, **kwargs):
        with suppress_openpyxl_data_validation_warning():
            return from_petl(tbl, **kwargs)


class _PandasAdapter:
    def validate(self, tbl):
        if not isinstance(tbl, pd.DataFrame):
            raise PipelineContractError(
                f"expected a pandas DataFrame, got {type(tbl).__name__} -- "
                f"set table_adapter='petl' (or leave it unset) if this "
                f"pipeline should use petl"
            )

    def nrows(self, tbl):
        return len(tbl)

    def display(self, tbl):
        print(tbl.head(1000).to_string())

    def to_excel(self, tbl, path):
        df = tbl.copy()  # never mutate the caller's out_tbl -- to_db_payload
                          # may still run on the same object afterward and
                          # needs full tz precision preserved

        # Fast path: columns already datetime64[ns, tz] dtype.
        for col in df.select_dtypes(include=['datetimetz']).columns:
            df[col] = df[col].dt.tz_localize(None)

        # Generic path: object-dtype columns can hold anything a dtype
        # check can't see into -- tz-aware datetime not coerced to
        # datetime64, tz-aware time (pandas has no native dtype for this
        # at all, so this is the only path that can ever catch it), or a
        # genuinely mixed column. Same per-value duck-typed check the
        # petl adapter uses via etl.convertall(tbl, normalize_for_excel).
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].map(normalize_for_excel)

        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='data')

    def to_db_payload(self, tbl, **kwargs):
        return from_pandas(tbl, **kwargs)


PETL_ADAPTER = _PetlAdapter()
PANDAS_ADAPTER = _PandasAdapter()

_ADAPTERS = {
    None: PETL_ADAPTER,
    'petl': PETL_ADAPTER,
    'pandas': PANDAS_ADAPTER,
}
assert set(_ADAPTERS) == VALID_TABLE_ADAPTERS  # fails at import time, not
                                                 # at some later call, if
                                                 # these ever drift apart


def get_table_adapter(table_adapter):
    try:
        return _ADAPTERS[table_adapter]
    except KeyError:
        raise PipelineContractError(f'unsupported table_adapter: {table_adapter!r}')
