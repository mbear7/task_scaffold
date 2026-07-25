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

from task_core.db_publish import from_pandas, from_petl, is_missing

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
    # happens to expose a same-named method.
    if getattr(value, 'tzinfo', None) is not None:
        return value.replace(tzinfo=None)

    # NaN (np.nan, pd.NA, pd.NaT, or any other value pd.isna() recognizes
    # as missing) is the second confirmed case, not a hypothetical one:
    # writing it as-is produces structurally malformed XML -- a
    # numeric-typed cell with an empty <v></v> (petl's toxlsx()) or an
    # inlineStr-typed cell with no content at all (pandas's to_excel())
    # -- confirmed directly by inspecting the raw XML both adapters
    # produce. Both happen to read back as None via openpyxl's own
    # leniency, which is exactly why this went unnoticed: it isn't the
    # same guarantee as the clean, standard XLSX a genuine None
    # produces (the cell correctly omitted entirely), and isn't a
    # reliable signal that real, desktop Excel would open the file
    # cleanly, without a repair prompt.
    #
    # This exact fix silently regressed once already (v0.2.0 was rebuilt
    # from a point before it existed, with no persistent test catching
    # the loss) -- see tests/test_table_adapters.py, which asserts
    # against the raw, on-disk worksheet XML specifically, not just an
    # in-memory table/DataFrame value, precisely because that's what let
    # the regression through unnoticed the first time.
    #
    # is_missing() (db_publish.py), not a bare value != value check: a
    # further review found that check genuinely broken for pd.NA
    # specifically -- see is_missing()'s own docstring for why -- which
    # this function had too, independently of _normalize_value() having
    # the identical bug.
    if is_missing(value):
        return None

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

    def stabilize(self, tbl, repeated):
        # etl.cache() materializes rows into memory as they're first
        # requested via iteration, then serves every later traversal from
        # that cache instead of re-running whatever lazy chain produced
        # them -- confirmed directly this is transparent to every caller
        # here (nrows(), display(), to_excel(), to_db_payload() all just
        # iterate tbl normally, with no idea whether it's cached).
        # Skipped when repeated is False: wrapping a table that's only
        # ever traversed once (by nrows() alone) would allocate a
        # CacheView for zero benefit.
        return tbl.cache() if repeated else tbl

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
                f"set table_adapter='petl' if this pipeline should use petl"
            )

    def stabilize(self, tbl, repeated):
        # A pandas DataFrame is already fully materialized in memory --
        # never a lazy chain the way a petl transformation can be, so
        # there's nothing here for repeated traversal to re-run.
        # validate() above already enforces this is a genuine
        # pd.DataFrame, not some lazier pandas-adjacent object, ruling
        # out any case where this would need to do more.
        return tbl

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

        # Numeric/datetime-dtype columns with a missing value: confirmed
        # directly that pandas's own df.to_excel() already treats an
        # untouched, native nan identically to a pre-converted None here
        # (both hit the same, separate na_rep='' limitation regardless),
        # so this loop currently changes nothing observable for the
        # pandas adapter specifically. Kept anyway as defensive,
        # forward-looking code -- if a future pandas version ever
        # differentiates nan from None here the way it currently doesn't,
        # this is already in place rather than needing to be
        # rediscovered. Only touches a column that actually has a
        # missing value, not every numeric/datetime column, so a column
        # with none is completely unaffected either way.
        for col in df.columns:
            if df[col].dtype == object:
                continue  # already handled above
            if df[col].isna().any():
                df[col] = df[col].astype(object).where(df[col].notna(), None)

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
