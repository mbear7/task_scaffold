# -*- coding: utf-8 -*-
"""
hr_task.py -- the full HR reporting task on task_core's mixed-engine
machinery (table_adapter='pandas' throughout): staff, prepare_funnel,
funnel_closed, funnel_open, declined_close, declined_open, ssch, and
recruiters. Originally began as a migration of funnel_pandas.py's ssch
pipeline specifically; every other pipeline was migrated here over time
since, and funnel_pandas.py is no longer this task's source of truth for
any of them.
"""

from __future__ import annotations

import datetime as dt
import logging
import re

import numpy as np
import pandas as pd

from petl_util import MONTH_MAP

from task_core import (
    PipelineSpec,
    ResourceEnvironment,
    SourceChangeCheckConfig,
    align_row_metadata,
    bind,
    build_resource_context,
    build_source_access,
    latest_xlsx,
    run_pipelines,
    setup_logging,
    xlsx_file_set,
)

try:
    from pgcreds import pgcreds as DEFAULT_PGCREDS
except ImportError:
    DEFAULT_PGCREDS = None


TASK_NAME = 'hr_task'
BASE_PATH = r'\\telecom.local\dfs\Стратегическое развитие\pbi-data\HR'
PG_SCHEMA = 'bsr'
OUTPUT_EXCEL = True
OUTPUT_DB = True

log = logging.getLogger(TASK_NAME)

SOURCE_CHANGE_CHECK = SourceChangeCheckConfig(
    enabled=True,
    schema='bsr',
    table='task_scaffold_meta',
)

# Epoch of clean source data -- vacancies opened before this are legacy
# artifacts from previous years, deliberately excluded (funnel_open.run).
MIN_VACANCY_OPEN_DATE = dt.date(2025, 1, 1)

# Shared by preprocess_sheet (prepare_funnel) and recruiters.process_workbook --
# the one legal entity these pipelines report on, out of every ЮР value
# split_customer can produce.
TARGET_LEGAL_ENTITY = 'ООО "УГМК-Телеком"'


# Standalone helpers copied from funnel_pandas.py, not imported -- no live
# dependency on the old file. No further dependencies beyond pandas/numpy/stdlib.

def month_start_date(value):
    if isinstance(value, pd.Series):
        s = pd.to_datetime(value, errors='coerce')
        return s.dt.to_period('M').dt.to_timestamp().dt.date
    ts = pd.to_datetime(value, errors='coerce')
    if pd.isna(ts):
        return None
    return ts.to_period('M').to_timestamp().date()


def is_blank_header_label(value):
    if value is None:
        return True
    if isinstance(value, str) and value == '':
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return False


def normalize_header_label(value):
    if is_blank_header_label(value):
        return None
    if not isinstance(value, str):
        return value

    s = value.replace('\r\n', '\n').replace('\r', '\n')
    s = re.sub(r'\s*\n\s*', '\n', s)
    s = re.sub(r'[\t ]+', ' ', s)
    return s.strip()


def check_no_duplicate_headers(labels, file_name):
    # Neutral to whether labels are already normalized -- just checks
    # whatever the caller already produced, never touches the labels
    # themselves. Duplicate header cells otherwise produce duplicate
    # column names, after which df['col'] returns a DataFrame instead of
    # a Series and everything downstream fails obscurely, far from the
    # actual cause.
    seen = {}
    duplicates = {}
    for i, label in enumerate(labels):
        if is_blank_header_label(label):
            continue
        if label in seen:
            duplicates.setdefault(label, [seen[label]]).append(i)
        else:
            seen[label] = i
    if duplicates:
        raise ValueError(f'{file_name}: duplicate header labels: {duplicates!r}')


def select_unique_required_columns(df, required_cols, location):
    # Deliberately a *narrower* duplicate policy than
    # check_no_duplicate_headers() above -- two policies coexist in this
    # file on purpose, not by accident. Pipelines that project down to a
    # fixed required-column set immediately (staff, recruiters) only need
    # uniqueness among the columns they actually keep; a duplicate among
    # headers they're about to discard is not their problem. Pipelines
    # that keep wide columns for later processing (prepare_funnel's
    # sheets, ssch's promoted tables, where drop_other/melt happens much
    # further downstream) check ALL headers via
    # check_no_duplicate_headers(), because any of those columns may be
    # touched later.
    required_cols = list(required_cols)
    required_set = set(required_cols)

    if len(required_set) != len(required_cols):
        duplicate_requirements = sorted({
            col for col in required_cols if required_cols.count(col) > 1
        })
        raise ValueError(
            f'{location}: required column list contains duplicates: '
            f'{duplicate_requirements!r}'
        )

    positions = {col: [] for col in required_cols}
    for index, label in enumerate(df.columns):
        if label in required_set:
            positions[label].append(index)

    missing = [col for col in required_cols if not positions[col]]
    duplicates = {
        col: indexes
        for col, indexes in positions.items()
        if len(indexes) > 1
    }

    if missing or duplicates:
        problems = []
        if missing:
            problems.append(f'missing: {missing!r}')
        if duplicates:
            problems.append(f'duplicates: {duplicates!r}')
        raise ValueError(
            f'{location}: invalid required headers: ' + '; '.join(problems)
        )

    return df.iloc[:, [positions[col][0] for col in required_cols]].copy()


def parse_date_value(value):
    # Same as funnel_pandas.py's version, but looks up month names in
    # petl_util.MONTH_MAP (babel/CLDR) instead of a hand-written dict --
    # confirmed a strict superset of it (same values, zero missing/mismatched).
    # Doesn't unlock abbreviated forms ("янв.") despite MONTH_MAP having them --
    # the regexes below only extract [а-яё]+ (letters, no period).
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return value

    if isinstance(value, (np.datetime64,)):
        ts = pd.to_datetime(value, errors='coerce')
        return None if pd.isna(ts) else ts

    if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day') and not isinstance(value, str):
        try:
            return pd.Timestamp(year=int(value.year), month=int(value.month), day=int(value.day))
        except Exception:
            pass

    if isinstance(value, str):
        s = value.strip()
        if s == '':
            return None

        try:
            return pd.to_datetime(s, dayfirst=True, errors='raise')
        except Exception:
            pass

        normalized = re.sub(r'\s+', ' ', s.lower().replace('г.', '').replace('г', '')).strip(' .')

        m = re.fullmatch(r'([а-яё]+)\s+(\d{4})', normalized)
        if m is not None:
            month_name = m.group(1)
            year = int(m.group(2))
            if month_name not in MONTH_MAP:
                raise ValueError(f'Unsupported month name {month_name!r}')
            return pd.Timestamp(year=year, month=MONTH_MAP[month_name], day=1)

        m = re.fullmatch(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})', normalized)
        if m is not None:
            day = int(m.group(1))
            month_name = m.group(2)
            year = int(m.group(3))
            if month_name not in MONTH_MAP:
                raise ValueError(f'Unsupported month name {month_name!r}')
            return pd.Timestamp(year=year, month=MONTH_MAP[month_name], day=day)

    raise ValueError(f'Cannot parse date from {value!r}')


# Replaces readxlsx_extra, decomposed into task_core-native calls so this
# pipeline is SMB-ready via file_set_resource.open_file(), instead of
# readxlsx_extra's own local zipfile/pd.read_excel calls against a plain path.

# === prepare_funnel: enumerates every sheet in the latest workbook,
# keeps only sheets containing a 'ФИО' marker in column 1, promotes each
# valid sheet's own header row (found by content, not position), filters
# to one legal entity, and concatenates. No excel/db output -- feeds
# downstream pipelines via publish_result. ===

def sheet_to_raw_dataframe(resource, sheet_name):
    # get_sheet_raw_rows() is genuinely headerless (a plain row sequence,
    # not a petl table), unlike get_sheet_rows()/get_range() -- a petl
    # table always treats its own first row as its header once read via
    # etl.header()/etl.data(), regardless of the header= argument used to
    # build it. Row 0 needs to stay available as real data here, since
    # the actual header could be anywhere in the sheet.
    raw_rows = [list(r) for r in resource.get_sheet_raw_rows(sheet_name)]
    return pd.DataFrame(raw_rows)


def is_valid_sheet(raw):
    if raw.shape[1] < 2:
        return False
    probe = raw.iloc[:10, 1]
    return probe.astype('string').str.contains('ФИО', na=False).any()


def find_header_row(raw):
    for i, row in raw.iterrows():
        if row.astype('string').str.contains('ID вакансии', na=False).any():
            return i
    raise ValueError("Header row with 'ID вакансии' not found")


def split_customer(df):
    # Shared with recruiters -- do not change this without checking that
    # pipeline too.
    parts = df['Заказчик'].astype('string').str.split(' / ', expand=True).iloc[:, :4]
    while parts.shape[1] < 4:
        parts[parts.shape[1]] = pd.NA
    parts.columns = ['ЮР', 'Блок', 'Подразделение', 'Подразделение3']
    df[parts.columns] = parts
    for c in parts.columns:
        df[c] = df[c].astype('string').str.strip()
    for c in parts.columns[1:]:
        df[c] = df[c].fillna('-- нет --')
    return df


def preprocess_sheet(raw, file_name=None, sheet_name=None):
    location = f'{file_name}::{sheet_name}' if file_name is not None else '<sheet>'
    hdr = find_header_row(raw)
    df = raw.iloc[hdr:].copy()
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)

    unnamed = [c for c in df.columns if pd.isna(c) or str(c).strip() == '']
    if unnamed:
        df = df.drop(columns=unnamed)

    check_no_duplicate_headers(list(df.columns), location)

    if 'Заказчик' not in df.columns:
        raise ValueError(f"{location}: missing required column: 'Заказчик'")

    df = split_customer(df)
    df = df[df['ЮР'] == TARGET_LEGAL_ENTITY].copy()

    norm_cols = [
        '№', 'ФИО', 'Ответственный за вакансию', 'ID вакансии', 'Источник',
        'Категории профессий', 'Причина возникновения вакансии', 'Вакансия',
        'Блок', 'Подразделение', 'Подразделение3', 'Статус вакансии', 'ЮР',
    ]
    for c in norm_cols:
        if c in df.columns:
            df[c] = df[c].astype('string').str.strip()

    if 'Ответственный за вакансию' in df.columns:
        df['Ответственный за вакансию'] = df['Ответственный за вакансию'].fillna('-- нет --')

    df = df[
        df['№'].notna() & df['ID вакансии'].notna()
        & (df['№'] != '') & (df['ID вакансии'] != '')
    ].copy()
    return df


class prepare_funnel:
    spec = PipelineSpec(
        publish_result=True,
        table_adapter='pandas',
    )

    @classmethod
    def run(cls, ctx, *, source):
        frames = []
        for sheet_name in source.sheets:
            raw = sheet_to_raw_dataframe(source, sheet_name)
            if not is_valid_sheet(raw):
                continue
            frames.append(preprocess_sheet(raw, source.file_path, sheet_name))
        if not frames:
            raise ValueError(f'No valid funnel sheets found in {source.file_path}')
        return pd.concat(frames, ignore_index=True)


# === Shared by funnel_closed and the three funnel-family pipelines still
# to come (funnel_open, declined_open, declined_close) -- all consume
# ctx.get_result('prepare_funnel')'s output and use these for schema
# coercion, stage-pair combining, and date/year extraction. ===

_SUPPORTED_COLUMN_TYPES = {'int', 'float', 'str', 'date', 'datetime', 'any'}


def _normalize_column_spec(spec):
    if isinstance(spec, str):
        spec = {'type': spec}
    elif not isinstance(spec, dict):
        raise TypeError(f'Invalid column spec: {spec!r}')

    if 'type' not in spec:
        raise ValueError(f'Column spec must contain type: {spec!r}')

    col_type = spec['type']
    if col_type not in _SUPPORTED_COLUMN_TYPES:
        raise ValueError(
            f'Unsupported column type {col_type!r}. '
            f'Expected one of: {sorted(_SUPPORTED_COLUMN_TYPES)}'
        )

    return {
        'type': col_type,
        'nullable': spec.get('nullable', True),
    }


def as_date(value):
    if isinstance(value, pd.Series):
        return pd.to_datetime(value, errors='coerce').dt.date
    ts = pd.to_datetime(value, errors='coerce')
    if pd.isna(ts):
        return None
    return ts.date()


def date_year(value):
    if isinstance(value, pd.Series):
        return pd.to_datetime(value, errors='coerce').dt.year
    ts = pd.to_datetime(value, errors='coerce')
    if pd.isna(ts):
        return None
    return int(ts.year)


def _cast_series(s, col_type):
    if col_type == 'any':
        return s

    if col_type == 'str':
        return s.astype('string')

    if col_type == 'float':
        return pd.to_numeric(s, errors='coerce')

    if col_type == 'int':
        return pd.to_numeric(s, errors='coerce').astype('Int64')

    if col_type == 'date':
        return as_date(s)

    if col_type == 'datetime':
        return pd.to_datetime(s, errors='coerce')

    raise ValueError(f'Unsupported column type: {col_type!r}')


def apply_column_types(df, schema):
    columns_spec = schema.get('columns')
    if not isinstance(columns_spec, dict) or not columns_spec:
        raise ValueError('Schema must contain non-empty dict columns')

    default_type = schema.get('default')
    drop_other = schema.get('drop_other', False)

    if default_type is not None and default_type not in _SUPPORTED_COLUMN_TYPES:
        raise ValueError(
            f'Unsupported default type {default_type!r}. '
            f'Expected one of: {sorted(_SUPPORTED_COLUMN_TYPES)}'
        )

    if drop_other and default_type is not None:
        raise ValueError('Schema cannot define both drop_other=True and default')

    missing_cols = [col for col in columns_spec if col not in df.columns]
    if missing_cols:
        raise ValueError(f'Missing required columns: {missing_cols}')

    df = df.copy()

    for col, raw_spec in columns_spec.items():
        spec = _normalize_column_spec(raw_spec)
        df[col] = _cast_series(df[col], spec['type'])

        if not spec['nullable']:
            null_count = int(df[col].isna().sum())
            if null_count:
                raise ValueError(
                    f'Column {col!r} is non-nullable but contains '
                    f'{null_count} nulls after type coercion'
                )

    other_cols = [col for col in df.columns if col not in columns_spec]

    if drop_other:
        return df[list(columns_spec)]

    if default_type is not None:
        for col in other_cols:
            df[col] = _cast_series(df[col], default_type)

    return df


def combine_stage_pair(df, new_col, left_col, right_col):
    df[new_col] = df[left_col].combine_first(df[right_col])
    return df.drop(columns=[left_col, right_col])


# Shared by declined_open and declined_close -- both had an identical
# copy in the original (verified byte-identical, not assumed), same
# situation apply_column_types/combine_stage_pair were in.
REASON_RENAME_MAP = {
    'Отсутствие требуемых навыков и квалификации': 'Навыки',
    'Отказ по формальным критериям': 'Бренд',
    'Кандидат не ищет работу': 'Не ищет работу',
    'Не согласован СБ': 'СБ',
    'Не прошел медосмотр': 'Медосмотр',
    'Несоответствие зарплатных ожиданий ': 'Зарплата',
    'Не устраивает социальный пакет/льготы': 'Соцпакет',
    'Не подходят условия труда ': 'Условия труда',
    'Не готов работать в офисе (удалёнка) ': 'Удалёнка',
    'Не готов к релокации': 'Релокация',
    'Не подходит график работы': 'График работы',
    'Не подходит уровень позиции / неинтересные задачи': 'Позиция/задачи',
}


class funnel_closed:
    spec = PipelineSpec(
        excel_name='funn_closed.xlsx',
        db_table='hr_funnel_closed',
        db_updated_at=True,
        db_contract={
            'Месяц': 'month',
            '№': 'n',
            'ФИО': 'fio',
            'Ответственный за вакансию': 'holder',
            'ID вакансии': 'id',
            'Источник': 'source',
            'Дата открытия вакансии': 'date_opened',
            'Категории профессий': 'category',
            'Причина возникновения вакансии': 'reason',
            'Вакансия': 'vacancy',
            'Блок': 'block',
            'Подразделение': 'unit',
            'Подразделение3': 'unit3',
            'Статус вакансии': 'vacancy_status',
            'Срок работы': 'age',
            'Уровень': 'level',
            'Значение': 'value',
        },
        table_adapter='pandas',
    )

    BASE_COLS = (
        '№', 'ФИО', 'Ответственный за вакансию', 'ID вакансии', 'Источник',
        'Дата открытия вакансии', 'Категории профессий', 'Причина возникновения вакансии',
        'Вакансия', 'Блок', 'Подразделение', 'Подразделение3', 'Статус вакансии',
        'Новый', 'Первичное интервью (Рекрутер)',
        'Резюме на рассмотрении у представителя Заказчика',
        'Предст. Заказчика - резюме согласовано',
        'Интервью 1',
        'Согласование оффера: Представитель заказчика',
        'Предст. Заказчика - оффер согласован',
        'Документы для проверки кандидата', 'СБ организации', 'Принят на работу',
    )

    STAGE_COLS = (
        'Отобрано', 'Рекрутер', 'Заказчик', 'Интервью', 'СБ', 'Оффер', 'Принят',
    )

    PREPARE_BASE_SCHEMA = {
        'columns': {
            '№': {'type': 'str', 'nullable': False},
            'ФИО': 'str',
            'Ответственный за вакансию': 'str',
            'ID вакансии': {'type': 'str', 'nullable': False},
            'Источник': 'str',
            'Дата открытия вакансии': 'date',
            'Категории профессий': 'str',
            'Причина возникновения вакансии': 'str',
            'Вакансия': 'str',
            'Блок': 'str',
            'Подразделение': 'str',
            'Подразделение3': 'str',
            'Статус вакансии': 'str',
            'Отобрано': 'datetime',
            'Рекрутер': 'datetime',
            'Заказчик': 'datetime',
            'Интервью': 'datetime',
            'СБ': 'datetime',
            'Оффер': 'datetime',
            'Принят': 'datetime',
        },
        'drop_other': True,
    }

    @classmethod
    def prepare_base(cls, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in cls.BASE_COLS if c not in df.columns]
        if missing:
            raise ValueError(f'Missing required columns: {missing}')

        df = df[list(cls.BASE_COLS)].copy()

        df = combine_stage_pair(
            df, 'Заказчик',
            'Резюме на рассмотрении у представителя Заказчика',
            'Предст. Заказчика - резюме согласовано',
        )
        df = combine_stage_pair(
            df, 'Оффер',
            'Согласование оффера: Представитель заказчика',
            'Предст. Заказчика - оффер согласован',
        )
        df = combine_stage_pair(
            df, 'СБ',
            'Документы для проверки кандидата',
            'СБ организации',
        )

        df = df.rename(columns={
            'Новый': 'Отобрано',
            'Первичное интервью (Рекрутер)': 'Рекрутер',
            'Интервью 1': 'Интервью',
            'Принят на работу': 'Принят',
        })

        df = apply_column_types(df, cls.PREPARE_BASE_SCHEMA)

        # Month bucket represented as month-start date.
        df['Мес'] = month_start_date(df['Принят'])

        non_stage_cols = [c for c in df.columns if c not in cls.STAGE_COLS]
        return df[[*non_stage_cols, *cls.STAGE_COLS]]

    @classmethod
    def build_month_scope(cls, df: pd.DataFrame) -> pd.DataFrame:
        curr_yr = date_year(df['Мес']).max()

        flt1 = df[
            df['Мес'].notna()
            & (df['Статус вакансии'] == 'Закрыта')
            & (date_year(df['Мес']) == curr_yr)
        ]

        scope = flt1[['Мес', 'ID вакансии']].drop_duplicates()

        df = df.merge(scope, on='ID вакансии', how='inner', suffixes=('', '_scope'))
        df = df.rename(columns={'Мес_scope': 'Месяц'})

        return df

    # A) groupby -> ids -> loop / concat
    #
    # @staticmethod
    # def build_month_scope(df: pd.DataFrame) -> pd.DataFrame:
    #     curr_yr = date_year(df['Мес']).max()
    #
    #     flt1 = df[
    #         df['Мес'].notna()
    #         & (df['Статус вакансии'] == 'Закрыта')
    #         & (date_year(df['Мес']) == curr_yr)
    #     ]
    #
    #     parts = []
    #
    #     for month, grp in flt1.groupby('Мес', sort=True):
    #         ids = grp['ID вакансии'].drop_duplicates()
    #         part = df[df['ID вакансии'].isin(ids)].copy()
    #         part['Месяц'] = month
    #         parts.append(part)
    #
    #     return pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()

    # D) PQ-like nested-table mental model
    #
    # @staticmethod
    # def build_month_scope(df: pd.DataFrame) -> pd.DataFrame:
    #     curr_yr = date_year(df['Мес']).max()
    #
    #     flt1 = df[
    #         df['Мес'].notna()
    #         & (df['Статус вакансии'] == 'Закрыта')
    #         & (date_year(df['Мес']) == curr_yr)
    #     ]
    #
    #     def attach_history(grp: pd.DataFrame) -> pd.DataFrame:
    #         month = grp.name
    #         ids = grp['ID вакансии'].drop_duplicates()
    #         part = df[df['ID вакансии'].isin(ids)].copy()
    #         part['Месяц'] = month
    #         return part
    #
    #     return (
    #         flt1.groupby('Мес', sort=True)
    #         .apply(attach_history)
    #         .reset_index(drop=True)
    #     )

    # Old row-wise version kept for reference during performance refactor.
    #
    # @staticmethod
    # def repair_stage_row(row: pd.Series) -> pd.Series:
    #     vals = [row[c] for c in funnel_closed.STAGE_COLS]
    #
    #     if pd.isna(vals).all():
    #         vals = [row['Месяц'] + pd.Timedelta(hours=14, minutes=10)] + [pd.NaT] * (len(funnel_closed.STAGE_COLS) - 1)
    #     else:
    #         carry = pd.NaT
    #         fixed = [pd.NaT] * len(funnel_closed.STAGE_COLS)
    #
    #         # Funnel stage order is semantic, not guaranteed chronological by source dates.
    #         for i in range(len(funnel_closed.STAGE_COLS) - 1, -1, -1):
    #             val = vals[i]
    #             if pd.notna(val):
    #                 carry = val
    #             fixed[i] = carry
    #
    #         vals = fixed
    #
    #     row[list(funnel_closed.STAGE_COLS)] = vals
    #     return row

    @classmethod
    def repair_stage_block(cls, df: pd.DataFrame) -> pd.DataFrame:
        stage_cols = list(cls.STAGE_COLS)
        first_stage = stage_cols[0]

        stage_block = df[stage_cols].bfill(axis=1)
        all_null = stage_block.isna().all(axis=1)

        if all_null.any():
            stage_block.loc[all_null, first_stage] = (
                pd.to_datetime(df.loc[all_null, 'Месяц'], errors='coerce')
                + pd.Timedelta(hours=14, minutes=10)
            )

        out = df.copy()
        out[stage_cols] = stage_block
        return out

    # Old groupby/apply version kept for reference during performance refactor.
    #
    # @staticmethod
    # def collapse_duplicate_group(df: pd.DataFrame) -> pd.Series:
    #     if len(df) == 1:
    #         return df.iloc[0]
    #
    #     nn = df.notna().sum(axis=1)
    #     row_max = df.loc[nn.idxmax()]
    #     row_min = df.loc[nn.idxmin()]
    #
    #     out = row_min.copy()
    #
    #     for c in funnel_closed.STAGE_COLS:
    #         a = row_min[c]
    #         b = row_max[c]
    #
    #         if pd.isna(a):
    #             out[c] = b
    #         elif pd.isna(b):
    #             out[c] = a
    #         else:
    #             out[c] = min(a, b)
    #
    #     return out

    @classmethod
    def collapse_duplicate_groups(cls, df: pd.DataFrame) -> pd.DataFrame:
        keys = ['Месяц', '№', 'ID вакансии']
        stage_cols = list(cls.STAGE_COLS)

        if df.empty:
            return df.copy()

        work = df.copy()
        value_cols = [c for c in work.columns if c not in keys]
        work['_nn'] = work[value_cols].notna().sum(axis=1)

        g = work.groupby(keys, dropna=False, sort=False)['_nn']
        idx_min = g.idxmin()
        idx_max = g.idxmax()

        row_min = work.loc[idx_min].copy()
        row_max = work.loc[idx_max].copy()

        row_min = row_min.set_index(keys)
        row_max = row_max.set_index(keys)

        for c in stage_cols:
            row_min[c] = pd.concat(
                [row_min[c], row_max[c]],
                axis=1,
            ).min(axis=1)

        out = row_min.reset_index()
        return out.drop(columns=['_nn'])

    @classmethod
    def run(cls, ctx):
        df = ctx.get_result('prepare_funnel')
        df = cls.prepare_base(df)
        df = cls.build_month_scope(df)

        # Reduce acceptance datetime to month bucket represented as month-start date.
        accept_month = month_start_date(df['Принят'])

        flt2 = df[
            df['Принят'].isna()
            | (df['Месяц'] == accept_month)
        ].copy()

        flt2 = flt2.drop(columns=['Мес'])

        non_stage_cols = [c for c in flt2.columns if c not in cls.STAGE_COLS and c != 'Месяц']
        ordered = flt2[['Месяц', *non_stage_cols, *cls.STAGE_COLS]]

        # Old row-wise version kept for reference during performance refactor.
        # repaired = ordered.apply(funnel_closed.repair_stage_row, axis=1)
        repaired = cls.repair_stage_block(ordered)

        # Old groupby/apply version kept for reference during performance refactor.
        # collapsed = (
        #     repaired.groupby(['Месяц', '№', 'ID вакансии'], dropna=False, sort=False)
        #     .apply(funnel_closed.collapse_duplicate_group, include_groups=False)
        #     .reset_index()
        # )
        collapsed = cls.collapse_duplicate_groups(repaired)

        collapsed['Срок работы'] = (
            pd.to_datetime(as_date(collapsed['Принят']), errors='coerce')
            - pd.to_datetime(collapsed['Дата открытия вакансии'], errors='coerce')
        ).dt.days

        return (
            collapsed
            .melt(
                id_vars=[c for c in collapsed.columns if c not in cls.STAGE_COLS],
                value_vars=list(cls.STAGE_COLS),
                var_name='Уровень',
                value_name='Значение',
            )
            .loc[lambda x: x['Значение'].notna()]
        )


class declined_close:
    spec = PipelineSpec(
        excel_name='declined_closed.xlsx',
        db_table='hr_declined_close',
        db_updated_at=True,
        db_contract={
            'ФИО': 'fio',
            'Ответственный за кандидата': 'holder',
            'ID вакансии': 'id',
            'Вакансия': 'vacancy',
            'Блок': 'block',
            'Подразделение': 'unit',
            'Подразделение3': 'unit3',
            'Месяц': 'month',
            'Причина': 'reason',
            'Значение': 'value',
        },
        table_adapter='pandas',
    )

    BASE_COLS = (
        'ФИО', 'Ответственный за кандидата', 'ID вакансии', 'Вакансия', 'Блок',
        'Подразделение', 'Подразделение3', 'Статус вакансии',
        'Отсутствие требуемых навыков и квалификации',
        'Кандидат не ищет работу',
        'Отказ по формальным критериям',
        'Не согласован СБ',
        'Не прошел медосмотр',
        'Несоответствие зарплатных ожиданий ',
        'Не устраивает социальный пакет/льготы',
        'Не подходят условия труда ',
        'Не готов работать в офисе (удалёнка) ',
        'Не готов к релокации',
        'Не подходит график работы',
        'Не подходит уровень позиции / неинтересные задачи',
        'Контроффер',
        'Принят на работу',
    )

    PREPARE_BASE_SCHEMA = {
        'columns': {
            'ФИО': 'str',
            'Ответственный за кандидата': 'str',
            'ID вакансии': {'type': 'str', 'nullable': False},
            'Вакансия': 'str',
            'Блок': 'str',
            'Подразделение': 'str',
            'Подразделение3': 'str',
            'Статус вакансии': 'str',
            'Отсутствие требуемых навыков и квалификации': 'datetime',
            'Кандидат не ищет работу': 'datetime',
            'Отказ по формальным критериям': 'datetime',
            'Не согласован СБ': 'datetime',
            'Не прошел медосмотр': 'datetime',
            'Несоответствие зарплатных ожиданий ': 'datetime',
            'Не устраивает социальный пакет/льготы': 'datetime',
            'Не подходят условия труда ': 'datetime',
            'Не готов работать в офисе (удалёнка) ': 'datetime',
            'Не готов к релокации': 'datetime',
            'Не подходит график работы': 'datetime',
            'Не подходит уровень позиции / неинтересные задачи': 'datetime',
            'Контроффер': 'datetime',
            'Принят на работу': 'datetime',
        },
        'drop_other': True,
    }

    @classmethod
    def prepare_base(cls, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in cls.BASE_COLS if c not in df.columns]
        if missing:
            raise ValueError(f'Missing required columns: {missing}')

        df = apply_column_types(df[list(cls.BASE_COLS)].copy(), cls.PREPARE_BASE_SCHEMA)
        df = df.rename(columns={'Принят на работу': '7_Принят'})
        df['Мес'] = month_start_date(df['7_Принят'])
        return df

    @classmethod
    def build_month_scope(cls, df: pd.DataFrame) -> pd.DataFrame:
        curr_yr = date_year(df['Мес']).max()

        flt1 = df[
            df['Мес'].notna()
            & (df['Статус вакансии'] == 'Закрыта')
            & (date_year(df['Мес']) == curr_yr)
        ]

        scope = flt1[['Мес', 'ID вакансии']].drop_duplicates()

        base = df[df['7_Принят'].isna()].drop(columns=['Мес'])
        out = base.merge(scope, on='ID вакансии', how='inner', suffixes=('', '_scope'))
        return out.rename(columns={'Мес': 'Месяц'})

    @classmethod
    def run(cls, ctx):
        df = ctx.get_result('prepare_funnel')
        df = cls.prepare_base(df)
        df = cls.build_month_scope(df)

        accept_month = month_start_date(df['7_Принят'])

        # Kept as a separate step to mirror PQ flow and allow easy toggling later if needed.
        flt2 = df[
            df['7_Принят'].isna()
            | (df['Месяц'] == accept_month)
        ].copy()

        flt2 = flt2.drop(columns=['7_Принят', 'Статус вакансии'])
        flt2 = flt2.rename(columns=REASON_RENAME_MAP)

        id_cols = [
            'Месяц', 'ФИО', 'Ответственный за кандидата', 'ID вакансии',
            'Вакансия', 'Блок', 'Подразделение', 'Подразделение3',
        ]

        out = (
            flt2
            .melt(
                id_vars=id_cols,
                value_vars=[c for c in flt2.columns if c not in id_cols],
                var_name='Причина',
                value_name='Значение',
            )
            .loc[lambda x: x['Значение'].notna()]
            .copy()
        )

        out = out[[
            'ФИО', 'Ответственный за кандидата', 'ID вакансии', 'Вакансия',
            'Блок', 'Подразделение', 'Подразделение3', 'Месяц', 'Причина', 'Значение',
        ]]

        return out


class declined_open:
    spec = PipelineSpec(
        excel_name='declined_open.xlsx',
        db_table='hr_declined_open',
        db_updated_at=True,
        db_contract={
            'Месяц': 'month',
            'Дата': 'date',
            '№': 'n',
            'ФИО': 'fio',
            'Ответственный за вакансию': 'holder',
            'ID вакансии': 'id',
            'Вакансия': 'vacancy',
            'Блок': 'block',
            'Подразделение': 'unit',
            'Подразделение3': 'unit3',
            'Статус вакансии': 'status',
            'Причина': 'reason',
            'Значение': 'value',
        },
        table_adapter='pandas',
    )

    BASE_COLS = (
        '№', 'ФИО', 'Ответственный за вакансию', 'ID вакансии', 'Вакансия', 'Блок',
        'Подразделение', 'Подразделение3', 'Статус вакансии',
        'Отсутствие требуемых навыков и квалификации',
        'Кандидат не ищет работу',
        'Отказ по формальным критериям',
        'Не согласован СБ',
        'Не прошел медосмотр',
        'Несоответствие зарплатных ожиданий ',
        'Не устраивает социальный пакет/льготы',
        'Не подходят условия труда ',
        'Не готов работать в офисе (удалёнка) ',
        'Не готов к релокации',
        'Не подходит график работы',
        'Не подходит уровень позиции / неинтересные задачи',
        'Контроффер',
        'Новый',
    )

    PREPARE_BASE_SCHEMA = {
        'columns': {
            'Месяц': {'type': 'date', 'nullable': False},
            'Дата': {'type': 'date', 'nullable': False},
            '№': {'type': 'str', 'nullable': False},
            'ФИО': 'str',
            'Ответственный за вакансию': 'str',
            'ID вакансии': {'type': 'str', 'nullable': False},
            'Вакансия': 'str',
            'Блок': 'str',
            'Подразделение': 'str',
            'Подразделение3': 'str',
            'Статус вакансии': 'str',
            'Отсутствие требуемых навыков и квалификации': 'datetime',
            'Кандидат не ищет работу': 'datetime',
            'Отказ по формальным критериям': 'datetime',
            'Не согласован СБ': 'datetime',
            'Не прошел медосмотр': 'datetime',
            'Несоответствие зарплатных ожиданий ': 'datetime',
            'Не устраивает социальный пакет/льготы': 'datetime',
            'Не подходят условия труда ': 'datetime',
            'Не готов работать в офисе (удалёнка) ': 'datetime',
            'Не готов к релокации': 'datetime',
            'Не подходит график работы': 'datetime',
            'Не подходит уровень позиции / неинтересные задачи': 'datetime',
            'Контроффер': 'datetime',
        },
        'drop_other': True,
    }

    @classmethod
    def prepare_base(cls, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in cls.BASE_COLS if c not in df.columns]
        if missing:
            raise ValueError(f'Missing required columns: {missing}')

        src = df[list(cls.BASE_COLS)].copy()

        curr_new = pd.to_datetime(src['Новый'], errors='coerce', format='mixed')
        if curr_new.notna().sum() == 0:
            raise ValueError("Cannot derive current year from source column 'Новый'")

        curr_yr = int(curr_new.max().year)

        tail_cols = list(src.columns[src.columns.get_loc('Статус вакансии') + 1:])
        if not tail_cols:
            raise ValueError("Cannot build 'Отобрано': no columns after 'Статус вакансии'")

        new_dates = pd.to_datetime(src['Новый'], errors='coerce', format='mixed')
        fallback_tail = src[tail_cols].apply(
            lambda s: pd.to_datetime(s, errors='coerce', format='mixed')
        )
        fallback_first = fallback_tail.bfill(axis=1).iloc[:, 0]

        src['Отобрано'] = new_dates.combine_first(fallback_first)
        src['Месяц'] = month_start_date(src['Отобрано'])

        src = src[
            (date_year(src['Месяц']) == curr_yr)
            & (src['Статус вакансии'] == 'Открыта')
        ].copy()

        src['Дата'] = as_date(src['Отобрано'])
        src = src.drop(columns=['Новый', 'Отобрано'])

        ordered_cols = [
            'Месяц', 'Дата', '№', 'ФИО', 'Ответственный за вакансию',
            'ID вакансии', 'Вакансия', 'Блок', 'Подразделение', 'Подразделение3',
            'Статус вакансии',
            'Отсутствие требуемых навыков и квалификации',
            'Кандидат не ищет работу',
            'Отказ по формальным критериям',
            'Не согласован СБ',
            'Не прошел медосмотр',
            'Несоответствие зарплатных ожиданий ',
            'Не устраивает социальный пакет/льготы',
            'Не подходят условия труда ',
            'Не готов работать в офисе (удалёнка) ',
            'Не готов к релокации',
            'Не подходит график работы',
            'Не подходит уровень позиции / неинтересные задачи',
            'Контроффер',
        ]

        src = src[ordered_cols]
        return apply_column_types(src, cls.PREPARE_BASE_SCHEMA)

    @classmethod
    def collapse_duplicate_groups(cls, df: pd.DataFrame) -> pd.DataFrame:
        keys = ['Месяц', '№', 'ID вакансии']

        if df.empty:
            return df.copy()

        work = df.copy()
        value_cols = [c for c in work.columns if c not in keys]
        work['_nn'] = work[value_cols].notna().sum(axis=1)

        g = work.groupby(keys, dropna=False, sort=False)
        idx_min = g['_nn'].idxmin()
        idx_max = g['_nn'].idxmax()

        row_min = work.loc[idx_min].copy().set_index(keys)
        row_max = work.loc[idx_max].copy().set_index(keys)

        out = row_min.copy()

        split_at = list(work.columns).index('Блок')
        ordered_cols = [c for c in work.columns if c != '_nn']
        right_side_cols = ordered_cols[split_at:]

        for c in right_side_cols:
            if c in keys:
                continue

            left = row_min[c]
            right = row_max[c]
            out[c] = left.where(left.notna() & (right.isna() | (left <= right)), right)

        out = out.reset_index()
        return out.drop(columns=['_nn'])

    @classmethod
    def run(cls, ctx):
        df = ctx.get_result('prepare_funnel')
        df = cls.prepare_base(df)
        collapsed = cls.collapse_duplicate_groups(df)
        collapsed = collapsed.rename(columns=REASON_RENAME_MAP)

        id_cols = [
            'Месяц', 'Дата', '№', 'ФИО', 'Ответственный за вакансию',
            'ID вакансии', 'Вакансия', 'Блок', 'Подразделение',
            'Подразделение3', 'Статус вакансии',
        ]

        out = (
            collapsed
            .melt(
                id_vars=id_cols,
                value_vars=[c for c in collapsed.columns if c not in id_cols],
                var_name='Причина',
                value_name='Значение',
            )
            .loc[lambda x: x['Значение'].notna()]
            .copy()
        )

        return out


class funnel_open:
    spec = PipelineSpec(
        excel_name='funn_open.xlsx',
        db_table='hr_funnel_open',
        db_updated_at=True,
        db_contract={
            'Месяц': 'month',
            '№': 'n',
            'Источник': 'source',
            'ФИО': 'fio',
            'Ответственный за вакансию': 'holder',
            'ID вакансии': 'id',
            'Вакансия': 'vacancy',
            'Блок': 'block',
            'Подразделение': 'unit',
            'Подразделение3': 'unit3',
            'Приоритет': 'priority',
            'Дата открытия вакансии': 'date_opened',
            'Статус вакансии': 'status',
            'Причина возникновения вакансии': 'reason',
            'Категории профессий': 'category',
            'Уровень': 'level',
            'Значение': 'value',
            'sort_level': 'sort_level',
            'Дата': 'date',
        },
        table_adapter='pandas',
    )

    BASE_COLS = (
        '№', 'ФИО', 'ID вакансии', 'Вакансия', 'Блок', 'Подразделение', 'Подразделение3',
        'Приоритет', 'Дата открытия вакансии', 'Ответственный за вакансию', 'Статус вакансии',
        'Источник', 'Первичное интервью (Рекрутер)', 'ПИ - согласован',
        'Резюме на рассмотрении у представителя Заказчика',
        'Предст. Заказчика - резюме согласовано',
        'Интервью 1', 'Инт 1 - согласован',
        'Согласование оффера: Представитель заказчика',
        'Предст. Заказчика - оффер согласован',
        'СБ организации', 'СБ орг. - проверен',
        'Причина возникновения вакансии', 'Категории профессий',
        'Отобрано', 'Месяц',
    )

    STAGE_COLS = (
        'Отобрано', 'Рекрутер', 'Заказчик', 'Интервью', 'Оффер', 'Проверка',
    )

    PREPARE_BASE_SCHEMA = {
        'columns': {
            'Месяц': {'type': 'date', 'nullable': False},
            '№': {'type': 'str', 'nullable': False},
            'Источник': 'str',
            'ФИО': 'str',
            'Ответственный за вакансию': 'str',
            'ID вакансии': {'type': 'str', 'nullable': False},
            'Вакансия': 'str',
            'Блок': 'str',
            'Подразделение': 'str',
            'Подразделение3': 'str',
            'Приоритет': 'str',
            'Дата открытия вакансии': 'date',
            'Статус вакансии': 'str',
            'Причина возникновения вакансии': 'str',
            'Категории профессий': 'str',
            'Отобрано': 'datetime',
            'Рекрутер': 'datetime',
            'Заказчик': 'datetime',
            'Интервью': 'datetime',
            'Оффер': 'datetime',
            'Проверка': 'datetime',
        },
        'drop_other': True,
    }

    @classmethod
    def prepare_base(cls, df: pd.DataFrame) -> pd.DataFrame:
        if 'Новый' not in df.columns:
            raise ValueError("Missing required column: 'Новый'")

        src = df.copy()

        tail_cols = list(src.columns[src.columns.get_loc('Новый'):])
        if not tail_cols:
            raise ValueError("Cannot build 'Отобрано': no columns at or after 'Новый'")

        stage_tail = src[tail_cols].apply(
            lambda s: pd.to_datetime(s, errors='coerce', format='mixed')
        )
        src['Отобрано'] = stage_tail.bfill(axis=1).iloc[:, 0]
        src['Месяц'] = month_start_date(src['Отобрано'])

        curr_new = pd.to_datetime(src['Новый'], errors='coerce')
        if curr_new.notna().sum() == 0:
            raise ValueError("Cannot derive current year from source column 'Новый'")

        curr_yr = int(curr_new.max().year)
        src = src[date_year(src['Месяц']) == curr_yr].copy()

        missing = [c for c in cls.BASE_COLS if c not in src.columns]
        if missing:
            raise ValueError(f'Missing required columns: {missing}')

        df = src[list(cls.BASE_COLS)].copy()

        df = combine_stage_pair(
            df, 'Рекрутер',
            'Первичное интервью (Рекрутер)',
            'ПИ - согласован',
        )
        df = combine_stage_pair(
            df, 'Заказчик',
            'Резюме на рассмотрении у представителя Заказчика',
            'Предст. Заказчика - резюме согласовано',
        )
        df = combine_stage_pair(
            df, 'Интервью',
            'Интервью 1',
            'Инт 1 - согласован',
        )
        df = combine_stage_pair(
            df, 'Оффер',
            'Согласование оффера: Представитель заказчика',
            'Предст. Заказчика - оффер согласован',
        )
        df = combine_stage_pair(
            df, 'Проверка',
            'СБ организации',
            'СБ орг. - проверен',
        )

        df = apply_column_types(df, cls.PREPARE_BASE_SCHEMA)

        ordered_cols = [
            'Месяц', '№', 'Источник', 'ФИО', 'Ответственный за вакансию',
            'ID вакансии', 'Вакансия', 'Блок', 'Подразделение', 'Подразделение3',
            'Приоритет', 'Дата открытия вакансии', 'Статус вакансии',
            'Причина возникновения вакансии', 'Категории профессий',
            *cls.STAGE_COLS,
        ]
        return df[ordered_cols]

    # PQ reference shape kept in mind:
    # - sparse row gives the base
    # - from 'Блок' onward take elementwise minimum across sparse/dense pair
    # - if any duplicate row is already terminally closed, keep final status = 'Закрыта'
    @classmethod
    def collapse_duplicate_groups(cls, df: pd.DataFrame) -> pd.DataFrame:
        keys = ['Месяц', '№', 'ID вакансии']

        if df.empty:
            return df.copy()

        work = df.copy()
        value_cols = [c for c in work.columns if c not in keys]
        work['_nn'] = work[value_cols].notna().sum(axis=1)
        work['_is_closed'] = work['Статус вакансии'].eq('Закрыта')

        g = work.groupby(keys, dropna=False, sort=False)
        idx_min = g['_nn'].idxmin()
        idx_max = g['_nn'].idxmax()
        has_closed = g['_is_closed'].any()

        row_min = work.loc[idx_min].copy().set_index(keys)
        row_max = work.loc[idx_max].copy().set_index(keys)

        out = row_min.copy()

        split_at = list(work.columns).index('Блок')
        ordered_cols = [c for c in work.columns if c not in {'_nn', '_is_closed'}]
        right_side_cols = ordered_cols[split_at:]

        for c in right_side_cols:
            if c in keys:
                continue

            left = row_min[c]
            right = row_max[c]
            out[c] = left.where(left.notna() & (right.isna() | (left <= right)), right)

        out['Статус вакансии'] = out['Статус вакансии'].where(~has_closed, 'Закрыта')

        out = out.reset_index()
        return out.drop(columns=['_nn', '_is_closed'])

    @classmethod
    def run(cls, ctx):
        df = ctx.get_result('prepare_funnel')
        df = cls.prepare_base(df)
        collapsed = cls.collapse_duplicate_groups(df)

        flt2 = collapsed[
            collapsed['Дата открытия вакансии'] >= MIN_VACANCY_OPEN_DATE
        ].copy()

        out = (
            flt2
            .melt(
                id_vars=[c for c in flt2.columns if c not in cls.STAGE_COLS],
                value_vars=list(cls.STAGE_COLS),
                var_name='Уровень',
                value_name='Значение',
            )
            .loc[lambda x: x['Значение'].notna()]
            .copy()
        )

        sort_map = {
            'Отобрано': 0,
            'Рекрутер': 1,
            'Заказчик': 2,
            'Интервью': 3,
            'Проверка': 4,
            'Оффер': 5,
        }

        out['sort_level'] = out['Уровень'].map(sort_map)
        out['Дата'] = as_date(out['Значение'])
        return out


# === Shared by staff and recruiters -- error-value cleaning and
# dynamic, column-name-pattern-driven type coercion. ===

ERROR_LITERALS = {
    '#N/A', '#N/A!', '#DIV/0', '#DIV/0!', '#VALUE!', '#REF!', '#NAME?',
    '#NUM!', '#NULL!', '#GETTING_DATA', '#SPILL!', '#CALC!', '#FIELD!',
    '#BLOCKED!', '#UNKNOWN!',
    '#Н/Д', '#Н/Д!', '#ДЕЛ/0', '#ДЕЛ/0!', '#ЗНАЧ!', '#ССЫЛКА!', '#ИМЯ?',
    '#ЧИСЛО!', '#ПУСТО!',
}


def clean_error_value(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str):
        s = value.strip()
        if s == '':
            return value
        normalized = s.upper().replace(' ', '')
        if normalized in ERROR_LITERALS:
            return None
    return value


def replace_error_values(df):
    return df.apply(lambda col: col.map(clean_error_value))


def trim_text_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, str):
        s = value.strip()
        return None if s == '' else s
    return value


def _type_error_location(file_name, sheet_name):
    if file_name is None:
        return ''
    location = str(file_name)
    if sheet_name is not None:
        location = f'{location}::{sheet_name}'
    return f'{location}: '


def strict_numeric(series, column_name, file_name=None, sheet_name=None):
    converted = pd.to_numeric(series, errors='coerce')
    bad_mask = converted.isna() & series.notna() & series.astype('string').str.strip().ne('')
    if bad_mask.any():
        sample = series[bad_mask].astype('string').head(10).tolist()
        location = _type_error_location(file_name, sheet_name)
        raise ValueError(f"{location}non-numeric values found in {column_name!r}. Samples: {sample!r}")
    return converted


def strict_int(series, column_name, file_name=None, sheet_name=None):
    converted = strict_numeric(series, column_name, file_name, sheet_name)
    non_null = converted.dropna()
    bad_mask = ~np.isclose(non_null % 1, 0)
    if bad_mask.any():
        sample = non_null[bad_mask].head(10).tolist()
        location = _type_error_location(file_name, sheet_name)
        raise ValueError(f"{location}non-integer values found in {column_name!r}. Samples: {sample!r}")
    return converted.astype('Int64')


def strict_date(series, column_name, file_name=None, sheet_name=None):
    converted = pd.to_datetime(series, errors='coerce')
    bad_mask = converted.isna() & series.notna() & series.astype('string').str.strip().ne('')
    if bad_mask.any():
        sample = series[bad_mask].astype('string').head(10).tolist()
        location = _type_error_location(file_name, sheet_name)
        raise ValueError(f"{location}invalid date values found in {column_name!r}. Samples: {sample!r}")
    return converted.dt.date


DYNAMIC_TYPE_PATTERNS = {
    'date': ['Дата'],
    'float': ['Цена', 'Количество', 'Кол-во', 'Факт', 'Сумма', 'Выручка'],
    'int': ['Клиент_'],
}


def apply_dynamic_types(df, file_name=None, sheet_name=None, type_patterns=None):
    df = df.copy()
    names = list(df.columns)
    type_patterns = type_patterns or DYNAMIC_TYPE_PATTERNS

    def contains_any(name, patterns):
        name_lower = str(name).lower()
        return any(pattern.lower() in name_lower for pattern in patterns)

    date_cols = [name for name in names if contains_any(name, type_patterns.get('date', []))]
    float_cols = [name for name in names if contains_any(name, type_patterns.get('float', []))]
    int_cols = [name for name in names if contains_any(name, type_patterns.get('int', []))]
    text_cols = [name for name in names if name not in date_cols and name not in float_cols and name not in int_cols]

    for col in date_cols:
        df[col] = strict_date(df[col], col, file_name, sheet_name)
    for col in float_cols:
        df[col] = strict_numeric(df[col], col, file_name, sheet_name)
    for col in int_cols:
        df[col] = strict_int(df[col], col, file_name, sheet_name)
    for col in text_cols:
        df[col] = df[col].astype('string')

    return df


class staff:
    spec = PipelineSpec(
        excel_name='staff.xlsx',
        db_table='hr_staff',
        db_updated_at=True,
        db_contract={
            'Блок': 'block',
            'Подразделение': 'unit',
            'Подразделение3': 'unit3',
            'Должность (специальность, профессия)': 'position',
            'Месяц': 'month',
            'Показатель': 'dimension',
            'Значение': 'value',
        },
        publish_result=True,
        table_adapter='pandas',
    )
    @classmethod
    def is_staff_sheet(cls, raw):
        return not raw.empty and raw.shape[1] > 0 and any(value == 'Блок' for value in raw.iloc[:50, 0].tolist())

    @classmethod
    def is_block_one(cls, value):
        if value is None or pd.isna(value):
            return False
        if value == '1' or value == 1:
            return True
        if isinstance(value, (int, float, np.integer, np.floating)) and not pd.isna(value):
            return float(value) == 1.0
        return False

    @classmethod
    def normalize_group_text(cls, value):
        if value is None or pd.isna(value):
            return '-- нет --'
        return str(value).strip()

    @classmethod
    def process_sheet(cls, file_name, sheet_name, raw):
        final_cols = [
            'Блок', 'Подразделение', 'Подразделение3',
            'Должность (специальность, профессия)', 'Месяц', 'Показатель', 'Значение',
        ]
        raw = raw.copy()
        if raw.empty:
            return pd.DataFrame(columns=final_cols)

        period_idx = next(
            (
                idx
                for idx, row in enumerate(raw.itertuples(index=False, name=None))
                if 'Период:' in ''.join(value for value in row if isinstance(value, str))
            ),
            None,
        )
        if period_idx is None:
            raise ValueError(f"{file_name}::{sheet_name}: row containing 'Период:' not found")

        trimmed = raw.iloc[period_idx:].reset_index(drop=True).copy()
        period_line = next(
            (value for value in trimmed.iloc[0].tolist() if isinstance(value, str) and value.startswith('Период:')),
            None,
        )
        if period_line is None:
            raise ValueError(
                f'{file_name}::{sheet_name}: first retained row does not contain '
                f'a period line starting with "Период:"'
            )

        period_value = period_line.split(':', 1)[1].strip()
        try:
            month = month_start_date(parse_date_value(period_value))
        except ValueError as exc:
            raise ValueError(f'{file_name}::{sheet_name}: cannot parse period date from {period_value!r}') from exc

        header_idx = next(
            (
                idx
                for idx, row in enumerate(trimmed.itertuples(index=False, name=None))
                if any(cell == 'Блок' for cell in row)
            ),
            None,
        )
        if header_idx is None:
            raise ValueError(f"{file_name}::{sheet_name}: row containing value 'Блок' not found")

        hdr = trimmed.iloc[header_idx:].copy().reset_index(drop=True)
        hdr.columns = [normalize_header_label(value) for value in hdr.iloc[0].tolist()]
        hdr = hdr.iloc[1:].reset_index(drop=True)

        required_cols = [
            'Блок',
            'Структурное подразделение\n(2 уровень)',
            'Структурное подразделение\n(3 уровень)',
            'Должность (специальность, профессия)',
            'ФИО',
            'Кол-во шт. ед.',
            'Кол-во занимаемых шт. ед.',
        ]
        hdr = select_unique_required_columns(
            hdr,
            required_cols,
            f'{file_name}::{sheet_name}',
        )

        out = (
            apply_dynamic_types(
                hdr.loc[
                    hdr['Блок'].notna() & hdr['ФИО'].notna() & ~hdr['Блок'].map(cls.is_block_one),
                    [
                        'Блок',
                        'Структурное подразделение\n(2 уровень)',
                        'Структурное подразделение\n(3 уровень)',
                        'Должность (специальность, профессия)',
                        'Кол-во шт. ед.',
                        'Кол-во занимаемых шт. ед.',
                    ],
                ].copy(),
                file_name,
                sheet_name,
            )
            .rename(columns={
                'Кол-во шт. ед.': 'План',
                'Кол-во занимаемых шт. ед.': 'Факт',
                'Структурное подразделение\n(2 уровень)': 'Подразделение',
                'Структурное подразделение\n(3 уровень)': 'Подразделение3',
            })
            .assign(
                Подразделение=lambda x: x['Подразделение'].map(cls.normalize_group_text),
                Подразделение3=lambda x: x['Подразделение3'].map(cls.normalize_group_text),
                **{'Должность (специальность, профессия)': lambda x: x['Должность (специальность, профессия)'].map(trim_text_value)},
            )
            .groupby(['Блок', 'Подразделение', 'Подразделение3', 'Должность (специальность, профессия)'], dropna=False, as_index=False)
            .agg({'План': 'sum', 'Факт': 'sum'})
            .assign(Месяц=month)
            .melt(
                id_vars=['Блок', 'Подразделение', 'Подразделение3', 'Должность (специальность, профессия)', 'Месяц'],
                value_vars=['План', 'Факт'],
                var_name='Показатель',
                value_name='Значение',
            )
            .loc[lambda x: x['Значение'].notna() & x['Значение'].ne(0)]
            .copy()
        )
        return out[final_cols]

    @classmethod
    def process_workbook(cls, resource):
        final_cols = [
            'Блок', 'Подразделение', 'Подразделение3',
            'Должность (специальность, профессия)', 'Месяц', 'Показатель', 'Значение',
        ]
        parts = [
            cls.process_sheet(resource.file_path, sheet_name, raw)
            for sheet_name in resource.sheets
            for raw in [replace_error_values(sheet_to_raw_dataframe(resource, sheet_name))]
            if cls.is_staff_sheet(raw)
        ]
        if not parts:
            raise ValueError(f'{resource.file_path}: no valid staff sheets found')
        return pd.concat(parts, ignore_index=True)[final_cols]

    @classmethod
    def run(cls, ctx, *, source):
        out = cls.process_workbook(source)
        months = [d for d in pd.to_datetime(out['Месяц'], errors='coerce').dt.date.tolist() if pd.notna(d)]
        if not months:
            raise ValueError('staff: cannot publish month bounds from empty result')
        ctx.set_shared('staff_min_month', min(months))
        ctx.set_shared('staff_max_month', max(months))
        log.info('[staff] published month bounds: %s .. %s', min(months), max(months))
        return out


class recruiters:
    spec = PipelineSpec(
        excel_name='recruiters.xlsx',
        db_table='hr_recruiters',
        db_updated_at=True,
        db_contract={
            'Id': 'id',
            'Название вакансии': 'vacancy',
            'ЮР': 'jur',
            'Блок': 'block',
            'Подразделение': 'unit',
            'Подразделение3': 'unit3',
            'Ответственный за вакансию': 'holder',
            'Месяц': 'month',
            'Статус': 'status',
        },
        table_adapter='pandas',
    )
    source_sheet = 'Данные'
    allowed_statuses = {'Открыта', 'Закрыта', 'Черновик'}

    @classmethod
    def expand_month_statuses(cls, open_month, close_month, status: str) -> list[tuple[object, str]]:
        if open_month is None:
            raise ValueError('Дата открытия is required for recruiters month expansion')

        months = pd.date_range(open_month, close_month, freq='MS').date.tolist()
        if not months:
            raise ValueError(f'Invalid recruiters month range: {open_month!r} .. {close_month!r}')
        if status == 'Закрыта':
            return [(month, 'Открыта') for month in months[:-1]] + [(months[-1], 'Закрыта')]
        if status == 'Открыта':
            return [(month, 'Открыта') for month in months]
        if status == 'Потребность':
            return [(month, 'Потребность') for month in months]
        return [(month, status) for month in months]

    @classmethod
    def process_workbook(cls, resource, mind) -> pd.DataFrame:
        final_cols = ['Id', 'Название вакансии', 'ЮР', 'Блок', 'Подразделение', 'Подразделение3', 'Ответственный за вакансию', 'Месяц', 'Статус']
        raw = replace_error_values(sheet_to_raw_dataframe(resource, cls.source_sheet))
        if raw.empty:
            return pd.DataFrame(columns=final_cols)

        header_idx = next(
            (
                idx
                for idx, row in enumerate(raw.itertuples(index=False, name=None))
                if any(cell == 'Название вакансии' for cell in row)
            ),
            None,
        )
        if header_idx is None:
            raise ValueError(f"{resource.file_path}::{cls.source_sheet}: row containing value 'Название вакансии' not found")

        hdr = raw.iloc[header_idx:].copy().reset_index(drop=True)
        hdr.columns = [normalize_header_label(value) for value in hdr.iloc[0].tolist()]
        hdr = hdr.iloc[1:].reset_index(drop=True)

        required_cols = ['Id', 'Название вакансии', 'Заказчик', 'Ответственный рекрутер', 'Статус', 'Дата открытия', 'Дата закрытия', 'Плановое закрытие']
        hdr = select_unique_required_columns(
            hdr,
            required_cols,
            f'{resource.file_path}::{cls.source_sheet}',
        )

        base = split_customer(apply_dynamic_types(hdr, resource.file_path, cls.source_sheet))
        base = base.drop(columns=['Заказчик'])
        flt1 = base[
            base['ЮР'].eq(TARGET_LEGAL_ENTITY)
            & base['Статус'].isin(cls.allowed_statuses)
        ].copy()
        if flt1.empty:
            return pd.DataFrame(columns=final_cols)

        max_values = [
            value
            for value in flt1['Дата открытия'].tolist() + flt1['Дата закрытия'].tolist()
            if value is not None and not pd.isna(value)
        ]
        if not max_values:
            raise ValueError(f'{resource.file_path}::{cls.source_sheet}: cannot determine max month from opening and closing dates')
        maxd = month_start_date(max(max_values))

        bt = flt1.assign(
            Блок=lambda x: x['Блок'].map(staff.normalize_group_text),
            Подразделение=lambda x: x['Подразделение'].map(staff.normalize_group_text),
            Подразделение3=lambda x: x['Подразделение3'].map(staff.normalize_group_text),
            **{'Ответственный рекрутер': lambda x: x['Ответственный рекрутер'].map(staff.normalize_group_text)},
            **{'Дата открытия': lambda x: x['Дата открытия'].map(lambda value: maxd if value is None or pd.isna(value) else month_start_date(value))},
            **{'Дата закрытия': lambda x: x['Дата закрытия'].map(lambda value: maxd if value is None or pd.isna(value) else month_start_date(value))},
            **{'Плановое закрытие': lambda x: x['Плановое закрытие'].map(month_start_date)},
            Статус=lambda x: x['Статус'].replace({'Черновик': 'Потребность'}),
        )

        branch1 = pd.DataFrame(
            [
                {
                    'Id': row['Id'],
                    'Название вакансии': row['Название вакансии'],
                    'ЮР': row['ЮР'],
                    'Блок': row['Блок'],
                    'Подразделение': row['Подразделение'],
                    'Подразделение3': row['Подразделение3'],
                    'Ответственный рекрутер': row['Ответственный рекрутер'],
                    'Месяц': month,
                    'Статус': month_status,
                }
                for row in bt.drop(columns=['Плановое закрытие']).to_dict('records')
                for month, month_status in cls.expand_month_statuses(row['Дата открытия'], row['Дата закрытия'], row['Статус'])
            ]
        )

        branch2 = bt.rename(columns={'Плановое закрытие': 'Месяц'}).drop(columns=['Дата открытия', 'Дата закрытия']).assign(
            Статус='План по закрытию',
            Месяц=lambda x: x['Месяц'].map(month_start_date),
        )

        out = pd.concat([branch1, branch2], ignore_index=True).assign(
            **{'Ответственный за вакансию': lambda x: x['Ответственный рекрутер']}
        ).drop(columns=['Ответственный рекрутер'])
        out = out[out['Месяц'].notna() & out['Месяц'].ge(mind) & out['Месяц'].le(maxd)].copy()
        return out[final_cols]

    @classmethod
    def run(cls, ctx, *, source):
        mind = ctx.require_shared('staff_min_month')
        return cls.process_workbook(source, mind)


def read_ssch_sheet(file_set, selected_file, *, sheet=0):
    metadata = file_set.read_excel_row_metadata(selected_file, sheet=sheet, mode='outline')

    with file_set.open_file(selected_file) as src:
        df = pd.read_excel(
            src,
            sheet_name=sheet,
            header=None,
            engine='openpyxl',
            engine_kwargs={'read_only': True, 'data_only': True},
        )

    if len(metadata) != len(df):
        log.warning(
            '%s: outline row count mismatch: %s rows from XML vs %s rows from pd.read_excel '
            '-- some rows may get a None Attribute value',
            selected_file.relative_path, len(metadata), len(df),
        )

    # pd.read_excel(header=None) always materializes starting from the
    # sheet's true row 1, even when that row is entirely absent from the
    # XML (confirmed directly: a genuinely untouched leading row still
    # produces a NaN row in the DataFrame, while metadata correctly has
    # no entry for it) -- so the alignment origin is always 1, never
    # metadata's own first key. Using min(metadata) here would silently
    # shift every row's outline_level by however many leading rows are
    # missing from the XML but still present in df.
    first_row = 1
    aligned = align_row_metadata(metadata, first_row=first_row, n_rows=len(df))
    df.insert(0, 'Attribute', aligned)
    return df


class ssch:
    spec = PipelineSpec(
        excel_name='ssch.xlsx',
        db_table='hr_ssch',
        db_updated_at=True,
        # lev.* columns vary by hierarchy depth, discovered only after
        # processing -- added via get_dynamic_db_contract below, not by
        # mutating this (frozen) spec.
        db_contract={
            'Должность': 'position',
            'Месяц': 'month',
            'Показатель': 'dimension',
            'Значение': 'value',
        },
        table_adapter='pandas',
    )

    @classmethod
    def parse_report_month(cls, raw: pd.DataFrame, file_name: str):
        report_line = next(
            (
                s
                for row in raw.itertuples(index=False, name=None)
                for value in row
                if isinstance(value, str)
                for s in [value.strip()]
                if s.startswith('Период отчета:')
            ),
            None,
        )
        if report_line is None:
            raise ValueError(f'{file_name}: report period line not found')

        m = re.search(r'(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})', report_line)
        if m is None:
            raise ValueError(f'{file_name}: cannot parse report month from {report_line!r}')

        try:
            return month_start_date(parse_date_value(m.group(1)))
        except ValueError as exc:
            raise ValueError(f'{file_name}: cannot parse report month from {report_line!r}') from exc

    @classmethod
    def read_file(cls, file_set, selected_file):
        raw = read_ssch_sheet(file_set, selected_file, sheet=0)
        if not isinstance(raw, pd.DataFrame):
            raise TypeError(f'{selected_file.relative_path}: read_ssch_sheet must return pandas DataFrame')
        month = cls.parse_report_month(raw, selected_file.relative_path)
        return raw.copy(), month

    @classmethod
    def find_anchor_row(cls, raw: pd.DataFrame, anchor: str, file_name: str) -> int:
        for idx, row in raw.iterrows():
            if any(value == anchor for value in row.tolist()):
                return int(idx)
        raise ValueError(f'{file_name}: anchor row {anchor!r} not found')

    @classmethod
    def promote_headers_from_row(cls, raw: pd.DataFrame, header_idx: int) -> pd.DataFrame:
        df = raw.iloc[header_idx:].copy().reset_index(drop=True)
        df.columns = [normalize_header_label(value) for value in df.iloc[0].tolist()]
        return df.iloc[1:].reset_index(drop=True)

    @classmethod
    def attribute_column_name(cls, df: pd.DataFrame):
        if 'Attribute' in df.columns:
            return 'Attribute'
        return df.columns[0]

    @classmethod
    def normalize_outline_level(cls, value) -> int:
        if value is None or pd.isna(value):
            return 0

        if isinstance(value, (int, np.integer)):
            return int(value)

        if isinstance(value, float):
            return 0 if pd.isna(value) else int(value)

        if isinstance(value, str):
            s = value.strip()
            if s == '':
                return 0
            try:
                return int(float(s))
            except Exception:
                return 0

        if isinstance(value, dict):
            raw = value.get('outlineLevel', 0)
        else:
            getter = getattr(value, 'get', None)
            raw = getter('outlineLevel', 0) if callable(getter) else 0

        try:
            return int(raw or 0)
        except Exception:
            return 0

    @classmethod
    def trim_hierarchy_text(cls, value):
        if pd.isna(value):
            return pd.NA
        s = str(value).strip()
        if s == '':
            return pd.NA
        s = re.split(r'\s+до\s+|\s*\(до', s, maxsplit=1)[0].strip()
        return pd.NA if s == '' else s

    @classmethod
    def drop_empty_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[:, ~df.isna().all(axis=0)].copy()

    @classmethod
    def build_hierarchy(cls, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        if df.empty:
            raise ValueError(f'{file_name}: no data rows after preprocessing')

        levels = pd.to_numeric(df.iloc[:, 0], errors='raise').astype(int).tolist()
        labels = df.iloc[:, 1].tolist()
        metrics = df.iloc[:, 2:].reset_index(drop=True)

        max_level = max(levels) if levels else 0

        metric_cols = metrics.columns.tolist()

        if max_level <= 0:
            records = [[label, *metrics.iloc[i].tolist()] for i, label in enumerate(labels)]
            return pd.DataFrame(records, columns=['Должность', *metric_cols])

        current = [None] * max_level
        records: list[list[object]] = []

        for i, (level, label) in enumerate(zip(levels, labels)):
            if level <= 0 or level > max_level:
                raise ValueError(
                    f'{file_name}: invalid outline level {level} at row {i + 1} -- a '
                    f'missing or None outline value normalizes to 0 (see normalize_outline_level), '
                    f'so this is likely the row the earlier "outline row count mismatch" warning '
                    f'(if any) was about'
                )

            current = current[:level - 1] + [label] + [None] * (max_level - level)
            next_level = levels[i + 1] if i + 1 < len(levels) else 0
            if next_level > level:
                continue

            non_null = [x for x in current if pd.notna(x)]
            if not non_null:
                continue

            lev_values = non_null[:-1] + [None] * (max_level - len(non_null))
            position = non_null[-1]
            row = [*lev_values, position, *metrics.iloc[i].tolist()]
            records.append(row)

        level_cols = [f'lev.{i}' for i in range(1, max_level)]
        return pd.DataFrame(records, columns=[*level_cols, 'Должность', *metric_cols])

    @classmethod
    def strict_numeric(cls, series: pd.Series, file_name: str, metric_series: pd.Series) -> pd.Series:
        try:
            return pd.to_numeric(series, errors='raise')
        except Exception:
            bad_mask = pd.to_numeric(series, errors='coerce').isna() & series.notna()
            sample = pd.DataFrame({
                'Показатель': metric_series[bad_mask].astype('string'),
                'raw_value': series[bad_mask].astype('string'),
            }).head(10)
            raise ValueError(
                f"{file_name}: non-numeric values found in 'Значение'. Samples: "
                + repr(sample.to_dict(orient='records'))
            )

    @classmethod
    def process_file(cls, file_set, selected_file) -> pd.DataFrame:
        raw, month = cls.read_file(file_set, selected_file)
        anchor_idx = cls.find_anchor_row(raw, 'Организация', selected_file.relative_path)
        hdr = cls.promote_headers_from_row(raw, anchor_idx)
        hdr = hdr.iloc[3:].reset_index(drop=True)
        hdr = cls.drop_empty_columns(hdr)
        check_no_duplicate_headers(list(hdr.columns), selected_file.relative_path)

        if 'Организация' not in hdr.columns:
            raise ValueError(f"{selected_file.relative_path}: promoted table does not contain column 'Организация'")

        flt0 = hdr[(hdr['Организация'].notna()) & (hdr['Организация'] != 'Итого')].copy()
        flt0 = flt0.drop(columns=['Уволено', 'Коэфф. текучести'], errors='ignore')

        attr_col = cls.attribute_column_name(flt0)
        flt0[attr_col] = flt0[attr_col].map(cls.normalize_outline_level).astype('Int64')

        hierarchy_src = flt0.rename(columns={attr_col: 'outline_level'})
        hierarchy = cls.build_hierarchy(hierarchy_src, selected_file.relative_path)

        level_cols = [c for c in hierarchy.columns if str(c).startswith('lev.')]
        id_cols = [*level_cols, 'Должность']
        out = (
            hierarchy.melt(
                id_vars=id_cols,
                value_vars=[c for c in hierarchy.columns if c not in id_cols],
                var_name='Показатель',
                value_name='Значение',
            )
            .loc[lambda x: x['Значение'].notna()]
            .copy()
        )

        out['Месяц'] = month
        for c in id_cols:
            out[c] = out[c].map(cls.trim_hierarchy_text)

        out['Значение'] = cls.strict_numeric(out['Значение'], selected_file.relative_path, out['Показатель'])
        final_cols = [*level_cols, 'Должность', 'Месяц', 'Показатель', 'Значение']
        return out[final_cols]

    @classmethod
    def run(cls, ctx, *, source):
        # No empty-file-set check -- build_file_set_resource's
        # on_empty='raise' default already fails earlier, at
        # ctx.get_resource() time.
        parts = [cls.process_file(source, f) for f in source.files]
        out = pd.concat(parts, ignore_index=True)
        level_cols = sorted(
            [c for c in out.columns if str(c).startswith('lev.')],
            key=lambda x: int(str(x).split('.')[1]),
        )
        return out[[*level_cols, 'Должность', 'Месяц', 'Показатель', 'Значение']]

    @classmethod
    def get_dynamic_db_contract(cls, out_tbl):
        # Only db_contract is dynamic; db_table/excel_name/table_adapter
        # stay static. lev.* columns vary by hierarchy depth per run.
        level_cols = [c for c in out_tbl.columns if str(c).startswith('lev.')]
        return {c: c for c in level_cols} | {
            'Должность': 'position',
            'Месяц': 'month',
            'Показатель': 'dimension',
            'Значение': 'value',
        }


FUNNEL_XLSX = latest_xlsx('funnel', pattern='*.xlsx', tracker=True)
STAFF_XLSX = latest_xlsx('staff', pattern='*.xlsx', tracker=True)
RECRUITERS_XLSX = latest_xlsx('recruiters', pattern='*.xlsx', tracker=True)
SSCH_FILES = xlsx_file_set('ssch', pattern='*.xlsx', tracker=True)

RESOURCES = {
    'funnel_xlsx': FUNNEL_XLSX,
    'staff_xlsx': STAFF_XLSX,
    'recruiters_xlsx': RECRUITERS_XLSX,
    'ssch_files': SSCH_FILES,
}


def build_context(base_path=BASE_PATH, dfs_creds=None):
    source_access = build_source_access(dfs_creds=dfs_creds)
    env = ResourceEnvironment(base_path=base_path, file_access=source_access)
    return build_resource_context(TASK_NAME, RESOURCES, PIPELINES, RUN_SEQUENCE, env)


PIPELINES = {
    'prepare_funnel': bind(prepare_funnel, source=FUNNEL_XLSX),
    'funnel_closed': funnel_closed,
    'funnel_open': funnel_open,
    'declined_close': declined_close,
    'declined_open': declined_open,
    'ssch': bind(ssch, source=SSCH_FILES),
    'staff': bind(staff, source=STAFF_XLSX),
    'recruiters': bind(recruiters, source=RECRUITERS_XLSX),
}
RUN_SEQUENCE = [
    'staff',
    'prepare_funnel',
    'funnel_closed',
    'funnel_open',
    'declined_close',
    'declined_open',
    'ssch',
    'recruiters',
]


def main(base_path=BASE_PATH, output_creds=None, pg_schema=PG_SCHEMA, dfs_creds=None, force_run=False, smb_level=logging.WARNING):
    output_creds = DEFAULT_PGCREDS if output_creds is None else output_creds
    setup_logging(TASK_NAME, smb_level=smb_level)
    return run_pipelines(
        task_name=TASK_NAME,
        build_context=lambda: build_context(base_path, dfs_creds=dfs_creds),
        pipelines=PIPELINES,
        run_sequence=RUN_SEQUENCE,
        output_excel=OUTPUT_EXCEL,
        output_db=OUTPUT_DB,
        creds=output_creds,
        pg_schema=pg_schema,
        source_change_check=SOURCE_CHANGE_CHECK,
        force_run=force_run,
    )


if __name__ == '__main__':
    main()
