# -*- coding: utf-8 -*-

from datetime import datetime, date
import logging
import re
from typing import Union

import petl as etl

from petl_util import make_cal, to_date

from task_core import (
    ResourceEnvironment,
    bind,
    build_db_resource,
    build_resource_context,
    latest_xlsx,
    resource,
    run_pipelines,
    setup_logging,
    build_source_access,
    PipelineSpec,
    SourceChangeCheckConfig,
)
try:
    from pgcreds import pgcreds as DEFAULT_PGCREDS
except ImportError:
    DEFAULT_PGCREDS = None


TASK_NAME = 'ops_support_etl'
BASE_PATH = r'\\telecom.local\dfs\Стратегическое развитие\pbi-data\ОЦО'

OUTPUT_EXCEL = True
OUTPUT_DB = True
PG_SCHEMA = 'bsr'

SOURCE_CHANGE_CHECK = SourceChangeCheckConfig(
    enabled=True,
    schema='bsr',
    table='task_scaffold_meta',
)


def parse_dt(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return None


def to_minutes(value: Union[str, int, bool, float, None]) -> int:
    if value is None:
        return 0

    if isinstance(value, (bool, int, float)):
        return int(value)
        
    units = {
        'секунд': 1 / 60,
        'секунда': 1 / 60,
        'секунды': 1 / 60,
        'минут': 1,
        'минута': 1,
        'минуты': 1,
        'час': 60,
        'часов': 60,
        'часа': 60,
    }

    total = sum(
        int(num) * units[unit]
        for num, unit in re.findall(r'(\d+)\s*([а-яА-Я]+)', value.lower())
        if unit in units
    )

    return int(total)

    
def push_date_range(ctx, pipeline_name, tbl, date_col='date', shared_name='date_ranges'):
    values = [x for x in etl.values(tbl, date_col) if x is not None]
    if not values:
        return

    ctx.push_shared(
        shared_name,
        {
            'pipeline': pipeline_name,
            'date_min': min(values),
            'date_max': max(values),
        },
    )


def build_calendar_from_ranges(ctx, shared_name='date_ranges'):
    ranges = ctx.require_shared(shared_name)

    dts_raw = min(x['date_min'] for x in ranges if x.get('date_min') is not None)
    dte_raw = max(x['date_max'] for x in ranges if x.get('date_max') is not None)

    # dts = date(dts_raw.year, dts_raw.month, 1)
    # dte = date(dte_raw.year, dte_raw.month, monthrange(dte_raw.year, dte_raw.month)[1])
    dts = date(dts_raw.year, 1, 1)
    dte = date(dte_raw.year, 12, 31)

    return make_cal(dts, dte)


OPS_XLSX = latest_xlsx('.', pattern='*.xlsx', tracker=True)

# Untracked -- matches the real system's own, already-documented state:
# no agreed fingerprint query with the strat_db view owner yet. The
# generic resource() factory (not latest_xlsx/xlsx_file_set) is the
# right one here since this isn't file-shaped at all.
STRAT_DB = resource(
    loader=lambda env: build_db_resource(creds=env.credentials['strat_db']),
    tracker=False,
)

RESOURCES = {
    'ops_xlsx': OPS_XLSX,
    'strat_db': STRAT_DB,
}


def build_context(base_path=BASE_PATH, strat_db_creds=None, dfs_creds=None):
    strat_db_creds = DEFAULT_PGCREDS if strat_db_creds is None else strat_db_creds
    source_access = build_source_access(dfs_creds=dfs_creds)
    env = ResourceEnvironment(
        base_path=base_path,
        file_access=source_access,
        credentials={'strat_db': strat_db_creds},
    )
    return build_resource_context(TASK_NAME, RESOURCES, PIPELINES, RUN_SEQUENCE, env)


class nsi_911:
    spec = PipelineSpec(
        excel_name='nsi_911.xlsx',
        db_table='ops_nsi_911',
        db_table_id_pix=280,
        db_output=[
            'id', 'name', 'created', 'closed', 'state', 'client_login', 'category', 'agent', 'responsible',
            'unit', 'date', 'days_diff', 'time_elapsed', 'sla3', 'sla6', 'status', 'metric', 'block', 'person',
        ],
    )

    @classmethod
    def run(cls, ctx, *, source):
        map_metrics = source.get_map('map_metrics')
        map_login_block = source.get_map('map_login_block')
        map_unit_block = source.get_map('map_unit_block')
        map_fio = source.get_map('map_fio')
        map_fio_block = source.get_map('map_fio_block')
        prev = source.get_table('nsi_911')
        map_state = {
            'closed': 'Закрыта успешно', #'Закрыта',
            'closed unsuccessful': 'Закрыта успешно', #'Закрыта',
            'closed with workaround': 'Закрыта успешно', # 'Закрыта',
            'closed successful': 'Закрыта успешно',
        }

        def rows_911(row):
            dts = (x if isinstance(x, datetime) else parse_dt(x[:19])) if (x := row.created) else None
            dte = (x if isinstance(x, datetime) else parse_dt(x[:19])) if (x := row.closed) else None
            days_diff = (dte - dts).total_seconds() / 86400 if dts and dte else None

            return list(row) + [
                dte.date() if dte else None, #dte.date().replace(day=1) if dte else None,
                days_diff,
                None if days_diff is None else int(days_diff <= 3.0),
                None if days_diff is None else int(days_diff <= 6.0),
                map_state.get(row.state, 'Открыта'),
                map_metrics.get(row.category, row.category),
                map_login_block.get(row.client_login, map_fio_block.get(row.client_login, map_unit_block.get(row.unit, '-- не определен --'))),
                map_fio.get(row.agent, 'прочие'),
            ]
        
        out = (
            prev
            .select(lambda r: r.id)
            .convert('time_elapsed', to_minutes)
            .rowmap(
                rows_911,
                header=list(prev.header()) + ['date', 'days_diff', 'sla3', 'sla6', 'status', 'metric', 'block', 'person'],
                failonerror=True,
            )
            .cut(*cls.spec.db_output)
        )

        push_date_range(ctx, 'nsi_911', out, date_col='date')
        return out


class mdm:
    spec = PipelineSpec(
        excel_name='mdm.xlsx',
        db_table='ops_mdm',
        db_table_id_pix=281,
        db_output=['type', 'id', 'created_by', 'be', 'status', 'date_start', 'date_end', 'date', 'total_days', 'dur_amend', 'proc_days', 'sla', 'metric', 'block'],
    )

    @classmethod
    def run(cls, ctx, *, source):
        map_fio_block = source.get_map('map_fio_block')
        prev = source.get_table('mdm')

        def rows_mdm(row):
            dts = row.date_start if row.date_start else None    #x.date() if (x := row.date_start) else None
            dte = row.date_end if row.date_end else None    #x.date() if (x := row.date_end) else None
            days_diff = (dte - dts).days if dts and dte else None
            total_days = row.dur_days if row.dur_days else days_diff
            
            dur_amend = row.dur_amend if row.dur_amend else 0
            proc_days = row.dur_proc if row.dur_proc else days_diff - dur_amend if days_diff is not None else None

            return list(row) + [
                dte, #dte.replace(day=1) if dte else None,
                total_days,
                proc_days,
                None if proc_days is None else int(proc_days <= 3),
                'Обработка MDM',
                map_fio_block.get(row.created_by, '-- не определен --'),
            ]
        
        out = (
            prev
            .select(lambda r: r.id and r.status in ('Выполнена', 'Отозвана'))
            .convert(('date_start', 'date_end'), lambda x: to_date(x) if x else None)
            .rowmap(
                rows_mdm,
                header=list(prev.header()) + ['date', 'total_days', 'proc_days', 'sla', 'metric', 'block'],
                failonerror=True,
            )
            .cut(*cls.spec.db_output)
        )

        push_date_range(ctx, 'mdm', out, date_col='date')
        return out


class tickets_1c:
    spec = PipelineSpec(
        excel_name='tickets_1c.xlsx',
        db_table='ops_tickets_1c',
        db_table_id_pix=282,
        db_output=[
            'op_type', 'date_start', 'date_end', 'proc_mins', 'unit', 'serviced_by', 'is_error', 'error_kind', 'input_attempts',
            'date', 'diff_days', 'sla', 'total_mins', 'total_days', 'metric', 'block', 'person',
        ],
    )

    @classmethod
    def run(cls, ctx, *, source):
        map_metrics = source.get_map('map_metrics')
        map_unit_block = source.get_map('map_unit_block')
        map_fio = source.get_map('map_fio')
        prev = source.get_table('tickets_1c')

        def rows_1c(row):
            dts = row.date_start if row.date_start else None    #x.date() if (x := row.date_start) else None
            dte = row.date_end if row.date_end else None    #x.date() if (x := row.date_end) else None
            days_diff = (dte - dts).days if dts and dte else None
            total_mins = None if (row.op_type is None or days_diff is None) else days_diff * 8 * 60 + (row.proc_mins or 0)

            return list(row) + [
                dte, #dte.replace(day=1) if dte else None,
                days_diff,
                None if days_diff is None else int(days_diff <= 1),
                total_mins,
                None if total_mins is None else total_mins / (24 * 60),
                map_metrics.get(row.op_type, row.op_type),
                map_unit_block.get(row.unit, '-- не определен --'),
                map_fio.get(row.serviced_by, 'прочие'),
            ]

        out = (
            prev
            .select(lambda r: r.date_start)
            .convert(('date_start', 'date_end'), lambda x: to_date(x) if x else None)
            .rowmap(
                rows_1c,
                header=list(prev.header()) + ['date', 'diff_days', 'sla', 'total_mins', 'total_days', 'metric', 'block', 'person', ],
                failonerror=True,
            )
            .cut(*cls.spec.db_output)
        )

        push_date_range(ctx, 'tickets_1c', out, date_col='date')
        return out


class ca:
    spec = PipelineSpec(
        excel_name='ca.xlsx',
        db_table='ops_ca',
        db_table_id_pix=283,
    )

    @classmethod
    def run(cls, ctx, *, source):
        map_unit_block = source.get_map('map_unit_block')
        prev = source.get_table('ca')

        def rows_ca(row):
            dt = to_date(x) if (x := row.date_op) else None
            return [dt] + list(row)[1:] + [
                dt, #dt.replace(day=1) if dt else None,
                map_unit_block.get(row.owner_unit, '-- не определен --'),
            ]

        out = (
            prev
            .select(lambda r: r.date_op)
            .rowmap(
                rows_ca,
                header=list(prev.header()) + ['date', 'block'],
                failonerror=True,
            )
        )

        push_date_range(ctx, 'ca', out, date_col='date')
        return out


class db_strat:
    spec = PipelineSpec(
        excel_name='db_strat.xlsx',
        db_table='ops_db_strat',
    )

    @classmethod
    def run(cls, ctx, *, source):
        return source.get_table(
            table='strat35',
            postprocess=lambda tbl: tbl.select(lambda r: r.year == 2025),
        )


class cal:
    spec = PipelineSpec(
        excel_name='cal.xlsx',
        db_table='ops_cal',
        db_table_id_pix=284,
    )

    @classmethod
    def run(cls, ctx):
        return build_calendar_from_ranges(ctx, shared_name='date_ranges')


PIPELINES = {
    'nsi_911': bind(nsi_911, source=OPS_XLSX),
    'mdm': bind(mdm, source=OPS_XLSX),
    'tickets_1c': bind(tickets_1c, source=OPS_XLSX),
    'ca': bind(ca, source=OPS_XLSX),
    'db_strat': bind(db_strat, source=STRAT_DB),
    'cal': cal,
}

RUN_SEQUENCE = [
    'nsi_911',
    'mdm',
    'tickets_1c',
    'ca',
    # 'db_strat',
    'cal',
]


def main(base_path=BASE_PATH, output_creds=None, strat_db_creds=None, pg_schema=PG_SCHEMA, dfs_creds=None, force_run=False, smb_level=logging.WARNING):
    output_creds = DEFAULT_PGCREDS if output_creds is None else output_creds
    strat_db_creds = DEFAULT_PGCREDS if strat_db_creds is None else strat_db_creds
    setup_logging(TASK_NAME, smb_level=smb_level)
    return run_pipelines(
        task_name=TASK_NAME,
        build_context=lambda: build_context(base_path, strat_db_creds=strat_db_creds, dfs_creds=dfs_creds),
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
    result = main()
    if result.skipped:
        # The outer DAG decides what result.skipped should mean (Airflow
        # skipped, success, etc.) -- this task file only reports it.
        logging.getLogger(TASK_NAME).info('Scaffold skipped task: %s', result.skip_reason)
    pix_ids = result.db_committed_table_ids_pix
