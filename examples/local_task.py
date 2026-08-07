"""
A complete, runnable task. No SMB share, no PostgreSQL, nothing outside
this project's declared requirements -- it creates its own input workbook
in a temporary directory, runs two pipelines over it, and writes Excel
output.

    python -m examples.local_task

This exists so that someone who has just cloned the repository can get a
working result before reading anything else. tasks/hr_petl_task.py is the
realistic reference: real remote paths, real database output, real
source-change checking.

Everything here is deliberately the smallest thing that is still honest
about the shape of a real task -- module-level RESOURCES / PIPELINES /
RUN_SEQUENCE, a bound resource, a PipelineSpec per pipeline, and one
run_pipelines() call.
"""

from __future__ import annotations

import logging
import os
import tempfile

import petl as etl
from openpyxl import Workbook

from task_core import (
    PipelineSpec,
    ResourceEnvironment,
    bind,
    build_resource_context,
    build_source_access,
    latest_xlsx,
    run_pipelines,
    setup_logging,
)

TASK_NAME = 'local_example'


def make_sample_workbook(folder):
    """Stands in for the remote workbook a real task would read."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = 'sales'
    sheet.append(['Регион', 'Менеджер', 'Сумма'])
    sheet.append(['Север', 'Иванов', 1200])
    sheet.append(['Юг', 'Петрова', 950])
    sheet.append(['Север', 'Сидоров', 1730])
    sheet.append(['Юг', 'Иванов', 400])

    path = os.path.join(folder, 'sales_2024.xlsx')
    workbook.save(path)
    return path


class deals:
    """Reads the workbook and renames its columns.

    db_contract maps the Cyrillic spreadsheet headers to portable
    identifiers. This task does not publish to a database, but the
    contract is declared anyway because that is where a real task would
    put it -- published column names must match ^[a-z_][a-z0-9_]*$.
    """

    spec = PipelineSpec(
        excel_name='deals.xlsx',
        publish_result=True,
        db_contract={'Регион': 'region', 'Менеджер': 'manager', 'Сумма': 'amount'},
    )

    @classmethod
    def run(cls, ctx, *, source):
        # Resources hand back petl tables, whatever adapter a pipeline
        # uses. source is the excel resource bound below.
        rows = source.get_sheet_rows('sales')
        return etl.rename(rows, {'Регион': 'region', 'Менеджер': 'manager', 'Сумма': 'amount'})


class by_region:
    """Aggregates the previous pipeline's result.

    Reads it through ctx.get_result(), which works because `deals`
    declares publish_result=True and runs earlier in RUN_SEQUENCE.
    """

    spec = PipelineSpec(excel_name='by_region.xlsx')

    @classmethod
    def run(cls, ctx):
        deals_table = ctx.get_result('deals')
        return etl.aggregate(deals_table, 'region', {'total': ('amount', sum)})


SALES_FILE = latest_xlsx('.', pattern='*.xlsx', tracker=False)

RESOURCES = {'sales_file': SALES_FILE}
PIPELINES = {
    'deals': bind(deals, source=SALES_FILE),
    'by_region': by_region,
}
RUN_SEQUENCE = ['deals', 'by_region']


def build_context(base_path):
    # build_source_access() with no SMB credentials gives local file
    # access. A real task passes dfs_creds and gets an SMB reader from the
    # same call.
    env = ResourceEnvironment(base_path=base_path, file_access=build_source_access())
    return build_resource_context(TASK_NAME, RESOURCES, PIPELINES, RUN_SEQUENCE, env)


def main():
    setup_logging(TASK_NAME, level=logging.INFO)

    with tempfile.TemporaryDirectory() as folder:
        make_sample_workbook(folder)
        previous_directory = os.getcwd()
        # Excel output is written relative to the working directory, so run
        # from the temporary folder to keep the example self-contained.
        os.chdir(folder)
        try:
            result = run_pipelines(
                task_name=TASK_NAME,
                build_context=lambda: build_context(folder),
                pipelines=PIPELINES,
                run_sequence=RUN_SEQUENCE,
                output_excel=True,
                output_db=False,
            )

            print(f'\nrows per pipeline : {result.pipeline_rows}')
            print(f'workbooks written : {result.excel_outputs}')
            for name in result.excel_outputs:
                print(f'\n{name}:')
                print(etl.look(etl.fromxlsx(name)))
        finally:
            os.chdir(previous_directory)

    return result


if __name__ == '__main__':
    main()
