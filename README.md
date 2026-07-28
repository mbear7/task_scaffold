# task_core

A scaffold for scheduled reporting tasks that read Excel workbooks (locally
or over SMB/DFS) and PostgreSQL, and transform them with petl or pandas.

Outputs are independent: a task can write Excel files, publish PostgreSQL
tables, do both, or neither. A task with no declared outputs still runs its
pipelines and returns their results, which is a reasonable way to use it
for computation that another process consumes.

**PostgreSQL tables are the production output. Excel output is a local
debugging aid** — no staging, no temporary files, no renames, and no
transactional relationship with database publication. See
[decisions/0007](docs/decisions/0007-excel-output-is-a-debugging-aid.md).

It exists to remove the parts every such task repeats: opening remote
workbooks and closing their handles reliably, deciding whether the sources
changed since last time, running a sequence of pipelines, exporting
results, publishing to the database atomically, and cleaning up afterwards
even when something fails halfway through.

A task file declares its resources and pipelines and calls one function.
`task_core` owns the lifecycle.

```python
RESOURCES = {'ssch_files': xlsx_file_set('ssch', pattern='*.xlsx', tracker=True)}
PIPELINES = {'ssch2': bind(ssch2, source=RESOURCES['ssch_files'])}
RUN_SEQUENCE = ['ssch2']

run_pipelines(
    task_name='hr_petl_task',
    build_context=build_context,
    pipelines=PIPELINES,
    run_sequence=RUN_SEQUENCE,
    output_excel=True,
    output_db=True,
    creds=PG_CREDS,
    source_change_check=SOURCE_CHANGE_CHECK,
)
```


## Quick start

No share, no database, no credentials:

```
python -m examples.local_task
```

It creates a sample workbook in a temporary directory, runs two pipelines
over it — one reading the workbook, one aggregating the first's result —
and prints the workbooks it wrote. Read
[examples/local_task.py](examples/local_task.py) alongside the output; it
is short, and every part of it is part of the shape a real task takes.

`tasks/hr_petl_task.py` is the realistic counterpart: SMB paths, database
output, source-change checking. It cannot run outside the production
environment.


## Documentation

| | |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | How the system works: run lifecycle, resources, transactions, failure and cleanup guarantees, extension points. |
| [docs/task-authoring.md](docs/task-authoring.md) | How to write a task: minimal example, full `PipelineSpec` and `run_pipelines()` reference, patterns, limitations. |
| [docs/decisions/](docs/decisions/) | Why certain non-obvious things are the way they are. Read before changing them. |
| [CHANGELOG.md](CHANGELOG.md) | What changed in each version. |


## Requirements

Python 3.11 or newer — enforced at import. The core test suite is
verified on 3.12.3 and 3.13.5. Your production task environment may be
narrower.

`task_core` itself needs `pandas`, `numpy`, `openpyxl`, `sqlalchemy`,
`psycopg2`, `petl`, `lxml` and `smbclient`. The reference tasks in
`tasks/` additionally need `babel`. See `requirements.txt`.

`task_core` depends on nothing beyond that list. The reference tasks in
`tasks/` import a shared in-house helper module that is not part of this
project and not shipped with it; `examples/` does not.


## Running

A task is a normal Python module with a `main()`:

```
python -m examples.local_task      # self-contained
python -m tasks.hr_task            # needs share access and DB credentials
```

`run_pipelines()` returns a `RunResult`, which is what a scheduler or
wrapper should inspect rather than relying on the exit code:

```python
result = main()

result.skipped            # True when execution was intentionally skipped
result.skip_reason        # 'sources_unchanged' | 'task_already_running'
result.pipeline_rows      # {pipeline_name: row_count}
result.excel_outputs      # workbooks written
result.db.committed       # whether the publication transaction committed
result.db.published_tables
result.db.row_counts
result.source_changed
```

A failed run raises rather than returning; a skipped run returns normally
with `skipped=True`. Full field list is in
[task-authoring.md](docs/task-authoring.md#run_pipelines).

The test suite uses `unittest` and needs no runner or plugins:

```
python -m unittest discover -s tests -t .
```

Tests cover `task_core` only. `tasks/` is deliberately not covered — see
[decisions/0002](docs/decisions/0002-keep-core-tests-independent-of-tasks.md).


## Adding a task

Start from [examples/local_task.py](examples/local_task.py) — it runs, so
you can change one thing at a time and see the effect. When you need real
inputs and database output, `tasks/hr_petl_task.py` is the smallest
realistic task: one resource, one pipeline, Excel and DB output,
source-change checking.

Then read [task-authoring.md](docs/task-authoring.md).


## Limitations

Known constraints that will affect you, in rough order of how likely they
are to matter. Each is expanded in
[task-authoring.md](docs/task-authoring.md#limitations).

- **Excel output is for debugging, not production.** A failed run may
  leave workbooks from pipelines that succeeded before it failed, and those
  files may disagree with the database. Nothing downstream may read them
  programmatically; if something needs the data, it reads the published
  table.
- **Published tables are dropped and recreated on every run.** Column
  types are inferred from the data, so a table's schema can change between
  runs. Grants are not preserved, and a dependent view makes the publish
  fail.
- **Publication is atomic; preparation is not.** Each DB target is
  prepared in its own committed transaction, and one short publication
  transaction swaps them all. No transaction spans the run — but a failed
  run leaves committed staging tables, which the next run of the same task
  removes.
- **Column names published to PostgreSQL must be lower-case ASCII
  identifiers** unless a pipeline opts into `db_identifier_mode='quoted'`.
- **`task_core.types` shadows the standard library `types` module** inside
  this package. Absolute imports only.
- **`requirements.txt` is unpinned**, and this codebase is sensitive to
  pandas missing-value semantics.
- **`creds` is required only when the run actually uses PostgreSQL** —
  a declared `db_table` or enabled source tracking. `output_db=True`
  alone does not require it.
