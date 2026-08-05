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
RESOURCES = {'source_files': xlsx_file_set('source', pattern='*.xlsx', tracker=True)}
PIPELINES = {'summary': bind(summary, source=RESOURCES['source_files'])}
RUN_SEQUENCE = ['summary']

run_pipelines(
    task_name='reporting_task',
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
`psycopg2`, `petl`, `lxml` and `smbclient`. See `requirements.txt`.
Project-specific task modules may require additional dependencies, but they
are outside the scaffold's runtime contract.


## Running

A task is a normal Python module with a `main()`:

```
python -m examples.local_task
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
you can change one thing at a time and see the effect. Then read
[task-authoring.md](docs/task-authoring.md) for database publication,
source-change checking, remote resources and the full API reference.


## Inferred and declared output schemas

Database schemas are inferred by default:

```python
import sqlalchemy as sa

from task_core import OutputColumn, PipelineSpec

PipelineSpec(db_table='customer_summary')
```

For a stable, strictly validated contract, provide the complete user-owned
`output_schema`:

```python
PipelineSpec(
    db_table='customer_summary',
    output_schema=(
        OutputColumn('customer_id', sa.BigInteger(), nullable=False),
        OutputColumn('revenue', sa.Numeric(18, 2)),
        OutputColumn('created_at', sa.DateTime(timezone=True), nullable=False),
    ),
)
```

When the destination table already exists, generate the declaration instead
of typing it manually:

```bash
python tools/generate_output_schema.py --table customer_summary
```

When `--schema` is omitted, PostgreSQL resolves the table through the active
`search_path`, including a value supplied by `pgcreds` such as
`options='-c search_path=bsr,public'`. Pass `--schema bsr` to force one explicit
schema. The tool accepts command-line connection overrides and also supports
no-argument notebook/editor execution through `TABLE_NAME` and optional
`SCHEMA_NAME` constants at the top of the script. Leave `SCHEMA_NAME = None`
to use `search_path`. It performs read-only introspection and emits paste-ready
class indentation. See
[task-authoring.md](docs/task-authoring.md#generate-a-declaration-from-an-existing-table).

Supplying `output_schema` disables inference. The declaration defines the
complete user-owned column set, order, types and nullability. Columns are
nullable by default. Enabled framework-owned columns are appended afterward:
`db_updated_at=True` adds `etl_updated_at`, while a string such as
`db_updated_at='loaded_at'` supplies a custom portable lower-case name. The
timestamp column is always `TIMESTAMPTZ NOT NULL` and is not repeated in
`output_schema`. Missing or unexpected user columns, incompatible values and
`NULL` in a column declared with `nullable=False` fail during staging
preparation before the live target is changed. Schema source does not select
publication strategy: both inferred and declared outputs use replacement by
default. A declared pipeline may explicitly request stable refill with
`db_publication_strategy='refill'` when preserving the ordinary table object
and its attached database objects is worth the extra write and longer lock.


The current declared-schema and publication contracts are documented in
[task-authoring.md](docs/task-authoring.md). Historical release-specific
changes remain in [CHANGELOG.md](CHANGELOG.md); standalone migration guides
are reserved for migrations that cannot be explained concisely in those two
places.


## Limitations

Known constraints that will affect you, in rough order of how likely they
are to matter. Each is expanded in
[task-authoring.md](docs/task-authoring.md#limitations).

- **Excel output is for debugging, not production.** A failed run may
  leave workbooks from pipelines that succeeded before it failed, and those
  files may disagree with the database. Nothing downstream may read them
  programmatically; if something needs the data, it reads the published
  table.
- **Replacement is the default for both schema sources.** It performs one
  database write and a short `DROP`/`RENAME` publication, but grants, indexes,
  ownership and triggers do not survive; dependent views make `DROP` fail.
  Explicit declared `refill` preserves the ordinary table object and attached
  objects, but writes every row twice and blocks readers through `TRUNCATE`,
  refill, index/constraint maintenance and commit.
- **Publication is atomic; preparation is not.** Each DB target is prepared
  in its own committed transaction, followed by one atomic publication
  transaction. Replacement publication is normally short; explicit stable
  refill may be materially longer and row-dependent. No transaction spans the
  run, but a failed run can leave committed staging tables for positively
  scoped cleanup.
- **Schema, table and column names published to PostgreSQL must be
  lower-case portable identifiers** matching `^[a-z_][a-z0-9_]*$`.
- **`task_core.types` shadows the standard library `types` module** inside
  this package. Absolute imports only.
- **`requirements.txt` is unpinned**, and this codebase is sensitive to
  pandas missing-value semantics.
- **`creds` is required only when the run actually uses PostgreSQL** —
  `output_db=True` with at least one executed pipeline declaring `db_table`,
  or enabled source tracking. `output_db=True` alone does not require it.
