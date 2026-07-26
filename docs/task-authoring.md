# Writing a task

Everything needed to build a task on `task_core`. For how the scaffold
works internally, see [architecture.md](architecture.md).


## Start from something that runs

[`examples/local_task.py`](../examples/local_task.py) is a complete task
that needs no share, no database and no in-house helper modules:

```
python -m examples.local_task
```

Change one thing at a time in it. When you need real inputs,
`tasks/hr_petl_task.py` is the smallest realistic task.


## The shape of a task file

Every task file has the same skeleton. This one is trimmed from
`tasks/hr_petl_task.py`:

```python
from task_core import (
    PipelineSpec, ResourceEnvironment, SourceChangeCheckConfig,
    bind, build_resource_context, build_source_access,
    run_pipelines, setup_logging, xlsx_file_set,
)

TASK_NAME = 'hr_petl_task'
BASE_PATH = r'\\server\share\folder'
PG_SCHEMA = 'bsr'

SOURCE_CHANGE_CHECK = SourceChangeCheckConfig(
    enabled=True, schema='bsr', table='task_scaffold_meta',
)


class ssch2:
    spec = PipelineSpec(excel_name='ssch2.xlsx', db_table='hr_ssch2')

    @classmethod
    def run(cls, ctx, *, source):
        selected = source.selected_files[0]
        rows = source.get_sheet_rows(0, selected_file=selected)
        return etl.cut(rows, 'a', 'b', 'c')


SSCH_FILES = xlsx_file_set('ssch', pattern='*.xlsx', tracker=True)

RESOURCES = {'ssch_files': SSCH_FILES}
PIPELINES = {'ssch2': bind(ssch2, source=SSCH_FILES)}
RUN_SEQUENCE = ['ssch2']


def build_context(base_path=BASE_PATH, dfs_creds=None):
    source_access = build_source_access(dfs_creds=dfs_creds)
    env = ResourceEnvironment(base_path=base_path, file_access=source_access)
    return build_resource_context(TASK_NAME, RESOURCES, PIPELINES, RUN_SEQUENCE, env)


def main(output_creds=None, force_run=False):
    setup_logging(TASK_NAME)
    return run_pipelines(
        task_name=TASK_NAME,
        build_context=build_context,
        pipelines=PIPELINES,
        run_sequence=RUN_SEQUENCE,
        output_excel=True,
        output_db=True,
        creds=output_creds,
        pg_schema=PG_SCHEMA,
        source_change_check=SOURCE_CHANGE_CHECK,
        force_run=force_run,
    )


if __name__ == '__main__':
    main()
```

Four module-level declarations do the wiring: `RESOURCES` names every
resource, `PIPELINES` binds resources to pipeline parameters,
`RUN_SEQUENCE` decides what actually runs and in what order, and
`SOURCE_CHANGE_CHECK` configures skipping.

A pipeline in `PIPELINES` but not in `RUN_SEQUENCE` is still validated and
never executed. A resource only reachable from such a pipeline is never
constructed — no file opened, no connection made.


## Pipelines

A pipeline is a class with a `spec` and a `run()` classmethod returning a
table.

```python
class funnel:
    spec = PipelineSpec(excel_name='funnel.xlsx', db_table='hr_funnel')

    @classmethod
    def run(cls, ctx, *, source):
        ...
```

`run()` takes exactly one positional parameter, which must be named
`ctx`. Resources supplied by `bind()` are **keyword-only** — the `*` is
required, and the keyword names must match the `bind()` call exactly. A
pipeline with no bound resources is just `run(cls, ctx)`.

`run()` must return a petl table or a pandas DataFrame. It must not close
resources, commit anything, or write to the database — the scaffold owns
all of that.

### Reading another pipeline's result

Set `publish_result=True` on the producer and read it from the context in
a later pipeline. Ordering comes from `RUN_SEQUENCE`.

```python
class base:
    spec = PipelineSpec(publish_result=True, excel_name='base.xlsx')

class derived:
    spec = PipelineSpec(db_table='hr_derived')

    @classmethod
    def run(cls, ctx, *, source):
        base_table = ctx.get_result('base')
        ...
```


## PipelineSpec

| field | default | meaning |
| --- | --- | --- |
| `excel_name` | `None` | Workbook filename to write. Omit for no Excel output. |
| `db_table` | `None` | Target table name. Omit for no DB output. |
| `db_output` | `None` | Declared column projection. **Declarative only** — see below. |
| `db_contract` | `None` | `{source_column: target_column}`. Applied by the scaffold: renames and restricts. |
| `db_type_overrides` | `None` | `{column: type}` pinning SQL types instead of inferring. |
| `db_table_id_pix` | `None` | Opaque identifier carried into `RunResult`. |
| `db_updated_at` | `False` | `True` for a `db_updated_at` timestamp column, or a string for a custom name. |
| `publish_result` | `False` | Make the result available to later pipelines via `ctx.get_result()`. |
| `debug_display` | `False` | Print the table during the run. |
| `table_adapter` | `None` | `'petl'`, `'pandas'`, or `None` to infer. |
| `db_identifier_mode` | `'portable'` | `'quoted'` to allow non-portable identifiers. See [limitations](#limitations). |

### `db_output` is declarative

The scaffold validates `db_output` and uses it during preflight to check
declared column names, but **does not apply it**. Projection is the
pipeline's own job:

```python
class mdm:
    spec = PipelineSpec(db_table='ops_mdm', db_output=['id', 'name', 'status'])

    @classmethod
    def run(cls, ctx, *, source):
        return build_table(source).cut(*cls.spec.db_output)
```

Declaring `db_output` without cutting is not an error — you get the
pipeline's own columns.

### `db_contract` is applied

```python
spec = PipelineSpec(
    db_table='hr_funnel',
    db_contract={'Блок': 'block', 'Подразделение': 'unit'},
)
```

Keys are the source column names as they exist in the table the pipeline
returns; values are the target names in PostgreSQL. Columns not mentioned
are dropped. This is where Cyrillic spreadsheet headers get renamed away —
required, because published column names must be portable identifiers.


## Resources

### Declarative forms

```python
latest_xlsx('hr/staff', pattern='*.xlsx', tracker=True)
xlsx_file_set('hr/ssch', pattern='*.xlsx', tracker=True)
resource(loader, tracker=False)
```

Paths are relative to `ResourceEnvironment.base_path`. `tracker=True`
includes the resource in source-change fingerprinting.

Resources return **petl tables**, whatever adapter the pipeline uses. A
pandas pipeline converts them itself.

### What a pipeline gets

**Excel resource** — `sheets`, `get_sheet_rows(sheet)`,
`get_sheet_raw_rows(sheet)`, `get_table(name)`, `get_map(name)`,
`get_range(sheet, ref)`, `read_row_metadata(...)`.

**File-set resource** — `selected_files`, plus the same sheet and table
accessors applied to a selected file.

**DB resource** — `get_table(table=...)` or `get_table(query=...)`
returning petl tables, with optional server-side cursors for large
results.

### Selection

A file set selects with `select_latest_file`, `select_fixed_file`, or all
matching files. Hidden, system and Excel temporary files (`~$…`) are
excluded by default.


## run_pipelines()

| parameter | default | meaning |
| --- | --- | --- |
| `task_name` | required | Used for logging and as the source-state key. |
| `build_context` | required | Callable returning a `task_context`. |
| `pipelines` | required | `{name: pipeline_class_or_binding}`. |
| `run_sequence` | required | Ordered list of names to execute. |
| `output_excel` | `False` | Whether `excel_name` outputs are written. Off by default — Excel is a debugging aid you switch on. |
| `output_db` | `False` | Whether `db_table` outputs are published. |
| `creds` | `None` | PostgreSQL credentials. Required only when the run actually uses PostgreSQL — a declared `db_table` or enabled source tracking. |
| `pg_schema` | `'bsr'` | Target schema. |
| `source_change_check` | `None` | `SourceChangeCheckConfig`, or `None` to always run. |
| `force_run` | `False` | Run even if sources are unchanged. |
| `publisher_factory` | `DbPublisher` | Extension seam; see [architecture.md](architecture.md#extension-points). |
| `db_max_identifier_bytes` | `63` | Identifier byte limit used by preflight. |

Returns a `RunResult`: `task_name`, `pipeline_rows`, `excel_outputs`,
`db`, `skipped`, `skip_reason`, `source_check_enabled`, `source_changed`,
`source_fingerprints`. The `db` field is a `DbRunResult` with `requested`,
`had_outputs`, `committed`, `committed_tables`, `published_tables`,
`row_counts`.


## Source-change checking

```python
SOURCE_CHANGE_CHECK = SourceChangeCheckConfig(
    enabled=True,
    schema='bsr',
    table='task_scaffold_meta',
    create_if_missing=True,
)
```

The stored state lives in PostgreSQL, so this needs `output_db=True`. If
it is enabled with `output_db=False` it is **ignored**, with an
informational log line — not an error. Only resources declared with
`tracker=True` are fingerprinted. If none are, an
enabled check fails with a clear error rather than silently never
skipping.

The state advances only when the run commits, so a failed run is retried
from the same starting point.

Use `force_run=True` to run regardless.


## Limitations

### Excel output is for debugging

Workbooks are written immediately, inside the pipeline loop. There is no
staging, no temporary file and no rename, and there never will be — see
[decisions/0007](decisions/0007-excel-output-is-a-debugging-aid.md).

What follows for you as a task author:

- A run that fails partway leaves workbooks from the pipelines that
  already succeeded. That is expected; delete them or re-run.
- Those files can disagree with the database, because a run can write
  workbooks and then fail before publishing.
- **Do not build anything that reads these files programmatically.** If a
  downstream process needs the data, publish a table and let it read that.
  A job consuming a workbook this scaffold wrote has no delivery
  guarantees at all.

Two pipelines still may not declare the same `excel_name` — that check is
about a task declaring something incoherent, not about publication
guarantees.

### Published tables are dropped and recreated

Every run drops the live table and replaces it. Consequences:

- **Schema can change between runs.** Types are inferred from the data, so
  a column that gains its first decimal changes from `bigint` to
  `numeric`. Pin types with `db_type_overrides` for any table with
  downstream consumers.
- **Grants are not preserved.** Use `ALTER DEFAULT PRIVILEGES` on the
  schema.
- **A dependent view breaks the publish.** `DROP TABLE` fails when a view
  depends on the table. Views and inferred schemas are mutually
  exclusive — see
  [decisions/0001](decisions/0001-replace-tables-instead-of-truncating.md).

### One transaction spans the run

Atomic, but long: it holds catalog locks, delays vacuum and accumulates
WAL for the duration. The staging swap keeps live tables readable until
the end, but the transaction itself lasts as long as the run.

Nothing prevents two runs of the same task from overlapping.

### Identifier rules

Table, schema and column names published to PostgreSQL must match
`^[a-z_][a-z0-9_]*$`. Cyrillic headers must be renamed via `db_contract`.
`db_identifier_mode='quoted'` relaxes the pattern only — the byte limit,
duplicate detection and column uniqueness still apply. The schema is
always validated as portable regardless of any pipeline's mode.

Identifiers are limited to 63 bytes, counted as UTF-8 bytes rather than
characters.

### pandas missing values

pandas 3 defaults object columns to `StringDtype`, which converts `None`
to `nan`. When building a DataFrame that must preserve `None`, be explicit
about dtype, and prefer `fromdataframe_preserve_none()` over
`etl.fromdataframe()` when converting back.

`normalize_for_excel()` and the DB payload path both convert every flavour
of missing value to `None`, but one-element containers are values, not
scalars, and are left alone.

### `types` shadowing

`task_core/types.py` shadows the standard library `types` module inside
this package. Use absolute imports (`import types` at top level resolves
to the stdlib; a relative import inside `task_core` would not).

(`task_core.file_access` had the same problem until 0.3.1, when the class
was renamed to `source_access`. The attribute now resolves to the module,
as it should.)

### Remote workbook handles

Reading a workbook over SMB/DFS requires `gc.collect()` after closing to
release the handle — openpyxl reference cycles keep the underlying ZIP
open otherwise. This is handled inside the scaffold, but if you open
workbooks yourself, see
[decisions/0003](decisions/0003-gc-collect-for-remote-workbook-handles.md).

### Environment

Python 3.11 or newer, enforced at import. Core tests verified on 3.12.3
and 3.13.5. Your production environment may be narrower, and any in-house
helper modules your tasks import are outside this project's control.

Dependencies are unpinned, and this codebase is sensitive to pandas
missing-value semantics, so an unpinned `pandas` is a live upgrade
hazard.
