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
| `db_type_overrides` | `None` | Inferred mode only: `{column: type}` pinning selected SQL types. |
| `db_not_null_columns` | `None` | Inferred mode only: columns that must be `NOT NULL`. |
| `output_schema` | `None` | Complete ordered schema of user-owned columns. Supplying it disables inference. |
| `db_table_id_pix` | `None` | Opaque identifier carried into `RunResult`. |
| `db_updated_at` | `False` | Framework-owned timestamp column in either schema mode: `True` uses `etl_updated_at`; a string supplies a custom portable lower-case name. The column is `TIMESTAMPTZ NOT NULL`. |
| `publish_result` | `False` | Make the result available to later pipelines via `ctx.get_result()`. |
| `debug_display` | `False` | Print the table during the run. |
| `table_adapter` | `None` | `'petl'`, `'pandas'`, or `None` to infer. |
| `db_publication_strategy` | `None` | `'replace'` (default) or `'refill'`. See below. |

### `db_output` is declarative

The scaffold validates `db_output` and uses it during preflight to check
declared column names, but **does not apply it**. Projection is the
pipeline's own job:

```python
class mdm:
    spec = PipelineSpec(db_table='customer_master', db_output=['id', 'name', 'status'])

    @classmethod
    def run(cls, ctx, *, source):
        return build_table(source).cut(*cls.spec.db_output)
```

Declaring `db_output` without cutting is not an error — you get the
pipeline's own columns. `db_output` is available only in inferred mode and
cannot be combined with `output_schema`.

### Inferred and declared database schemas

The examples below use:

```python
import sqlalchemy as sa

from task_core import OutputColumn, PipelineSpec
```

Without `output_schema`, the scaffold infers the complete column set and
PostgreSQL types. `db_type_overrides` may pin selected types and
`db_not_null_columns` may mark selected inferred columns `NOT NULL`:

```python
spec = PipelineSpec(
    db_table='customers',
    db_type_overrides={'revenue': sa.Numeric(18, 2)},
    db_not_null_columns=('customer_id',),
)
```

For a complete declared user-column contract, declare every user column:

```python
spec = PipelineSpec(
    db_table='customer_summary',
    output_schema=(
        OutputColumn('customer_id', sa.BigInteger(), nullable=False),
        OutputColumn('revenue', sa.Numeric(18, 2)),
        OutputColumn(
            'created_at',
            sa.DateTime(timezone=True),
            nullable=False,
        ),
    ),
)
```

Columns are nullable by default. Declared output must contain exactly the same
column set; source order may differ and is reordered into declaration order.
Missing or unexpected columns, normalized missing values in non-nullable
columns and incompatible value families fail during staging preparation before
publication.

Declared validation performs no implicit parsing or lossy conversion. In
particular, `float` is not converted to `NUMERIC`, `datetime` is not converted
to `DATE`, and strings are not parsed into typed values. Python `Decimal` is
supported for `NUMERIC` when precision and scale fit without rounding.
Fixed-length `CHAR`, enums and other PostgreSQL-specific type families are not
part of the initial declared-schema contract. Naive
datetimes are required for `timestamp without time zone`; timezone-aware
datetimes are required for `timestamp with time zone`.

`output_schema` cannot be combined with `db_output`, `db_type_overrides`,
`db_not_null_columns`, or `get_dynamic_db_contract()`. The dynamic hook conflict
is rejected during structural pipeline validation, before resources are built.
A static `db_contract` may still rename/project source columns before declared
validation.

`output_schema` contains user-owned columns only. When `db_updated_at=True`,
the framework appends the default `etl_updated_at` column. When
`db_updated_at` is a string, that value is the custom column name instead:

```python
spec = PipelineSpec(
    db_table='customer_summary',
    db_updated_at='loaded_at',
    output_schema=(
        OutputColumn('customer_id', sa.BigInteger(), nullable=False),
        OutputColumn('revenue', sa.Numeric(18, 2)),
    ),
)
```

The framework-owned timestamp column is appended after the declared user
columns as `TIMESTAMPTZ NOT NULL`. Its name must be a portable lower-case
identifier and must not appear in `output_schema`, `db_type_overrides`,
`db_not_null_columns`, or the produced user columns.

Declaration does not select publication mechanics. By default, the prepared
declared staging table replaces the live target with the same staged
`DROP`/`RENAME` path used by inferred outputs. When explicit `refill` is
selected, first publication creates and fills a permanent ordinary target;
later publications require an exact physical match and use transactional
`TRUNCATE` plus `INSERT FROM` staging. That explicit path preserves the target
OID and attached objects, rejects unsupported relation kinds, incompatible
schemas and external incoming foreign keys, and keeps the table comment under
framework ownership. See **Publication strategy** below.

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
| `publisher_config` | `None` | `PublisherConfig()` — everything about how publication behaves. See below. |

Returns a `RunResult`: `task_name`, `pipeline_rows`, `excel_outputs`,
`db`, `skipped`, `skip_reason`, `source_check_enabled`, `source_changed`,
`source_fingerprints`. The `db` field is a `DbRunResult` with `requested`,
`had_outputs`, `committed`, `committed_tables`, `published_tables`,
`row_counts`.


## PublisherConfig

Every publication-behaviour setting lives in one frozen object, so a task
that needs to change one does not have to restate the rest.

```python
from task_core import PublisherConfig, PublicationLockPolicy

run_pipelines(
    ...,
    publisher_config=PublisherConfig(
        publication_lock_policy=PublicationLockPolicy(
            retry_horizon_seconds=300,
            retry_delay_max_seconds=15,
        ),
    ),
)
```

| field | default | meaning |
| --- | --- | --- |
| `publisher_factory` | `DbPublisher` | Extension seam; see [architecture.md](architecture.md#extension-points). |
| `identifier_policy` | `IdentifierPolicy()` | Identifier rules, currently the byte limit. |
| `publication_lock_policy` | `PublicationLockPolicy()` | How long publication may wait for its target locks. |

`PublicationLockPolicy` bounds the `ACCESS EXCLUSIVE` wait during
publication, so a long-running reader cannot turn one publication into a
read outage. Defaults: `lock_timeout_ms=500`,
`acquisition_timeout_ms=5000`, `retry_horizon_seconds=60`,
`retry_delay_min_seconds=1`, `retry_delay_max_seconds=5`,
`max_attempts=100`.

The horizon is the real bound: it gates *completion* of lock acquisition,
so the per-attempt timeouts are ceilings and a final attempt may run with
less. `max_attempts` is a defensive ceiling only — unreachable under the
defaults, and there to stop a runaway if someone configures a sub-second
delay.

Widen the horizon for tables under constant BI load; leave it alone
otherwise. Nothing about a normal task changes.


## Publication strategy

Two independent choices. `output_schema` decides where the table's shape
comes from; `db_publication_strategy` decides how new data replaces old.

| schema | `replace` (default) | `refill` |
| --- | --- | --- |
| inferred | yes | rejected |
| declared | yes | optional |

**`replace`** drops the live table and renames staging into its place. One
database write, and the lock is held only for catalog operations. The
target is a new relation each run, so views, grants, indexes, ownership
and triggers **do not survive** — and `DROP` fails outright if a view
depends on the table.

**`refill`** truncates the live table and inserts from staging. The OID is
stable, so everything attached to the table survives. It costs a second
database write and holds the lock for a window **proportional to row
count**, which blocks readers for that whole window.

Choose `refill` when something is attached to the table that must survive
— a view, a grant, an index, a trigger. Otherwise leave it alone: refill's
cost is a pure loss when nothing depends on the target.

```python
spec = PipelineSpec(
    db_table='customer_summary',
    output_schema=(OutputColumn('id', sa.BigInteger(), nullable=False),),
    db_publication_strategy='refill',   # a BI view depends on this table
)
```

`refill` requires `output_schema`: it truncates and inserts into the
existing table, so the target's physical schema must be stable across
runs, and only a declaration can promise that. An inferred schema changes
whenever the data does, so the combination is rejected at spec
construction rather than failing on the first drift.

Changing a declared schema is only possible under `replace`. Under
`refill` the compatibility check refuses a target whose physical shape no
longer matches, and asks you to migrate or recreate it explicitly.

### Migrating existing 0.5.0 scripts

Search every running script for `output_schema=`. In 0.5.0 that implied stable
refill; in 0.5.1 it defaults to replacement. Add
`db_publication_strategy='refill'` only where the target must preserve its OID,
views, grants, indexes, ownership, triggers or RLS. Otherwise no source change
is required. Direct callers of `DbPayload`, `from_petl()` or `from_pandas()`
must also use only `replace`, or `refill` together with `output_schema`. See
[migrating-to-0.5.1.md](migrating-to-0.5.1.md).


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

### Replacement publication recreates the table

`replace` is the default for both inferred and declared schemas. Consequences:

- **Schema can change between runs in inferred mode.** Pin types with
  `db_type_overrides` when a consumer requires stability, or declare the full
  schema with `output_schema`.
- **Grants, ownership, indexes and triggers are not preserved.** Use schema
  default privileges and recreate target-owned objects where replacement is
  appropriate.
- **A dependent view blocks publication.** `DROP TABLE` fails when a view
  depends on the target. Use explicit declared `refill` only when preserving
  the ordinary table object is worth the extra write and row-dependent lock.

### Publication is staged, not run-long

Each target is prepared in its own committed transaction. One final
transaction publishes all targets and source state atomically. Replacement
keeps that transaction short; explicit refill holds the target lock through
`TRUNCATE`, the second full-row write, index/constraint maintenance and commit.
A session advisory lock prevents two runs of the same task from overlapping.

### Identifier rules

Table, schema and column names published to PostgreSQL must match
`^[a-z_][a-z0-9_]*$`. Cyrillic headers must be renamed via `db_contract`.
There is no quoted-identifier escape hatch: invalid names are rejected,
not normalized. Generated SQL still quotes identifiers defensively.

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
