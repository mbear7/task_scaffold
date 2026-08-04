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
| `db_loader` | `'insert'` | Staging loader: `'insert'` for materialized SQLAlchemy INSERT batches or `'copy'` for bounded local spooling followed by PostgreSQL `COPY FROM STDIN`. |
| `db_copy_spool_encryption` | `None` | COPY only. `None` inherits the secure `CopyLoadPolicy` default; `False` explicitly stores spool bodies as plaintext and emits a warning. |

### Choosing the database loader

`db_loader='insert'` preserves the established materialized mapping path.
`db_loader='copy'` consumes the adapter output once, prepares bounded local
spools, and streams the final COPY-text body through PostgreSQL `COPY FROM
STDIN` on the publisher's existing connection. Both loaders use the same
resolved schema and publication protocol.

COPY spools are encrypted by default. A task may explicitly opt out:

```python
spec = PipelineSpec(
    db_table='large_output',
    db_loader='copy',
    db_copy_spool_encryption=False,
)
```

The opt-out persists spool bodies as plaintext and emits a warning. Spools live
on the Python task host under the platform temporary directory unless
`PublisherConfig.copy_load_policy.spool_directory` supplies another local
path. Current-run spools are deleted on ordinary success and handled failure
paths. If a process crash prevents that cleanup, the next execution deletes
positively identified predecessor spools only after acquiring the same task
advisory lock. Unknown, malformed and foreign files are preserved. Failure to
remove a positively owned predecessor is fatal. This is artifact cleanup only;
no task output or interrupted publication is recovered. COPY does not support
`get_dynamic_db_contract()` because its final positional row shape must be
fixed before one-shot traversal begins.

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

In inferred mode, naive Python datetimes resolve to PostgreSQL `TIMESTAMP` and
timezone-aware datetimes resolve to `TIMESTAMPTZ`. Bare dates may widen with
naive datetimes to `TIMESTAMP` at midnight. A column that mixes aware
datetimes with naive datetimes or bare dates is rejected before database work;
normalize the values to one awareness policy or declare `output_schema`
explicitly.

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

### Generate a declaration from an existing table

When a table already exists, especially after an earlier inferred-schema run,
do not hand-write a 100-column declaration. Use the repository tool:

```bash
python tools/generate_output_schema.py --schema bsr --table customer_summary
```

The default output is indented for direct paste inside a task class's
`PipelineSpec(...)` call:

```python
class customer_summary:
    spec = PipelineSpec(
        db_table='customer_summary',
        # Generated from bsr.customer_summary.
        output_schema=(
            OutputColumn('customer_id', sa.BigInteger(), nullable=False),
            OutputColumn('revenue', sa.Numeric(18, 2), nullable=True),
        ),
    )
```

Use `--style class-constant` to emit a four-space `OUTPUT_SCHEMA = (...)`
class attribute instead. Use `--output schema_snippet.py` to write UTF-8 code
to a file. Run `python tools/generate_output_schema.py --help` for all options.

For notebook or editor execution without command-line arguments, open
`tools/generate_output_schema.py`, edit `TABLE_NAME` and `SCHEMA_NAME` in the
configuration block near the top, then run the file. The script prints the
generated code.

Connection settings are resolved in this order:

1. command-line `--host`, `--port`, `--dbname`, `--user`, `--password`;
2. inline `DB_*` values in the script;
3. an importable `pgcreds.pgcreds` mapping.

Command-line values override only the supplied keys, so other pgcreds options
such as `sslmode` remain active. The catalog connection is read-only and the
tool never changes the table.

The generated declaration contains user-owned columns only. Exclude a
framework-owned timestamp column explicitly:

```bash
python tools/generate_output_schema.py \
    --schema bsr \
    --table customer_summary \
    --exclude-column etl_updated_at
```

The tool does not invent conversions. It emits code only when the existing
column order, PostgreSQL types, type parameters and nullability fit the exact
task_core declared-schema subset. Unsupported types, domains, enums, identity
or generated columns, non-default collations and nonportable identifiers are
reported together; no partial code is emitted. Resolve those differences by
manually migrating the table, dropping and recreating it, excluding a genuinely
framework-owned column, or writing the declaration manually.

Column defaults are not part of `OutputColumn`. The tool therefore emits the
type and nullability but writes a warning to stderr. `refill` preserves a
default attached to the existing table; `replace` recreates the table without
it. Review those warnings before committing the generated declaration.

Columns are nullable by default. Declared output must contain exactly the same
column set; source order may differ and is reordered into declaration order.
Missing or unexpected columns, normalized missing values in non-nullable
columns and incompatible value families fail during staging preparation before
publication.

Declared validation performs no implicit parsing or lossy conversion. In
particular, `float` is not converted to `NUMERIC`, `datetime` is not converted
to `DATE`, and strings are not parsed into typed values. Python `Decimal` is
supported for `NUMERIC` when precision and scale fit without rounding.

Declared type parameters must survive PostgreSQL rendering exactly:

- `Float()` is unconstrained; `Float(p)` requires integer `1 <= p <= 53`;
- `String()` is unbounded; `String(n)` requires a positive integer length;
- `Numeric()` is unconstrained; constrained `Numeric(p[, s])` requires integer
  `1 <= p <= 1000` and the supported subset `0 <= s <= p`;
- `Numeric(scale=s)` without precision is rejected because PostgreSQL receives
  unconstrained `NUMERIC` and silently loses the requested scale;
- bounded `LargeBinary(n)` and text collations are not supported because the
  current declared/refill contract does not preserve or compare those shapes.

Text containing NUL (`\x00`) is rejected during staging validation with a
contextual `DbPublishError`; PostgreSQL text cannot store it. Fixed-length
`CHAR`, enums and other PostgreSQL-specific type families are not part of the
initial declared-schema contract. Naive datetimes are required for `timestamp
without time zone`; timezone-aware datetimes are required for `timestamp with
time zone`.

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
| `copy_load_policy` | `CopyLoadPolicy()` | COPY spool directory, bounded I/O buffer size and deployment-wide encryption default. |

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

### Review existing declared-schema scripts

Search older running scripts for `output_schema=`. The current contract treats
schema source and publication strategy as separate choices: declared outputs
use `replace` unless they explicitly request `refill`. Add
`db_publication_strategy='refill'` only where the target must preserve its OID,
views, grants, indexes, ownership, triggers or RLS. Direct callers of
`DbPayload`, `from_petl()` or `from_pandas()` must likewise use `replace`, or
`refill` together with `output_schema`.

Review existing declarations against the type-shape and value rules in
[Inferred and declared database schemas](#inferred-and-declared-database-schemas).
Valid declarations require no change. Under `refill`, the existing target must
match the declaration exactly; task_core does not widen, cast or migrate it.
Under `replace`, the declaration is authoritative because the live relation is
replaced rather than updated in place.


## Performance and loader selection

Performance is a task-authoring decision, not an incidental database detail.
Measure the complete path: source traversal, normalization, schema work, local
spooling, PostgreSQL staging and final publication. A faster database load does
not imply a faster task when preparation dominates.

### Choose the loader deliberately

Use `db_loader='insert'` as the default for small and medium materialized
outputs. It avoids local spool construction and is often the shortest
end-to-end path when memory is not a constraint.

Use `db_loader='copy'` when one or more of these properties matter:

- the source is one-shot and must be consumed with bounded Python memory;
- output size makes materialization unsafe or operationally undesirable;
- lower staging WAL is valuable;
- PostgreSQL staging time is the dominant bottleneck;
- an encrypted local spool is acceptable on the task host.

Do not select COPY from a blanket claim that it is faster. The 0.6.8 Phase 8
baseline showed much faster PostgreSQL ingestion and lower WAL, but Python-side
serialization and spool construction offset most or all of that gain in several
profiles. Version 0.6.9 removes the unnecessary neutral-spool pass for declared
schemas. Version 0.6.10 additionally compiles family-specific field writers
that bypass the generic pandas/NumPy normalization stack for ordinary native
Python values. Benchmark representative tasks again rather than carrying any
earlier crossover forward as a guarantee.


### Database output mode matrix

Use this matrix to choose the initial configuration. It is a starting point for
measurement, not a substitute for a representative benchmark.

| Schema | Loader | Publication | Use when | Principal trade-off |
|---|---|---|---|---|
| Inferred | INSERT | `replace` | The output contract is exploratory or genuinely variable and sufficient memory is available. | No spool, but materialized row mappings can grow with output size. |
| Inferred | encrypted COPY | `replace` | Schema inference is required and the source is one-shot or must remain bounded-memory. | Requires a neutral inference spool plus the final COPY-text spool; this is the highest-preparation-cost supported mode. |
| Declared | INSERT | `replace` | The contract is stable, output is small or moderate, and avoiding a local spool matters more than memory or staging WAL. | Simple path and short publication lock, but memory grows with the materialized payload. |
| Declared | encrypted COPY | `replace` | The contract is stable and the output is large, one-shot, memory-sensitive or WAL-sensitive. | One-pass final spool, bounded memory and short publication lock; this is the strongest measured 0.6.10 combination. |
| Declared | INSERT | `refill` | Table identity and attached objects must survive, sufficient memory is available, and the output is not large enough to justify COPY. | Materialized payload plus a full target rewrite under `ACCESS EXCLUSIVE`; highest measured memory and WAL. |
| Declared | encrypted COPY | `refill` | Table identity must survive and bounded memory or faster staging matters. | COPY accelerates staging, but the target-side `TRUNCATE` and `INSERT FROM staging` still dominate lock duration and can vary with checkpoints. |
| Inferred | any | `refill` | Never. | Unsupported: `refill` requires `output_schema` so the stable target can be checked before it is truncated. |

Plaintext COPY is omitted deliberately. It is a diagnostic benchmark for
separating serialization, filesystem and encryption cost, not the production
recommendation.

### Measured 0.6.10 final acceptance

The accepted target-host campaign used PostgreSQL 18.4 and three randomized
repeats for each declared loader/publication combination at one million and ten
million rows:

| Rows | Publication | Loader | Median end-to-end | Median peak RSS | Median WAL |
| ---: | --- | --- | ---: | ---: | ---: |
| 1m | `replace` | INSERT | 33.38 s | 469.6 MiB | 122.6 MiB |
| 1m | `replace` | encrypted COPY | 9.87 s | 131.1 MiB | 77.4 MiB |
| 1m | `refill` | INSERT | 39.63 s | 469.4 MiB | 245.0 MiB |
| 1m | `refill` | encrypted COPY | 20.35 s | 131.3 MiB | 199.8 MiB |
| 10m | `replace` | INSERT | 337.39 s | 3.46 GiB | 1.20 GiB |
| 10m | `replace` | encrypted COPY | 106.12 s | 131.4 MiB | 773.5 MiB |
| 10m | `refill` | INSERT | 542.37 s | 3.46 GiB | 3.56 GiB |
| 10m | `refill` | encrypted COPY | 315.44 s | 131.7 MiB | 3.14 GiB |

These measurements support four task-authoring conclusions:

- keep INSERT as the global default because small outputs avoid spool setup and
  no universal crossover is promised;
- prefer declared encrypted COPY + `replace` for large stable outputs unless a
  representative task benchmark contradicts it;
- choose `refill` only to preserve table identity and attached objects, not as a
  performance optimization;
- treat large refill as a maintenance-window operation because publication
  rewrites the live table under `ACCESS EXCLUSIVE`, blocks readers and can
  generate several times the WAL of replacement.

COPY solves the source-to-staging transport. It does not reduce the cost of
rewriting the live table during refill. In the 10m campaign, median refill
publication was about 203 seconds for both INSERT and encrypted COPY after
staging was ready. COPY still reduced total runtime materially by making the
staging phase much faster.

The figures are evidence from one development environment, not a performance
contract. Preserve the raw campaign outside the project tree and rerun when row
width, scalar families, hardware, PostgreSQL configuration or task behavior
changes materially.

### Prefer declared schemas for stable production outputs

A declared schema gives the loader its target types and wire order before the
source is traversed. COPY can validate and serialize directly into the final
spool in one pass. The declared hot path uses one compiled writer per column,
combining missing-value handling, type validation and COPY-text encoding while
retaining the generic normalizer only for pandas, NumPy and other scalar
wrappers. Inferred COPY must retain a type-neutral first spool, resolve
the schema at EOF and replay the normalized values into the final spool.

Use inference for exploratory or genuinely variable outputs. Use
`output_schema` when the output contract is stable, especially for large COPY
loads or any `refill` target.

### Account for memory and scratch disk

INSERT owns materialized row mappings and can grow with row count. COPY keeps
Python memory bounded by column metadata, one normalized row and configured I/O
buffers, but writes local scratch data:

- declared COPY: one final COPY-text spool;
- inferred COPY: a neutral spool plus the final COPY-text spool during the
  overlap window;
- encrypted COPY: ciphertext is slightly larger than the logical body.

Size the spool filesystem for the largest expected output with operational
headroom. Keep encryption enabled for production unless the deployment has an
explicitly documented reason to accept plaintext business data on local disk.
Plaintext COPY is useful as a diagnostic benchmark, not as a default tuning
switch.

When no custom spool path is configured, task_core uses its own
`task_core-copy-spool` directory below the platform temporary directory. After
owned spool files are removed, it best-effort removes that directory if it is
empty. A configured `CopyLoadPolicy.spool_directory` is operator-owned and is
never removed. Concurrent default-root removal is tolerated: spool creation
recreates a directory removed between path resolution and file creation.

### Understand replacement and refill cost

`replace` keeps the publication lock short because publication is primarily
catalog work. `refill` preserves the table object but performs a second full-row
write while holding `ACCESS EXCLUSIVE`; elapsed time, WAL and reader blocking
therefore grow with row count and attached index/constraint work.

For large refills, monitor publication lock duration separately from staging.
A fast COPY into staging cannot remove the target-side `TRUNCATE` and
`INSERT FROM staging` cost. Run large refills in an exclusive maintenance
window when multi-minute reader blocking, WAL volume or replica lag would be
operationally significant.

Partition or relation switching may remove the row rewrite, but it is a
different publication architecture with new constraints, cleanup and rollback
semantics. Consider it only after measured refill lock duration or WAL volume
violates an operational SLA.

### PostgreSQL settings that matter

Tune only after stage-level measurements show a database bottleneck. For large
bulk loads and refills, inspect checkpoint and WAL pressure first. A
`max_wal_size` smaller than one campaign's WAL volume can cause avoidable
checkpoints. On a shared 16 GB development machine, test conservative values
such as 1–2 GB `shared_buffers` and a larger `max_wal_size` in an isolated A/B
campaign; do not present them as universal production settings.

`work_mem`, `maintenance_work_mem` and `effective_cache_size` usually do not
control this straight staging path. Do not disable `fsync` or
`full_page_writes`, and do not change durability semantics merely to improve a
benchmark number.

### Benchmark a task correctly

Use representative row width and value families, not row count alone. For a
loader decision:

1. warm the environment with a non-recorded run;
2. run at least three measured repeats;
3. randomize INSERT, plaintext COPY and encrypted COPY order;
4. report median, minimum and maximum;
5. capture source, spool, database staging and publication durations
   separately;
6. capture peak worker RSS, spool sizes, WAL, checkpoint and lock metrics;
7. invalidate runs affected by suspend, sleep, competing workloads or changed
   configuration;
8. preserve raw logs and environment facts outside the project tree.

Treat differences of only a few percent as inconclusive until repeated. Record
both the fastest path and the operational trade-off: memory, disk, WAL,
publication blocking and artifact protection.

### Practical decision rule

Keep INSERT as the global default. For small or moderate outputs, and for
inferred outputs where memory is sufficient, begin with INSERT. For a large
stable declared output, begin with encrypted COPY and `replace` unless table
identity or attached objects require `refill`. Keep inferred COPY only when
schema inference and bounded-memory traversal are both required. Benchmark the
representative task before treating any crossover as permanent.


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
