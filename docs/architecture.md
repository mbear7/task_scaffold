# Architecture

How `task_core` works as of 0.2.11. This describes the present system, not
how it came to be that way; durable rationale lives in
[decisions/](decisions/), and the history is in git and
[CHANGELOG.md](../CHANGELOG.md).


## Layering

Modules form a strict hierarchy. Nothing at a lower level imports from a
higher one.

```
level 0   types.py                    dataclasses, errors, PORTABLE_IDENTIFIER_RE
          cleanup.py                  attempt_all_cleanup()
          logging_setup.py
          openpyxl_compat.py

level 1   file_access.py              local and SMB file/workbook access
          excel_metadata.py
          source_tracking.py          fingerprints

level 2   context.py                  task_context: lazy resources, close-once
          binding.py                  ResourceSpec, bind(), wiring
          resources/                  excel, file_set, db
          db_publish.py               DbPublisher, payload construction
          source_state.py             SourceStateStore
          table_adapters.py           petl / pandas behind one interface
          export.py

level 3   runner.py                   run_pipelines()
```

Within level 2 there are lateral dependencies: `table_adapters.py` imports
payload constructors from `db_publish.py`, and `export.py` imports
`get_table_adapter` from `table_adapters.py`.

`runner.py` imports `context.py` and `source_tracking.py` under
`TYPE_CHECKING` only — it duck-types the context and the source-change
config rather than depending on them at runtime.

`runner.py` imports neither petl nor pandas and never branches on which
engine a pipeline uses — every engine difference is reached through the
adapter interface.

Other modules do import an engine, for different reasons:
`table_adapters.py` imports both because encapsulating their differences
is its job; `db_publish.py` imports pandas to normalize scalar values;
`resources/excel.py` and `resources/db.py` import petl because
**resources return petl tables**. That last one is visible to task
authors: a pandas pipeline reading an Excel or DB resource receives petl
tables and converts them itself.


## Run lifecycle

`run_pipelines()` is the entry point and owns the whole lifecycle.

```
validate_pipeline_classes(pipelines, run_sequence)
    structural checks: run_sequence names exist, no duplicates,
    each class has a usable run(), no two active pipelines
    declare the same output target

publisher_factory.preflight(specs, schema, source_state_target, ...)
    backend-specific validation of declared identifiers.
    Pure, no connection. See "Identifier validation" below.

build_context()
    the task's own function, returning a task_context

  ── from here, everything is inside try/finally ──

source-change check (if enabled and output_db)
    ensure the source-state table, read stored fingerprints,
    compare against the current ones
    → unchanged and not force_run: return RunResult(skipped=True)

for each pipeline in run_sequence:
    resolve bound resources (lazily constructed on first use)
    run the pipeline
    validate the returned table via the adapter
    stabilize if it will be traversed more than once
    count rows
    publish_result → store in the context for later pipelines
    export Excel if enabled
    build and publish the DB payload if enabled

update_source_state()      writes new fingerprints
publisher.commit()         swaps staging tables, then commits

  ── finally ──

publisher.close()
ctx.close()
```

Cleanup runs whether or not the body succeeded, and a failure during
cleanup never replaces the original exception. See "Failure and cleanup".


## Resources

A resource is anything a pipeline reads: a workbook, a folder of
workbooks, a database connection. Resources are declared once at module
level in the task file and referenced by pipelines through `bind()`.

```python
SSCH_FILES = xlsx_file_set('ssch', pattern='*.xlsx', tracker=True)
RESOURCES = {'ssch_files': SSCH_FILES}
PIPELINES = {'ssch2': bind(ssch2, source=SSCH_FILES)}
```

### Lazy construction, cached identity

`task_context` holds *loaders*, not resources. A resource is constructed
on first `get_resource()` and cached under its key. Two consequences that
the rest of the design depends on:

- A resource declared but never reached by any pipeline in `RUN_SEQUENCE`
  is never constructed. No file is opened, no connection is made.
- The resource fingerprinted during source-change checking is *the same
  object* later injected into the pipeline. Not an equal one — the same
  one. A file cannot be selected during fingerprinting and a different
  file read during the run.

### Ownership and closing

The context owns every resource it constructed and closes them all in
`close()`, which is idempotent. Resources are not closed individually by
pipelines.

`excel_resource` retains one open workbook for its lifetime and caches
sheet rows, tables, ranges and row metadata. `close()` drops its
references to the workbook *before* releasing it, then clears every cache.
`db_resource` closes its connection and clears its table cache, and does
both even if the close itself fails.

See [decisions/0003](decisions/0003-gc-collect-for-remote-workbook-handles.md)
for why releasing a workbook involves `gc.collect()`.

### Kinds

| builder | what it gives the pipeline |
| --- | --- |
| `build_excel_resource(path)` | one workbook: sheets, tables, named ranges, row metadata |
| `build_file_set_resource(folder, pattern)` | a folder of workbooks, plus selection (latest / fixed / all) |
| `build_db_resource(creds=...)` | petl tables from queries or whole tables, optionally server-side cursors |

`latest_xlsx()` and `xlsx_file_set()` are the declarative forms used in
task files; they produce a `ResourceSpec` that the context turns into a
resource when needed.


## Pipelines and specs

A pipeline is a class with a `spec` and a `run(ctx, **resources)`
classmethod. It returns a table — a petl table or a pandas DataFrame.

```python
class ssch2:
    spec = PipelineSpec(excel_name='ssch2.xlsx', db_table='hr_ssch2')

    @classmethod
    def run(cls, ctx, source):
        ...
        return table
```

The spec declares *what to do with the result*, not how to compute it.
Full field reference is in
[task-authoring.md](task-authoring.md#pipelinespec).

`bind()` wires resources to parameter names. Binding is validated
structurally for every declared pipeline, whether or not it is in
`RUN_SEQUENCE`; the active resource set is derived from `RUN_SEQUENCE`.


## Table adapters

petl and pandas are reached through one six-method interface, selected
per pipeline by `spec.table_adapter` or inferred from the returned object.

```
validate(tbl)          reject something that is not a table of this kind
nrows(tbl)             row count
display(tbl)           debug output
to_excel(tbl, name)    write a workbook
to_db_payload(tbl, …)  build a DbPayload
stabilize(tbl, …)      materialize a lazy table that will be traversed twice
```

`stabilize()` exists because a petl table is a lazy generator re-traversed
on every pass. A pipeline that both counts rows and exports Excel would
otherwise recompute the whole chain. `run_pipelines()` stabilizes before
the first traversal when it knows more than one is coming.


## Source-change checking

Optional, enabled per task through `SourceChangeCheckConfig`. Requires
`output_db`, because the stored state lives in a PostgreSQL table.

Each tracked resource produces a `SourceFingerprint`: root path, mask,
recursion flag, file count, total size, latest modification time, and a
signature over the selected files. These are compared against the values
stored under the task's name from the previous run.

If nothing changed and `force_run` is not set, the run returns
`RunResult(skipped=True)` without executing any pipeline.

The state table is written in the same transaction as the published
tables, so a failed run does not advance it. A retry after a failure sees
the same sources as changed and runs again.


## Database publication

### Payload construction

`to_db_payload()` produces a `DbPayload`: table name, schema, ordered
column names, rows as a list of dicts, and optional type overrides.
`db_contract` renames source columns to target names and restricts the
column set, and is applied by the scaffold. `db_updated_at` appends a
timestamp column afterwards.

`db_output` is **declarative only**. The scaffold validates it and reads
it during preflight, but does not apply it — the pipeline projects its own
columns, conventionally with `.cut(*cls.spec.db_output)`. Declaring it
without cutting produces a payload with the pipeline's own columns and no
error.

Values are normalized on the way in: pandas and numpy scalars become
plain Python objects, and every flavour of missing value becomes `None`.
Containers are left alone — a one-element list is a value, not a scalar.

### Type inference

Column types are inferred from the data unless pinned with
`db_type_overrides`. The first 5000 rows are sampled; if the sampled
answer is one that PostgreSQL could silently widen — `BigInteger` or
`Date` — the remaining rows are swept with a cheap exact-type check, and
the column is re-inferred over everything if the sample turns out too
narrow. If the whole sample is null, the sample is discarded and the full
column scanned.

See [decisions/0001](decisions/0001-replace-tables-instead-of-truncating.md)
for why inference is viable at all, and its limitations for tables with
downstream consumers.

### Staging and swap

`publish()` does not touch the live table. It creates a staging table with
the freshly inferred schema, loads it, and records a pending swap.
`commit()` drops each live table and renames its staging table into place,
sorted by final name, then commits.

Staging names are
`<shortened readable prefix>__stg_<target_token>_<run_token>`. Only the
readable prefix is ever shortened, by bytes and on character boundaries.
`target_token` hashes schema, final table name, and the staging namespace
constant, and deliberately excludes the run — which is what makes
collisions statically checkable. `run_token` isolates concurrent runs.

### Identifier validation

Two tiers.

**Preflight**, before `build_context()`: pure, connection-free, and always
run when anything declares a DB target or source-change checking is
enabled. Validates the schema, each declared `db_table`, each derived
staging name, cross-spec staging collisions, declared column targets, and
the source-state schema and table. Rejects any pipeline declaring the
source-state table as its own target.

**Runtime**, when publishing *and* when source-change checking builds its
store: reads the server's own `max_identifier_length` and uses the lower
of that and the configured limit — configuration can only tighten. It
validates the payload's table name and columns under `portable` mode, its
schema under either mode, and does so after all renaming has happened. It
asserts generated staging names and guards against collisions.

The source-state table gets the same treatment before its own DDL runs,
because it is a real table this run creates and writes. `SourceStateStore`
performs that check itself rather than through a new publisher method —
`publisher_factory` is an extension seam and has been expanded once by
accident already. It also compares the existing table's columns against
what it reads and writes, so a table left by an older version fails at
startup instead of at the first write, mid-run.

Names must match `^[a-z_][a-z0-9_]*$` unless a pipeline sets
`db_identifier_mode='quoted'`. See
[decisions/0004](decisions/0004-lowercase-portable-identifiers.md).


## Transactions

> This section describes 0.2.11 and is the part of the architecture most
> likely to change. A staged model — per-target preparation transactions
> and one short publication transaction — is designed but not implemented.

One connection, and one *publication* transaction — not one transaction
for everything.

```
source-state ensure/read      implicit transaction, autobegun by the
                              first statement
discard_pending_read()        rolls that transaction back
first publisher.publish()     BEGIN the explicit publication transaction
remaining pipeline work       inside it
update_source_state()         inside it
staging swap                  inside it
commit()                      COMMIT
```

The source-state *read* runs in its own implicit transaction, which is
deliberately discarded. `DbPublisher` distinguishes that implicit
transaction from the explicit one it opens for publishing, and
`discard_pending_read()` clears it — otherwise the first `publish()` calls
`conn.begin()` on an already-transacted connection, which SQLAlchemy
rejects.

The source-state *write* is inside the publication transaction, so
everything the run publishes lands together or not at all: a failed run
does not advance the stored fingerprints.

When source-change checking is enabled but no pipeline declares a
`db_table`, `publish()` is never called and no explicit transaction is
opened; the source-state write runs in an implicit one and `commit()`
commits the connection directly.

**Cost.** The publication transaction stays open across pipeline execution — remote
file reads, transformations, Excel exports. The staging swap keeps live
tables unlocked until the end, but the transaction itself is as long as
the run: it holds catalog and staging locks, delays vacuum, accumulates
WAL, and makes a late rollback expensive.

**Concurrency.** Nothing prevents two runs of the same task from
overlapping. They stage against different physical tables and only
contend at the final swap.


## Failure and cleanup

`run_pipelines()` tracks the primary error explicitly rather than
inferring it, then runs every cleanup step regardless of outcome.

```
try:
    … whole run …
except BaseException as exc:
    primary_error = exc
    publisher.rollback()
    raise
finally:
    publisher.close()
    ctx.close()
```

`attempt_all_cleanup()` runs every step even if earlier ones raised, and
collects the failures. The single caller decides what to do with them:

- **The run already failed** — cleanup errors are logged and discarded.
  The original exception propagates. A failure to close a workbook must
  not hide the failure that actually broke the task.
- **The run succeeded** — cleanup errors are raised. A leaked connection
  or unreleased remote handle is a real failure and a run that leaks one
  did not fully succeed.

Multiple cleanup failures on a successful run are raised as an
`ExceptionGroup`.

`ctx.close()` is idempotent and marks the context closed before releasing
anything, so a failure part-way through does not leave it half-open.


## Extension points

- **`publisher_factory`** — anything with `publish`, `commit`, `rollback`,
  `close`, `discard_pending_read`, `ensure_connection`, and the four
  result properties. May optionally provide a `preflight` classmethod; if
  it does not, the real `DbPublisher.preflight` is used, so validation
  always runs.
- **`build_context`** — the task supplies its own, or uses
  `build_resource_context()` for the standard `RESOURCES`/`bind()` model.
- **`table_adapter`** — registered in `table_adapters.py`. Adding one means
  implementing the five methods; nothing in `runner.py` changes.
- **`source_access`** — `build_source_access()` selects local or SMB file
  access; a resource takes whichever it is given.
