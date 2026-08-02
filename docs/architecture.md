# Architecture

How `task_core` works as of 0.6.8. This describes the present system, not
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
          db_insert.py                staging-table INSERT loader

level 1   file_access.py              local and SMB file/workbook access
          excel_metadata.py
          source_tracking.py          fingerprints
          db_values.py                stateless schema/value kernel

level 2   context.py                  task_context: lazy resources, close-once
          binding.py                  ResourceSpec, bind(), wiring
          resources/                  excel, file_set, db
          db_publish.py               DbPublisher, payload construction
          db_copy.py                  COPY spool preparation + DBAPI transport
          source_state.py             SourceStateStore
          table_adapters.py           petl / pandas behind one interface
          export.py

level 3   runner.py                   run_pipelines()
```

Within level 2 there are lateral dependencies: `table_adapters.py` imports
payload constructors from `db_publish.py`, `export.py` imports
`get_table_adapter` from `table_adapters.py`, and `db_publish.py` re-exports
`CopyLoadPolicy` from `db_copy.py` (the config's home matches its layer;
`db_copy` does not import back from `db_publish`).

`runner.py` imports `context.py` and `source_tracking.py` under
`TYPE_CHECKING` only — it duck-types the context and the source-change
config rather than depending on them at runtime.

`runner.py` imports neither petl nor pandas and never branches on which
engine a pipeline uses — every engine difference is reached through the
adapter interface.

Other modules do import an engine, for different reasons:
`table_adapters.py` imports both because encapsulating their differences
is its job; `db_publish.py` imports pandas to accept a DataFrame in
`from_pandas()`; `db_values.py` imports pandas to normalize scalar values
and identify missing markers; `resources/excel.py` and `resources/db.py`
import petl because **resources return petl tables**. That last one is visible to task
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

publisher.begin_run() (if the run touches PostgreSQL)
    claim the task's advisory lock; drop staging artifacts left by a
    dead previous run
    → lock not acquired: return RunResult(skipped=True)

source-change check (if enabled and output_db)
    collect fingerprints, ensure the source-state table, read stored
    fingerprints, compare
    → unchanged and not force_run: return RunResult(skipped=True)

for each pipeline in run_sequence:
    resolve bound resources (lazily constructed on first use)
    run the pipeline
    validate the returned table via the adapter
    stabilize if another consumer requires a second traversal
    count rows up front except for database-only COPY
    publish_result → store in the context for later pipelines
    export Excel if enabled
    prepare and load the DB payload if enabled (its own committed transaction)
    database-only COPY obtains its exact row count during spool preparation

queue the source-state write
publisher.commit()         publication transaction: verify, write source
                           state, swap every staging table, commit

publisher.close()          release the advisory lock, close

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
    def run(cls, ctx, *, source):
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

petl and pandas are reached through one seven-method interface, selected
per pipeline by `spec.table_adapter` or inferred from the returned object.

```
validate(tbl)          reject something that is not a table of this kind
nrows(tbl)             row count
display(tbl)           debug output
to_excel(tbl, name)    write a workbook
to_db_payload(tbl, …)  build a DbPayload for the INSERT path
to_row_source(tbl)     (columns, DbRowSource) for the row-source path
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


## Excel output

Written immediately, inside the pipeline loop, with no staging and no
temporary files. A workbook exists as soon as its pipeline has produced
it, whether or not the rest of the run succeeds.

This is deliberate and permanent: Excel output is a local debugging aid,
not a publication target, and does not get the guarantees the database
path has. See
[decisions/0007](decisions/0007-excel-output-is-a-debugging-aid.md) for
why the asymmetry is the design rather than an unfinished half of it.


## Database publication

### Payload construction

`to_db_payload()` produces a `DbPayload`: table name, schema, ordered
column names, rows as a list of dicts, and optional type overrides.
`db_contract` renames source columns to target names and restricts the
column set, and is applied by the scaffold. `db_updated_at` appends a
framework-owned `TIMESTAMPTZ NOT NULL` column afterwards: `True` uses the
default name `etl_updated_at`, while a string supplies a custom portable
lower-case name.

`db_output` is **declarative only**. The scaffold validates it and reads
it during preflight, but does not apply it — the pipeline projects its own
columns, conventionally with `.cut(*cls.spec.db_output)`. Declaring it
without cutting produces a payload with the pipeline's own columns and no
error.

Values are normalized on the way in: pandas and numpy scalars become
plain Python objects, and every flavour of missing value becomes `None`.
Containers are left alone — a one-element list is a value, not a scalar.

The COPY path applies the same normalization once while consuming its
one-shot positional row source. It writes a type-neutral local spool while
accumulating schema state, resolves one schema at EOF, replays into a final
PostgreSQL COPY-text body, and streams that body through psycopg2
`copy_expert()` on the publisher's existing connection and preparation
transaction. No decrypted temporary file is created.

### Type inference

Column types are inferred from the data unless pinned with
`db_type_overrides`. Inferred columns may also be marked `NOT NULL` with
`db_not_null_columns`.

A pipeline that supplies `output_schema` uses the second schema resolver. It
validates the complete produced user-column set, normalizes and validates every
value against the declared type/nullability contract, reorders into declaration
order, appends enabled framework-owned columns, and produces the same internal
`ResolvedSchema` used by inference. The timestamp column configured by
`db_updated_at` is therefore not repeated in `output_schema`.

Schema source and publication strategy are independent. Both inferred and
declared outputs use staged `DROP`/`RENAME` replacement by default. A declared
payload may explicitly request stable refill; only that path verifies exact
catalog compatibility and later uses `TRUNCATE` plus `INSERT FROM` staging.
All source-state work and all refill preflight complete before the first
live-target lock.

The first 5000 rows are sampled. If the sampled answer is one that PostgreSQL
could silently widen — `BigInteger`, `Date`, or either timestamp awareness —
the remaining rows are swept with a cheap compatibility check, and the column
is re-inferred over everything if the sample turns out too narrow or
ambiguous. If the whole sample is null, the sample is discarded and the full
column scanned.

Naive datetimes infer `TIMESTAMP`; timezone-aware datetimes infer
`TIMESTAMPTZ`. A column that mixes aware datetimes with naive datetimes or bare
dates is rejected before database work because task_core does not assume a
timezone for values that do not carry one. A date may still widen together
with naive datetimes to `TIMESTAMP` at midnight.

### COPY spool protection

COPY preparation uses two local, versioned spool containers: `neutral` and
`copytext`. Their bodies are encrypted by default with independently generated
AES-256-GCM keys that task_core retains only on in-memory preparation objects
and never intentionally persists. This does not promise exclusion from swap,
process dumps or library-internal copies. The ownership header remains plaintext so a
later run can identify abandoned files without recovering their contents.

`PipelineSpec.db_copy_spool_encryption=False` is an explicit per-task opt-out;
it keeps the same container, permissions, ownership checks and cleanup rules
but stores the body in plaintext and emits a warning. The final encrypted body
is read through a bounded decrypting stream. No decrypted spool file is
created.

Unless `PublisherConfig.copy_load_policy.spool_directory` supplies another
path, spools reside under
`Path(tempfile.gettempdir()) / 'task_core-copy-spool'` on the Python task host,
not on the PostgreSQL server. Current-run cleanup uses bounded retries and logs
exact residual paths. Positive predecessor cleanup additionally requires the
filename token/stage and plaintext header token/stage/task to agree. After `begin_run()` acquires the
task advisory lock, it deletes positively identified spools left by earlier
executions before preparing new output. This includes residue left when a
process crash prevented current-run cleanup. Unknown, malformed and foreign
files remain untouched. If a positively owned predecessor still cannot be
removed after bounded retries, startup fails rather than knowingly accumulating
another spool beside it.

See [decisions/0001](decisions/0001-replace-tables-instead-of-truncating.md)
for why inference is viable at all, and its limitations for tables with
downstream consumers.

### Staging and publication

`publish()` prepares one output in its own committed transaction: it resolves
or prepares the schema, creates a staging table, loads it through INSERT or
COPY, verifies it, attaches ownership metadata, and commits. COPY spool
preparation happens before that transaction; the final spool is removed after
successful database consumption and before the preparation transaction commits.
The live table is untouched.

`commit()` is the publication phase. It verifies every prepared artifact,
preflights only targets explicitly configured for refill, creates and fills an
absent refill target when necessary, runs queued source-state work, then locks
all existing targets in deterministic sorted order. Replacement targets,
inferred or declared, use `DROP`/`RENAME`; explicit refill targets retain their
identity and use `TRUNCATE` plus `INSERT FROM` staging. Comments are replaced
with framework provenance and the whole multi-table publication commits
atomically.

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
validates the payload's schema, table and final column names under the one
portable lower-case contract, after all renaming has happened. It
asserts generated staging names and guards against collisions.

The source-state table gets the same treatment before its own DDL runs,
because it is a real table this run creates and writes. `SourceStateStore`
performs that check itself rather than through a new publisher method —
`publisher_factory` is an advertised extension seam, so protocol growth must
be deliberate and documented rather than introduced merely to route one
internal check. It also compares the existing table's columns against what it
reads and writes, so a table left by an older version fails at startup instead
of at the first write, mid-run.

Names must match `^[a-z_][a-z0-9_]*$`. Generated SQL still quotes
identifiers defensively, but the public contract has no quoted-name mode.
See [decisions/0010](decisions/0010-require-portable-database-identifiers.md).


## Transactions

Many committed preparation transactions, then one atomic publication
transaction. The final transaction is normally short for replacement,
regardless of schema source. An explicit stable refill remains open while rows,
indexes and database-side constraints are rebuilt. See
[decisions/0005](decisions/0005-prepare-staging-outside-the-publication-transaction.md)
for why, and what it costs.

```
begin_run()                claim the task, clean predecessor artifacts
                           (its own committed transaction)

source-state ensure/read   implicit transaction, autobegun

for each pipeline with a DB target:
    BEGIN
      resolve inferred or declared schema
      create and load a staging table
      verify exact ordered column names and the row count
      attach ownership metadata
    COMMIT                 <- the live table is still untouched

BEGIN                      <- the atomic publication transaction
  verify every prepared artifact still exists and is still ours
  validate existing explicit-refill targets; create/fill absent refill targets
  run queued work (the source-state write)
  enforce A >= n * L + M for the actual existing lock set
  lock all existing targets in deterministic sorted order
  replace: DROP live target, RENAME staging into place
  refill: TRUNCATE stable target, refill from staging, DROP staging
  replace comments with provenance
COMMIT

release the task lock, close
```

The source-state read phase commits itself, in `sources_unchanged()`.
Leaving it to `_ensure_transaction()` meant the autobegun transaction
survived until the first `publish()` — which, for a source-check-only task
or one whose first DB output came late, was the whole run. `no transaction
spans the run` only holds because the store closes its own phase.

**Lock bounding.** All targets are locked in one sorted statement under a
bounded wait before anything is published. For the actual existing lock set,
the publisher enforces `acquisition_timeout_ms >= n * lock_timeout_ms + M`;
otherwise cumulative per-target waits could surface as terminal `57014` rather
than retryable `55P03`. The whole publication is retried on lock unavailability
within a wall-clock horizon. Without it, a publisher waiting on one long reader
blocks every reader arriving afterwards. See
[decisions/0008](decisions/0008-bound-the-publication-lock-wait.md).

**Atomicity.** Publication is all-or-nothing: every replacement, explicit
refill and source-state write lands in one transaction, so a failed publication does
not advance the stored fingerprints and a retry sees the same sources as
changed. Preparation is deliberately *not* part of that guarantee — a
prepared staging table is committed and visible, and is cleaned up rather
than rolled back.

**Duration.** No transaction spans the run. Each preparation transaction
is bounded by one table's load. Replacement publication is normally
row-independent catalog work; explicit refill is row- and index-dependent and
holds the live-table lock for that full critical section.

**Concurrency.** A session-scoped advisory lock, claimed before
fingerprinting, means two runs of the same task cannot overlap — the
second exits immediately with `skipped=True` and a distinct
`skip_reason`. See
[decisions/0006](decisions/0006-three-rules-that-keep-cleanup-safe.md).

**The lock is enforced by the publisher**, not just by the runner:
`publish()` and `commit()` refuse on PostgreSQL without it, and an
invalidated connection is detected before every reuse — SQLAlchemy would
otherwise reconnect transparently onto a session holding none of this
run's locks.

**Cleanup.** Because preparation commits, `rollback()` drops this run's
staging tables rather than undoing a transaction, and never raises. A run
that dies without cleaning up leaves artifacts that the next run of the
same task removes under the lock.

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

- **`PublisherConfig`** — one frozen object holding `publisher_factory`,
  `identifier_policy`, `publication_lock_policy` and `copy_load_policy`, passed to
  `run_pipelines()` as `publisher_config`. A factory is anything with
  `publish`, `commit`, `rollback`, `close`, `begin_run`,
  `ensure_connection`, and the four result properties; it is constructed
  with `creds`, `schema`, `logger`, `identifier_policy`,
  `publication_lock_policy`, `publication_plan`, `task_name` and
  `copy_load_policy`. May optionally provide a `preflight` classmethod; if
  it does not, the real `DbPublisher.preflight` is used, so validation
  always runs.
- **`build_context`** — the task supplies its own, or uses
  `build_resource_context()` for the standard `RESOURCES`/`bind()` model.
- **`table_adapter`** — registered in `table_adapters.py`. Adding one means
  implementing the seven methods; nothing in `runner.py` changes.
- **`source_access`** — `build_source_access()` selects local or SMB file
  access; a resource takes whichever it is given.
