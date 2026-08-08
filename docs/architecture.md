# Architecture

How `task_core` works as of 0.7.9. This describes the present system, not
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
          db/insert.py                staging-table INSERT loader

level 1   file_access.py              local and SMB file/workbook access
          excel_metadata.py
          source_tracking.py          fingerprints
          db/values.py                stateless schema/value kernel
          db/identifiers.py           names, staging comments, lock key
          db/payload.py               DbPayload, RowProjection, constructors
          db/policies.py              plan, lock, identifier and COPY policy
          db/spool_format.py          spool identity, header, neutral framing
          db/copytext.py              PostgreSQL COPY-text serialization
          db/spool_io.py              spool handles, AES-GCM, cleanup

level 2   context.py                  task_context: lazy resources, close-once
          binding.py                  ResourceSpec, bind(), wiring
          resources/factories.py      declarative xlsx_*/csv_* recipes
          resources/excel.py          one workbook
          resources/csv.py            CSV parsing, lazy re-iterable tables
          resources/file_set.py       a folder of workbooks
          resources/db.py             petl tables from queries
          db/publish.py               DbPublisher, payload construction
          db/copy.py                  COPY spool preparation + DBAPI transport
          source_state.py             SourceStateStore
          table_adapters.py           petl / pandas behind one interface
          export.py

level 3   runner.py                   run_pipelines()
```

`db/` is one subsystem — publication lifecycle, staging loaders, spool
format and the schema/value kernel — with its own internal layering, shown
above and enforced by the same test. Within it:

```
publish → payload      → values
publish → identifiers  → values
publish → policies     → identifiers
publish → insert
publish → copy         → spool_io → spool_format → policies
publish → copy         → copytext → values
```

`values.py` is the bottom of the subsystem and imports nothing from it.
`publish.py` is the top and is the only member that connects, locks or
commits. The rest are stateless: shapes, names and configuration. That order
is enforced, not just drawn — see
`tests/test_docs.py::test_the_db_subsystem_order_is_as_documented`.

Submodules are listed individually rather than as `db/` and `resources/`
group entries. The level check resolves an import by its package-relative
path, so a group entry would match nothing and silently exempt everything
inside it.

Within level 2 there are lateral dependencies: `table_adapters.py` imports
payload constructors from `db/payload.py` and missing-value semantics
directly from `db/values.py`; `export.py` imports `get_table_adapter` from
`table_adapters.py`.

Every module imports a name from the module that *defines* it, never
through one that merely re-imports it. `CopyLoadPolicy` lives in
`db/policies.py` with the other three publication policies, so
`db/publish.py` takes it from there rather than through `db/copy.py`, and
`source_state.py` takes identifier rules from `db/identifiers.py` rather
than through `db/publish.py`. Definitions moving without their dependency
edges moving is how a package stays coupled while looking split, and the
ordering test cannot see it: both spellings are legal edges.

`runner.py` imports `context.py` and `source_tracking.py` under
`TYPE_CHECKING` only — it duck-types the context and the source-change
config rather than depending on them at runtime.

`runner.py` imports neither petl nor pandas and never branches on which
engine a pipeline uses — every engine difference is reached through the
adapter interface.

Other modules do import an engine, for different reasons:
`table_adapters.py` imports both because encapsulating their differences
is its job; `db/payload.py` imports pandas to accept a DataFrame in
`from_pandas()`; `db/values.py` imports pandas to normalize scalar values
and identify missing markers; `resources/excel.py`, `resources/csv.py` and
`resources/db.py` import petl because **resources return petl tables**.
`db/publish.py` imports neither: it publishes a `DbPayload`, and building
one from a DataFrame is `db/payload.py`'s job. That last one is visible to
task authors: a pandas pipeline reading an Excel, CSV or DB resource
receives petl tables and converts them itself.


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
workbooks, a CSV file or folder, a database connection. Resources are
declared once at module level in the task file and referenced by pipelines
through `bind()`.

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
| `build_xlsx_file_resource(path)` | the same workbook, plus the selection metadata a source-change check needs |
| `build_latest_xlsx_resource(folder, pattern)` | the newest matching workbook, tracked |
| `build_file_set_resource(folder, pattern)` | a folder of workbooks, plus selection (latest / fixed / all) |
| `build_csv_file_resource(path)` | one CSV file as a lazy petl table |
| `build_latest_csv_resource(folder, pattern)` | the newest matching CSV, tracked |
| `build_csv_file_set_resource(folder, pattern)` | a folder of CSVs as one logical table |
| `build_db_resource(creds=...)` | petl tables from queries or whole tables, optionally server-side cursors |

`build_excel_resource()` takes a path and knows nothing about where it came
from, so `source_fingerprint()` refuses. The two builders below it capture
the `SelectedFile` that chose the workbook, and that is the whole difference
— the parsing is the same code.

The three CSV builders share one parser. `build_csv_file_set_resource()`
*composes* `build_file_set_resource()` rather than extending it — selection,
ordering, membership and the file-set fingerprint stay in the generic
layer, which also holds workbooks and arbitrary binaries, while the CSV
wrapper owns decoding, headers, widths and cross-file agreement. See
[decisions/0015](decisions/0015-add-first-class-csv-input-resources.md).

`latest_xlsx()`, `xlsx_file()`, `xlsx_file_set()`, `latest_csv()`,
`csv_file()` and `csv_file_set()` are the declarative forms used in task
files; they produce a `ResourceSpec` that the context turns into a resource
when needed.


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


Declared COPY compiles one family-specific field writer per output column before
source traversal. For ordinary native Python values, each writer performs
missing handling, declared validation and COPY-text encoding directly into a
reused row buffer. The generic pandas/NumPy normalization kernel is retained as
a fallback only for non-native scalar wrappers. No normalized row tuple is
allocated on the declared hot path.

COPY preparation uses local, versioned spool containers named `neutral` and
`copytext`. Inferred mode uses both; declared mode writes only the final
`copytext` spool. Their bodies are encrypted by default with independently generated
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

task_core removes owned spool *files* and leaves the shared root directory in
place. It does not remove the root, and never removes an operator-configured
spool directory. An empty directory under the platform temporary directory is
not residue; it may persist until removed by external temporary-directory
maintenance. 0.6.11 did remove the root, and that made one task able to delete
a directory another had just resolved — the framework-controlled source of
that race. See `decisions/0011`.

The exclusive file-open path keeps one defensive recreate for deletion by
something outside task_core, such as a temporary-directory reaper. A
filesystem exception propagating from a spool operation keeps its native type
rather than being normalized into `DbPublishError`; predecessor cleanup is the
deliberate exception, refusing to continue when known residue survives. See
`decisions/0011`.

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
validates the payload's schema, table and final column names after all
renaming has happened. It asserts generated staging names and guards against
collisions.

Schemas, table names and generated relation names take the portable
lower-case contract: `^[a-z_][a-z0-9_]*$`, chosen so an identifier behaves
identically quoted or unquoted. **Published column names take a wider one**,
`^[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*$`, permitting a dot between parts so
that analytical vocabulary such as `lev.1` need not be renamed. A dotted
column is deliberately not portable in that sense and must be quoted in
hand-written SQL. Both contracts are subject to the same 63-byte limit. See
`decisions/0014`.

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
