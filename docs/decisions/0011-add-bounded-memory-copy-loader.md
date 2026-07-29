# 0011 — Add a bounded-memory `COPY FROM STDIN` loader

Status: proposed

Not implemented in 0.5.0.

## Problem

`task_core` 0.5.0 prepares PostgreSQL staging tables through chunked
SQLAlchemy inserts. The path is proven and remains the compatibility baseline,
but it has two costs that become dominant for large or wide outputs:

- every output row is materialized as a Python dictionary before preparation;
- every row is sent through SQL statement binding rather than PostgreSQL's
  bulk-ingestion protocol.

Increasing the insert chunk size may reduce round trips, but it does not remove
the `O(rows)` Python object graph or the per-row binding work.

PostgreSQL provides `COPY FROM STDIN` for bulk ingestion. Adding it is useful
only if it changes the staging transport without weakening the guarantees that
already exist in 0.5.0:

- one session-scoped task advisory lock;
- ordinary logged staging tables;
- one committed preparation transaction per output;
- exact positive ownership of staging artifacts;
- fatal connection-loss behavior with no transparent reconnect;
- atomic multi-table publication and source-state advancement;
- bounded and deterministically ordered live-target locking;
- inferred `DROP`/`RENAME` publication;
- declared stable-target creation or `TRUNCATE` plus refill;
- whole-publication rollback and retry;
- portable lower-case identifiers;
- deterministic predecessor cleanup.

The loader must change only how rows enter a staging table. Once preparation
commits, an insert-loaded and a COPY-loaded staging table must participate in
exactly the same publication protocol.

The current runner also matters. It always obtains `nrows()` before database
preparation and stabilizes PETL output when a later consumer will traverse it.
For a lazy PETL source, stabilization uses `etl.cache()` and therefore
materializes all rows in memory. A COPY transport added only inside
`DbPublisher.publish()` would still inherit that `O(rows)` cache and would not
provide an end-to-end bounded-memory path.

COPY therefore requires a small lifecycle change above the transport layer as
well as a responsibility-based module split below it.

## Current 0.5.0 baseline

The decision is based on the actual 0.5.0 scaffold, not the earlier single-mode
publisher.

### Two schema sources, one resolved representation

```text
output_schema is None
→ infer the complete schema, with optional type and NOT NULL overrides

output_schema is supplied
→ use the complete declared schema and disable inference
```

Both paths produce `ResolvedSchema` / `ResolvedColumn`.

Declared mode already performs strict normalization, complete column-set
validation, declaration-order reordering, nullability checks and target-aware
scalar validation before the preparation transaction opens.

Inferred mode resolves the complete schema from materialized rows, including
the existing late-value protection for sampled `BIGINT` and `DATE` results.

### Two publication mechanisms

Inferred outputs retain staged replacement:

```text
prepare staging
→ lock existing target
→ DROP target
→ RENAME staging
→ commit
```

Declared outputs retain stable identity:

```text
first publication:
prepare staging
→ create and fill permanent target atomically
→ commit

later publication:
prepare staging
→ verify exact target compatibility
→ lock target
→ TRUNCATE
→ INSERT FROM staging
→ commit
```

COPY does not select or alter either publication mechanism. Schema mode still
determines publication behavior.

### Current payload shape

`from_pandas()` and `from_petl()` currently build:

```python
DbPayload(
    columns=[...],
    rows=[
        {"column_a": value, "column_b": value},
        ...,
    ],
)
```

This shape remains valid for the insert loader. COPY needs a one-shot row
source that does not first become `list[dict[str, Any]]`.

## Decision

Add an explicit PostgreSQL `COPY FROM STDIN` loader alongside the existing
insert loader.

The task author selects the staging loader per database output:

```python
PipelineSpec(
    db_table="events",
    db_loader="copy",
)
```

The default remains:

```python
db_loader="insert"
```

Allowed values are exactly:

```text
insert
copy
```

No `auto` value is introduced.

The framework will not:

- infer the loader from row count, column count, sampled values or estimated
  bytes;
- change loaders after source consumption begins;
- combine insert and COPY within one staging load;
- fall back from COPY to insert after a COPY failure;
- retry a COPY failure through a different transport.

The loader is a deliberate task-design choice. Insert remains the proven
compatibility path. COPY is selected for outputs whose expected row count,
width or serialized volume makes insert binding material.

Both schema modes support both loaders:

```text
inferred schema + insert
inferred schema + copy

declared schema + insert
declared schema + copy
```

`db_output` remains inferred-mode only. `output_schema`,
`db_not_null_columns`, `db_type_overrides`, static `db_contract` and the
framework-owned `etl_updated_at` column keep their 0.5.0 meanings.

The first COPY implementation rejects `get_dynamic_db_contract()`. That hook is
arbitrary task code and may traverse a lazy table while deciding its mapping;
the framework cannot prove that the one-shot source remains unconsumed. Static
`db_contract` is supported. Dynamic projection may be reconsidered only with a
separate replayability contract rather than an undocumented convention.

## Public configuration

`db_loader` is appended after every field that exists in `PipelineSpec` 0.5.0
so old positional construction retains its meaning. New code should continue
to use keyword arguments.

```python
@dataclass(frozen=True)
class PipelineSpec:
    ...
    db_not_null_columns: ... = None
    output_schema: ... = None
    db_loader: str = "insert"
```

Validation occurs at two boundaries:

1. `PipelineSpec` rejects any value other than `"insert"` or `"copy"`;
2. structural pipeline validation rejects COPY combined with
   `get_dynamic_db_contract()` before resources are built;
3. the database payload/publisher boundary repeats loader validation so direct
   callers cannot bypass it.

Normal insert tasks require no source change. COPY tasks state the choice
explicitly so the performance and scratch-disk decision is visible in review.

COPY tuning belongs under `PublisherConfig`, not as loose `run_pipelines()`
arguments:

```python
@dataclass(frozen=True)
class CopyLoadPolicy:
    spool_directory: Path | None = None
    buffer_bytes: int = 1_048_576


@dataclass(frozen=True)
class PublisherConfig:
    ...
    copy_load_policy: CopyLoadPolicy = field(
        default_factory=CopyLoadPolicy
    )
```

Only settings proven useful by implementation or benchmark evidence become
public. Driver cursor details, serialization grammar and SQLSTATE handling
remain implementation details.

## Bounded-memory contract

COPY mode must not construct:

- a list containing every output row;
- a whole-table list of dictionaries;
- a whole-table `StringIO`;
- a complete CSV or COPY string in memory;
- a second full DataFrame solely for database publication.

The COPY subsystem's additional memory must be proportional to:

```text
number of columns
+ bounded schema-resolution state
+ one row
+ bounded I/O buffers
```

It must not grow proportionally with row count.

A source may already be materialized, such as a pandas DataFrame. That source
object is not duplicated by COPY.

The guarantee is specifically about database preparation. Existing optional
consumers can still make the whole pipeline materialized:

- `publish_result=True` retains the pipeline result;
- debug display traverses it;
- Excel export traverses it;
- PETL stabilization for multiple traversals uses `etl.cache()`.

For a lazy PETL output, the end-to-end bounded-memory path therefore exists
when database COPY is the only consumer that requires traversal. When another
consumer is active, COPY must still avoid a second `list[dict]`, but the run may
already be `O(rows)` because the source was intentionally stabilized for that
other consumer. Documentation and logs must not describe such a run as
end-to-end bounded-memory.

## Runner and adapter lifecycle

The runner must know the resolved loader before it decides whether to count or
stabilize the output.

### Database-only COPY path

For a pipeline whose only traversing consumer is PostgreSQL COPY:

```text
pipeline.run()
→ adapter validates the result
→ adapter exposes ordered columns and a one-shot row source
→ COPY preparation consumes the source once
→ COPY preparation returns the exact row count
→ runner records that count in pipeline_rows
```

The runner does not call `adapter.nrows()` first and does not wrap a lazy PETL
source in `etl.cache()` merely to make a second traversal possible.

`DbPublisher.publish()` may return the existing logical `DbTableResult`, or a
small preparation result containing the same exact row count. Existing callers
that ignore the return value remain compatible.

### Multiple-consumer path

When the same output must also be retained, displayed or exported, the current
stabilization rules remain. COPY consumes the stabilized source and avoids an
additional whole-table database payload.

This preserves one consistent output for all consumers. It does not promise
that a lazy source remains globally bounded-memory after the author requested
multiple consumers.

### Row-source contract

COPY receives ordered columns without consuming the data rows and a source that
can be consumed exactly once:

```python
class DbRowSource(Protocol):
    def iter_rows(self) -> Iterator[Sequence[Any]]:
        ...
```

The source yields positional rows rather than dictionaries. Column order is
owned separately and row width is checked exactly.

Pandas uses `itertuples(index=False, name=None)`. PETL consumes its header once
and then yields each remaining row once.

The payload keeps the current insert representation and gains COPY-specific
state without breaking existing positional construction. Conceptually:

```python
@dataclass
class DbPayload:
    table_name: str
    schema: str
    columns: list[str]
    rows: list[dict[str, Any]] | None
    ...                         # every 0.5.0 field, unchanged
    loader: str = "insert"
    row_source: DbRowSource | None = None
```

Valid states are exact:

```text
loader=insert → rows is present, row_source is absent
loader=copy   → rows is absent, row_source is present
```

Any other combination is a configuration error before source execution or
staging DDL.

`db_contract` projection/renaming and `etl_updated_at` application must be
implemented as row-source transformations for COPY. They must not force the
source into dictionaries first.

Declared output order is still controlled solely by `output_schema`. COPY
validates the exact source column set and maps positional values into declared
order before spooling. Missing and unexpected columns fail before database
work begins.

## Module architecture

The implementation is split into four modules:

```text
task_core/
├── db_publish.py
├── db_values.py
├── db_insert.py
└── db_copy.py
```

The split follows ownership and lifecycle, not line count.

### `db_publish.py`

`db_publish.py` remains the sole owner of publication correctness.

It owns:

- `DbPublisher`;
- payload validation and loader dispatch;
- task advisory locking;
- staging-name generation;
- preparation transaction orchestration;
- staging-table DDL;
- staging ownership comments;
- prepared-artifact verification;
- pending-publication registration;
- inferred replacement publication;
- declared first-target creation;
- declared compatibility preflight and stable refill;
- deterministic target locking;
- publication retry and SQLSTATE policy;
- source-state publication;
- rollback and fatal connection-loss state;
- predecessor database-artifact cleanup.

It decides which transport loads the already defined staging table:

```python
if payload.loader == "insert":
    load_result = load_with_insert(...)
elif payload.loader == "copy":
    prepared = prepare_copy_source(...)
    load_result = load_with_copy(...)
else:
    raise DbPublishError(...)
```

It remains responsible for:

1. requiring the task advisory lock;
2. validating payload identifiers and ownership scope;
3. resolving or receiving the complete `ResolvedSchema`;
4. opening the preparation transaction;
5. creating the ordinary logged staging table;
6. invoking the selected transport;
7. verifying the load result and ordered columns;
8. attaching the ownership comment;
9. committing the preparation transaction;
10. registering the pending publication.

Neither loader may commit, roll back, reconnect, publish, rename, truncate,
comment on live targets or manage advisory locks.

The staging and publication protocol remains together. It is not split into
`db_locks.py`, `db_staging.py`, `db_comments.py`, `db_cleanup.py` or similar
helper-category modules because those operations form one invariant-bearing
state machine.

### `db_values.py`

`db_values.py` becomes the shared semantic kernel for insert and COPY.

It owns the behavior currently concentrated in `db_publish.py`:

- `ResolvedColumn` and `ResolvedSchema`;
- missing-value detection;
- scalar normalization;
- type alias and override resolution;
- value-family classification;
- inferred-family accumulation and widening;
- all-null handling;
- declared type resolution;
- declared nullability, range, precision, scale, length and timezone checks;
- framework-column integration;
- shared compatibility rules used before either transport.

It must preserve the 0.5.0 meanings of:

- scalar NaN and `NaT` normalizing to SQL `NULL`;
- containers not being collapsed into scalar `NULL`;
- Python `Decimal` support for declared `NUMERIC`;
- strict aware/naive timestamp matching;
- inferred `db_not_null_columns`;
- declaration-order reordering;
- sampled inference and late-value protection.

The exact function names may change during extraction, but insert behavior is
not changed merely to make COPY easier.

`db_values.py` knows nothing about:

- SQLAlchemy transactions;
- table names;
- advisory locks;
- ownership comments;
- publication mechanisms;
- local spool paths;
- DBAPI cursors.

### `db_insert.py`

`db_insert.py` owns the existing compatibility transport:

- insert chunk construction;
- SQLAlchemy insert execution;
- emitted-row accounting;
- insert-specific errors.

Its initial extraction is mechanical. Chunking, SQL shape, row counts and
failure semantics remain equivalent to 0.5.0.

It does not create or finish transactions, create tables, manage locks, attach
comments or publish targets.

### `db_copy.py`

`db_copy.py` owns the bounded-memory COPY subsystem:

- `CopyLoadPolicy`;
- row-source consumption;
- local spool creation and ownership;
- bounded buffers;
- spool encoding and decoding;
- streaming schema-state accumulation through `db_values`;
- target-aware COPY text serialization;
- DBAPI `COPY FROM STDIN` execution;
- exact source/emitted row accounting;
- spool cleanup and predecessor spool cleanup;
- COPY-specific diagnostics and errors.

It must not import or depend on `DbPublisher`.

## Dependency direction

```text
db_publish ─────→ db_insert ─────→ db_values
      │
      └─────────→ db_copy ───────→ db_values
```

The following directions are prohibited:

```text
db_copy   ─→ DbPublisher
db_insert ─→ DbPublisher
db_values ─→ db_publish
db_values ─→ db_copy
db_values ─→ db_insert
```

Public facade imports remain stable. New public types such as
`CopyLoadPolicy` may be re-exported through `task_core.__init__` and the
existing configuration surface without requiring task authors to know the
physical module layout.

## No generic loader framework

The implementation does not introduce a loader class hierarchy or plugin
registry.

Insert and COPY have intentionally different lifecycles:

```text
insert:
materialized rows
→ resolve schema
→ preparation transaction
→ chunked INSERT

copy:
one-shot rows
→ local spool and schema work
→ preparation transaction
→ COPY
```

Forcing both into identical `prepare/load/cleanup` methods would hide the
important pre-transaction spool phase or add meaningless methods to insert.
Explicit functions and small result objects are preferred until actual third
loader requirements demonstrate a useful abstraction.

A common result is justified only where both transports share meaning:

```python
@dataclass(frozen=True)
class LoadResult:
    rows_emitted: int
```

COPY-only metrics such as spool bytes and serialization duration remain
COPY-specific diagnostics rather than placeholders in insert results.

## Local spool design

A local spool is required even when `output_schema` is declared.

The source must be consumed and all task-core-owned normalization and
validation must complete before the database preparation transaction opens.
Otherwise a slow or failing source would extend the transaction and recreate
the long-transaction problem solved by ADR 0005.

The spool also makes a one-shot source replayable for `COPY FROM STDIN`.

### Type-neutral first spool

The first spool is a private, versioned, length-framed binary format that
losslessly represents the normalized scalar families supported by COPY:

- `NULL`;
- Boolean;
- integer;
- floating point;
- `Decimal`;
- text;
- bytes-like values;
- date;
- naive datetime;
- timezone-aware datetime.

It is not:

- Python pickle;
- CSV;
- a user-facing export;
- PostgreSQL binary COPY format;
- executable object serialization.

A type-neutral spool is necessary because inferred column types are not final
until the source reaches EOF. Writing final PostgreSQL text during the first
pass would make early values depend on a schema that is not yet known and
would risk transport-specific coercion differences.

Unsupported non-scalar values fail COPY preparation before database work. The
insert loader remains available for unusual DBAPI-adapted values outside the
COPY compatibility corpus.

### Schema resolution and final COPY spool

COPY preparation uses two bounded passes before opening the database
transaction:

```text
source
→ normalize each row
→ write type-neutral spool
→ accumulate inferred state or validate declared structure
→ source EOF
→ resolve one ResolvedSchema
→ replay type-neutral spool
→ perform final nullability/type validation
→ serialize PostgreSQL COPY text into final spool
→ delete type-neutral spool
```

Declared mode already knows its schema, but it follows the same lifecycle in
the initial implementation. One shared path is preferred over an early
optimization that gives declared and inferred COPY different cleanup and
failure behavior. A later implementation may collapse declared preparation to
one spool only if equivalence and cleanup guarantees remain unchanged.

The final spool is opened in binary mode but contains PostgreSQL COPY text
records. Binary file mode prevents newline translation; it does not mean
PostgreSQL binary COPY.

Peak scratch-disk use may temporarily approach the sum of the type-neutral and
final serialized spools. That cost is explicit. COPY replaces `O(rows)` Python
memory with `O(serialized rows)` local scratch disk.

### Final serialization

The serializer is target-aware and receives the resolved column type. It must
handle, at minimum:

- SQL `NULL` versus empty string;
- literal text `\\N`;
- tabs, newlines, carriage returns and backslashes;
- Unicode;
- integers and Boolean values;
- finite and supported non-finite floating-point values under existing
  normalization rules;
- exact `Decimal` values;
- dates;
- naive and timezone-aware datetimes;
- bytes and other accepted binary values.

Escaping exists once in `db_copy.py`. Adapters must not implement their own
COPY escaping.

The implementation initially uses PostgreSQL COPY text format. PostgreSQL
binary COPY is deferred because it requires a separate type/OID encoding
matrix and would multiply correctness work before text COPY is proven.

## Inferred-schema COPY

Inferred COPY must produce the same `ResolvedSchema` as insert for the supported
compatibility corpus.

Streaming inference keeps bounded per-column family state rather than rescanning
a materialized row list. It must preserve the current outcomes for:

- all-null columns;
- Boolean;
- integer;
- numeric values;
- date;
- datetime;
- mixed date and datetime;
- text;
- bytes;
- explicit type overrides;
- values appearing after the former 5000-row sample boundary.

The existing protection against silent `BIGINT` rounding and `DATE` truncation
remains mandatory. Streaming accumulation may make the sample-specific
implementation unnecessary, but it must not change the resolved answer.

If heterogeneous inferred values collapse to `TEXT`, COPY may accept them only
when the shared semantic layer can prove a value-preserving textual
representation equivalent to the insert path. Otherwise COPY fails clearly and
the task must pre-convert the column, declare a stable type, or use insert.
It must not invent a new `str(value)` coercion merely to make COPY succeed.

`db_not_null_columns` is enforced while building the final spool. A normalized
missing value fails before the database transaction opens.

## Declared-schema COPY

Declared COPY uses the exact 0.5.0 contract:

- `output_schema` is the sole complete user schema;
- produced and declared column sets must match exactly;
- source order may differ and is mapped into declaration order;
- framework columns are appended after user columns;
- every value is normalized, checked for `NULL`, and validated against the
  declared type;
- Python `Decimal` is accepted for `NUMERIC` only when precision and scale fit
  without rounding;
- naive and aware timestamps must match the declared timezone mode exactly;
- no implicit string parsing or cross-family coercion occurs.

The final COPY spool is not considered ready until every row has passed this
validation. Therefore a declared validation failure creates no staging table,
acquires no live-target lock and leaves the existing stable target untouched.

COPY does not change declared target compatibility, incoming foreign-key
preflight, first-target creation or stable refill behavior.

## Database integration

Both transports use the connection supplied by `DbPublisher`.

Neither may:

- create a second engine;
- create a second database session;
- borrow a pooled connection;
- commit or roll back through the raw driver;
- reconnect after connection loss;
- bypass the explicit SQLAlchemy transaction.

`db_copy.py` accesses the psycopg2 cursor underlying the publisher's existing
SQLAlchemy connection and executes a column-explicit statement:

```sql
COPY "schema"."staging_table" (
    "column_a",
    "column_b"
)
FROM STDIN
WITH (FORMAT text);
```

Identifiers are still restricted to the portable lower-case contract from ADR
0010. SQL generation quotes them defensively. COPY introduces no quoted-name
escape hatch.

Values are transported only through STDIN. They are never interpolated into
SQL.

The loader returns control without committing or rolling back. The publisher
then performs the same staging verification, ownership comment and preparation
commit used by insert.

## Preparation flows

### Insert

```text
1. Require the task advisory lock.
2. Validate payload and identifiers.
3. Resolve inferred or declared schema through db_values.
4. Generate the staging name.
5. Open the preparation transaction.
6. Create the ordinary logged staging table.
7. Insert materialized rows through db_insert in bounded chunks.
8. Verify emitted row count and exact ordered columns.
9. Attach staging ownership metadata.
10. Commit the preparation transaction.
11. Register the pending publication.
```

### COPY

```text
1. Require the task advisory lock.
2. Validate payload, loader and identifiers.
3. Generate the staging name.
4. Consume the source once into the type-neutral local spool.
5. Resolve inferred or declared schema through db_values.
6. Replay, validate and serialize the final COPY text spool.
7. Record exact rows and bytes.
8. Open the preparation transaction.
9. Create the ordinary logged staging table.
10. COPY the final spool through the existing connection.
11. Verify emitted row count and exact ordered columns.
12. Attach staging ownership metadata.
13. Commit the preparation transaction.
14. Register the pending publication.
15. Delete local spools.
```

Steps 4 through 7 happen before the database transaction.

Steps 9 through 13 remain one transaction.

For either loader:

```text
committed ordinary staging table
+ valid ownership comment
= complete publishable artifact
```

No ready flag or registry table is introduced.

## Publication remains loader-independent

A prepared staging table does not record or require its loading transport for
publication correctness.

After preparation commits, the existing 0.5.0 state machine remains:

```text
verify every owned staging artifact
→ preflight declared targets
→ create/fill absent declared targets
→ run source-state publication work
→ lock every existing target in sorted order
→ inferred: DROP/RENAME
→ declared: TRUNCATE/refill/drop staging
→ replace framework provenance comments
→ commit atomically
```

A task may prepare some tables with insert and others with COPY. It may also mix
inferred replacement targets and declared stable targets. The final publication
is still one all-or-nothing transaction.

A retryable `55P03` publication attempt reuses the already committed staging
tables. It does not consume the source again or rebuild COPY spools.

## Spool ownership and cleanup

Spools contain task data and are controlled temporary artifacts.

Requirements:

- one dedicated task-core spool directory;
- directory permissions restricted to the current user where supported;
- exact portable filename grammar;
- filename containing task, target and run digests/tokens;
- an internal header containing a magic value, format version and matching
  ownership metadata;
- no raw user-provided path component in a filename;
- deletion after successful staging preparation commit;
- deletion after source, validation, serialization or COPY failure;
- best-effort deletion during normal rollback;
- predecessor cleanup only while the same task advisory lock is held;
- unknown or malformed files preserved rather than guessed to be ours.

Filename alone is not sufficient positive ownership. Predecessor cleanup must
validate both the exact grammar and the internal header before deletion.

A spool is scratch data, not a recovery artifact. It is not fsynced for durable
recovery. After process or host death, the next run rebuilds it from sources.

Secure erasure is not promised. Tasks handling sensitive data must place the
spool directory on storage governed by the same access controls as their
sources.

## Failure semantics

### Source failure

Examples:

- source iterator raises;
- row width differs from the header;
- one-shot source is consumed twice;
- normalization fails.

Result:

- no database transaction has opened;
- no staging table exists;
- local spools are deleted best-effort;
- the run fails through COPY with no insert fallback.

### Local spool failure

Examples:

- disk full;
- permission failure;
- write or rename failure;
- malformed internal spool during replay.

Result is the same as source failure: no staging DDL and no live-target work.

### Schema or value failure

Examples:

- declared value violates nullability, range, scale, length or timezone mode;
- inferred `db_not_null_columns` encounters `NULL`;
- a COPY value cannot be represented without changing insert semantics;
- explicit type override conflicts with a value.

Result:

- failure occurs before the preparation transaction;
- no staging table is created;
- the live target remains untouched.

### COPY database failure

Examples:

- backend rejects the COPY stream;
- constraint or trigger on staging rejects a row;
- invalid byte sequence reaches PostgreSQL;
- connection fails during COPY.

Result:

- PostgreSQL rolls back staging creation and copied rows together;
- no committed staging artifact remains from that transaction;
- local spools are deleted best-effort;
- connection loss enters the existing terminal state;
- no transparent reconnect or insert fallback occurs.

### Process death after staging commit

The owned staging table remains complete and publishable, exactly as for
insert. The next run removes it under the task advisory lock if publication did
not finish.

A local spool may also remain. The next run removes it only after positive
ownership verification under the same task lock.

### Publication failure

Loader behavior is no longer involved. Existing publication SQLSTATE policy
remains:

| SQLSTATE | Action |
| --- | --- |
| `55P03` | roll back the publication attempt and retry within the horizon |
| `57014` | terminal |
| `40P01` | terminal and logged at ERROR |
| anything else | terminal |

COPY preparation is not repeated during publication retry.

## Validation and accounting

Loader success is not inferred only from the absence of a Python exception.
The implementation verifies:

- every source row produced exactly one normalized spool row;
- every normalized spool row produced exactly one final COPY row;
- every final COPY row was offered to the driver;
- source, final-spool and emitted row counts agree;
- the staging table has the exact ordered column list;
- the staging ownership comment is attached before preparation commit.

The source/spool count is authoritative. A permanent `COUNT(*)` verification
is not added because it would scan the complete table immediately after bulk
loading. Acceptance tests may use `COUNT(*)` to validate the implementation.

Business validation remains task-owned.

`DbTableResult.rows` and `RunResult.pipeline_rows` use the exact source/spool
count, including the database-only COPY path where no preliminary `nrows()`
traversal occurs.

## Logging

Preparation logs identify schema source and loader:

```text
preparing schema.table as table__stg_... schema=inferred loader=insert rows=...
```

```text
copy source complete schema=declared rows=... raw_spool_bytes=...
copy serialization complete rows=... copy_spool_bytes=...
preparing schema.table as table__stg_... schema=declared loader=copy rows=...
copy loaded rows=... elapsed=... rows_per_second=...
```

Logs never contain row values, spool contents or credentials.

Run results continue to report the logical final table name, never staging or
spool paths.

## Implementation sequence

The change is implemented in reviewable phases. Every phase leaves the full
existing suite passing.

### Phase 1 - extract shared value semantics

Create `db_values.py` and move the current normalization, inference,
`ResolvedSchema`, declared validation and type-resolution behavior without
changing observable insert semantics.

No COPY code is added in this phase.

### Phase 2 - extract insert transport

Create `db_insert.py` and move the current chunked SQLAlchemy insert loop out of
`DbPublisher` mechanically.

No loader selection or insert behavior change is introduced.

### Phase 3 - add configuration and row-source representation

Add `PipelineSpec.db_loader` after all 0.5.0 fields, payload validation and the
one-shot row-source adapters.

Insert remains the default and current tasks remain unchanged.

### Phase 4 - make the runner COPY-aware

Remove the mandatory pre-count/cache from the database-only COPY path. Return
the exact prepared row count to the runner. Preserve current stabilization when
another output consumer requires it.

No database COPY execution is integrated until traversal and row-count tests
are complete.

### Phase 5 - implement spool preparation

Create `db_copy.py` and implement:

- policy and directory handling;
- type-neutral spool format;
- ownership header and filename grammar;
- streaming normalization and schema state;
- final target-aware COPY text spool;
- all cleanup paths.

This phase is testable without PostgreSQL.

### Phase 6 - integrate DBAPI COPY

Load the final spool through the publisher's existing SQLAlchemy/psycopg2
connection and preparation transaction. Reuse current staging creation,
verification, comments, commit and pending-publication registration.

### Phase 7 - predecessor spool cleanup

Add positive, task-scoped spool cleanup under the advisory lock. Unknown files
remain untouched.

### Phase 8 - equivalence, failure and performance acceptance

Run the complete unit suite and both live PostgreSQL campaigns before release.

## Tests

### Baseline regression

The accepted 0.5.0 baseline must remain green:

- 450 automated tests and 204 subtests;
- the existing-server PostgreSQL 16.11 declared-schema campaign;
- the constrained PostgreSQL 16.11 VPS concurrency and recovery campaign.

The exact count may grow during implementation; the requirement is no lost
coverage or weakened assertion.

### Module-boundary tests

Required coverage:

- `db_values` imports neither publisher nor loader modules;
- `db_insert` and `db_copy` do not create or finish transactions;
- neither loader creates an engine or second connection;
- neither loader imports `DbPublisher`;
- `db_publish` remains the only owner of comments and pending publication;
- public facade imports remain stable.

### Configuration and lifecycle tests

Required coverage:

- omitted `db_loader` resolves to insert;
- invalid values fail during structural validation;
- old positional `PipelineSpec` construction retains 0.5.0 meanings;
- direct payload callers cannot bypass loader validation;
- COPY plus `get_dynamic_db_contract()` fails during structural validation;
- static `db_contract` remains supported in both schema modes;
- no `auto` value exists;
- no loader selection depends on values discovered after consumption;
- COPY failure never invokes insert;
- insert failure never invokes COPY;
- one-shot COPY sources are consumed exactly once;
- database-only PETL COPY does not call `nrows()` first and does not use
  `etl.cache()`;
- multiple-consumer runs retain current stabilization behavior;
- mixed outputs in one task may select different loaders.

### Value and spool tests

Required coverage:

- insert and COPY resolve equivalent inferred schemas for the supported corpus;
- insert and COPY enforce the same declared schema contract;
- all-null columns;
- late integer-to-numeric and date-to-datetime widening;
- explicit type overrides;
- `db_not_null_columns`;
- framework `etl_updated_at`;
- declaration-order mapping;
- `NULL`, empty string and literal `\\N`;
- tabs, newlines, carriage returns and backslashes;
- Unicode;
- `Decimal` precision and scale;
- Boolean and integer boundaries;
- finite and supported non-finite floats;
- date and both timestamp timezone modes;
- binary values;
- row-width mismatch;
- unsupported non-scalar values;
- source failure midway;
- raw-spool and final-spool write failures;
- malformed or incomplete spool;
- deletion after every success and failure path;
- unknown files are never deleted;
- peak buffer size remains bounded.

### Real PostgreSQL acceptance

The live-server campaign uses the existing permitted schema and uniquely
prefixed objects. It must verify:

1. inferred insert/COPY value and schema equivalence;
2. declared insert/COPY value and schema equivalence;
3. successful COPY of at least one million rows;
4. portable lower-case identifiers with defensive SQL quoting;
5. Unicode, multiline text, `NULL`, empty string and literal `\\N`;
6. decimals, Boolean, date, naive timestamp and aware timestamp values;
7. binary values;
8. atomic first declared publication through COPY;
9. stable declared OID and dependent-object preservation after COPY-loaded
   refresh;
10. inferred COPY replacement publication;
11. mixed insert/COPY and inferred/declared multi-table atomicity;
12. type or value error after many source rows with no committed staging table;
13. source error before the database transaction;
14. connection termination during COPY;
15. committed staging recovery after process interruption;
16. complete positively scoped cleanup.

No quoted-identifier acceptance case exists because ADR 0010 removed that
public mode.

### VPS concurrency and recovery acceptance

The controlled VPS campaign must rerun the publication risks with at least one
COPY-loaded staging table:

- `55P03` lock retry;
- observable reader blocking during declared stable refill;
- stable target OID;
- backend termination during COPY preparation;
- backend termination during declared live refill;
- rollback of live contents;
- successor cleanup of abandoned staging tables and spools;
- final residual-artifact verification.

### Memory acceptance

Measure the same schema at increasing row counts, for example:

```text
100,000
1,000,000
5,000,000
```

Report separately:

- memory already occupied by the source object;
- memory added by task_core;
- raw spool bytes;
- final COPY spool bytes;
- preparation duration;
- database COPY duration.

For a database-only lazy PETL source, peak additional Python memory must remain
approximately flat as row count grows. A whole-table row list, dictionary list,
cache or `StringIO` fails this criterion.

### Performance acceptance

Benchmark insert and COPY under the same conditions:

- identical rows and schema;
- identical PostgreSQL server and network path;
- identical staging and publication mechanism;
- identical logging level;
- same source representation where practical.

Record:

- source/spool preparation time;
- database load time;
- total preparation wall time;
- rows per second;
- peak process memory;
- scratch-disk bytes;
- PostgreSQL WAL bytes where practical;
- client and server CPU.

COPY must demonstrate a material throughput improvement on million-row or
comparably wide workloads before documentation recommends it broadly. One
machine's crossover point does not become an automatic framework threshold.

## Rollout

The feature is released as a minor version.

Initial behavior:

- insert remains the default;
- COPY is opt-in per output;
- both inferred and declared schemas are supported;
- loader selection is manual;
- no automatic fallback or switching exists;
- current publication guarantees remain unchanged.

The first production candidate should be one large, reproducible table whose
insert-loaded result is already known.

Before switching its live configuration:

1. publish the same source through insert and COPY into separate test targets;
2. compare schema, row count and values;
3. measure memory, scratch disk and throughput;
4. induce a COPY failure and a connection loss;
5. confirm staging and spool cleanup;
6. verify the correct inferred replacement or declared stable-target behavior;
7. select `db_loader="copy"` explicitly and rerun.

Rollback is a task configuration change back to `"insert"` followed by a new
run. It is not an automatic branch inside the failed run.

## Rejected

### Keep COPY inside `db_publish.py`

This would make the publisher own publication transactions, local filesystem
state, source streaming, spool encoding, serialization and DBAPI transport.
Those are independently testable lifecycles and would make cleanup reasoning
harder.

### Split the publication protocol by helper category

Locks, comments, staging DDL, publication retry and cleanup are one
invariant-bearing protocol. Putting each in a small module would reduce line
count while increasing cross-module state coupling.

### Keep insert embedded while extracting COPY

Once loader choice is real, leaving insert internals in `DbPublisher` creates
an unnecessary asymmetry. Mechanical insert extraction provides a clear
compatibility baseline.

### Generic loader hierarchy or plugin registry

There are two transports with different preparation lifecycles. A generic
framework is premature and would hide meaningful differences.

### Dynamic contract hook with COPY

`get_dynamic_db_contract()` may execute arbitrary logic and traverse the output.
Allowing it on a one-shot COPY source would make bounded-memory behavior depend
on undocumented task code. The initial COPY contract requires a static
`db_contract`; dynamic projection remains available through insert.

### Automatic loader selection

A generic heuristic would depend on incomplete or late information such as row
count, column count, estimated bytes, source metadata or historical results.
One-shot sources may not expose those values before consumption, and row count
alone is misleading for wide data.

The task author makes the choice explicitly.

### Mid-run switching or automatic fallback

Switching after consumption begins requires replay, mixed transport, unbounded
buffering or a universal spool. Falling back after COPY failure can hide a
serializer, data, disk or connection defect and may produce different value
semantics.

The selected loader owns the complete attempt.

### Whole-table `StringIO` or `DataFrame.to_csv()` string

Both duplicate the complete dataset in memory and violate the primary
constraint.

### Rely on current PETL cache and call it bounded-memory

`etl.cache()` stores all rows. It may still be required for explicitly
requested multiple consumers, but it cannot be the database-only COPY path.

### Direct source-to-database COPY

It would hold the preparation transaction while the source is consumed and
would expose source failure, remote I/O and late schema discovery inside that
transaction. It also cannot replay a one-shot source after schema resolution.

### Final COPY text as the first inferred spool

The final target type is unknown until inferred source consumption reaches EOF.
Serializing early values against an unresolved type risks coercion differences
and loses the original type information needed for strict equivalence checks.

### Python pickle spool

Pickle is executable object serialization and makes spool replacement a code
execution boundary. A restricted private binary grammar is more work but has a
smaller and auditable trust surface.

### PostgreSQL binary COPY in the first implementation

Binary COPY requires an OID- and type-specific encoder and creates a second
large compatibility matrix. Text COPY provides the bulk protocol while keeping
serialization inspectable. Binary format can be considered only after text
COPY is stable and measured.

### All-text staging followed by typed conversion

This adds another full-table database transformation, more WAL, another staging
object and a larger cleanup surface.

### Declared-only COPY

Declared mode is easier because its types are known, but loader choice should
remain independent of schema source. The type-neutral spool allows inferred
COPY without weakening schema semantics.

### UNLOGGED staging

An inferred rename would publish an UNLOGGED live table, and declared loading
would no longer match the durability baseline. Staging remains ordinary and
logged.

### Replace insert immediately

Insert is the proven compatibility and diagnostic path and remains efficient
for small outputs.

## Consequences

- Loader selection becomes an explicit part of task design.
- `PipelineSpec` gains one backward-compatible field.
- Database-only lazy PETL COPY can avoid both the current cache and the
  whole-table list of dictionaries.
- Multiple-consumer PETL pipelines may still materialize because those
  consumers require repeatable traversal; COPY avoids adding another full copy.
- COPY uses scratch disk proportional to the serialized source and may briefly
  hold two spool representations.
- The spool becomes a new positively owned artifact class with its own cleanup
  grammar and acceptance tests.
- `db_publish.py` becomes smaller but remains the sole owner of publication
  invariants.
- Value semantics become independently testable in `db_values.py` and are
  shared by both transports.
- Insert remains the compatibility baseline.
- Preparation remains restartable rather than resumable.
- Publication retry stays cheap because it reuses committed staging tables.
- A future loader can be added without embedding transport internals in the
  publisher, but no plugin system is created in advance.

## Verification status

No COPY implementation exists yet.

The 0.5.0 baseline that this decision must preserve has passed:

- the complete automated suite;
- the existing-server PostgreSQL 16.11 declared-schema acceptance campaign;
- the constrained PostgreSQL 16.11 VPS lock, termination, rollback and cleanup
  campaign.

ADR 0011 is complete as a decision only when the implementation demonstrates:

- behavior-preserving extraction of `db_values.py` and `db_insert.py`;
- no circular or upward dependencies;
- a database-only COPY path without `O(rows)` Python materialization;
- exact inferred and declared schema equivalence for the supported corpus;
- correct COPY text serialization;
- same-connection transaction integration;
- positive spool ownership and predecessor cleanup;
- fatal connection-loss behavior;
- mixed-loader atomic publication;
- real memory and throughput results.

The feature is not complete when COPY merely loads a table. It is complete when
it loads large data materially faster without introducing a new way to publish
incomplete, stale, corrupted or unowned data.
