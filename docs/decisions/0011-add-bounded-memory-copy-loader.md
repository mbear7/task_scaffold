# 0011 — Add a bounded-memory `COPY FROM STDIN` loader

Status: accepted — complete.

`db_loader='copy'` is a public staging transport with bounded encrypted spooling
and predecessor-spool cleanup under the task advisory lock. Formal Phase 8
closure passed on the target localhost PostgreSQL 18.4 instance: 19/19 harness
self-tests, 4/4 repository commands, 13/13 adversarial concurrency/failure
cases, 10/10 memory-scaling checks, 24/24 randomized 1m/10m release
measurements and 9/9 aggregate assertions. PostgreSQL server tuning is
explicitly outside this ADR's closure scope.

Amended before implementation by ADR 0012: schema source and publication
strategy are independent. COPY changes staging transport only and must work
with every legal schema/publication combination.

## Problem

`task_core` 0.5.2 prepares PostgreSQL staging tables through chunked
SQLAlchemy inserts. The path is proven and remains the compatibility baseline,
but it has two costs that become dominant for large or wide outputs:

- every output row is materialized as a Python dictionary before preparation;
- every row is sent through SQL statement binding rather than PostgreSQL's
  bulk-ingestion protocol.

Increasing the insert chunk size may reduce round trips, but it does not remove
the `O(rows)` Python object graph or the per-row binding work.

PostgreSQL provides `COPY FROM STDIN` for bulk ingestion. Adding it is useful
only if it changes the staging transport without weakening the guarantees that
already exist in 0.5.2:

- one session-scoped task advisory lock;
- ordinary logged staging tables;
- one committed preparation transaction per output;
- exact positive ownership of staging artifacts;
- fatal connection-loss behavior with no transparent reconnect;
- atomic multi-table publication and source-state advancement;
- bounded and deterministically ordered live-target locking;
- replacement publication for inferred or declared schemas;
- explicit declared stable-target creation or `TRUNCATE` plus refill;
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

## Current 0.5.2 baseline

The decision is based on the actual 0.5.2 scaffold, not the earlier coupled
schema/publication design.

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

### Two publication strategies

Replacement is the default for both schema sources:

```text
prepare staging
→ lock existing target
→ DROP target
→ RENAME staging
→ commit
```

A declared output may explicitly request stable refill:

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

COPY does not select or alter publication strategy. Schema source, loader and
publication strategy remain separate, subject to the legal matrix from ADR
0012: inferred supports replacement only; declared supports replacement or
explicit refill.

The accepted 0.5.1 external acceptance campaigns verified this separation on
PostgreSQL 16.11. The live-server campaign passed all 9 cases, including
declared replacement, explicit refill, mixed replace/refill rollback, exact
default-metadata rejection and hard multi-target timeout enforcement. The VPS
campaign passed all 10 concurrency and recovery cases, including `55P03`
retry, stable OID, deliberate backend termination, rollback and successor
cleanup.

The VPS campaign also measured the existing cost boundary of stable refill:
50,000 rows committed in 4.433 seconds while a concurrent reader was blocked
for 2.415 seconds. These measurements are release evidence, not universal
performance expectations, but they prove that explicit refill has a
row-proportional live-target critical section.

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

Both schema modes support both loaders. Loader choice does not broaden the
publication matrix:

```text
inferred + insert/copy + replace
declared + insert/copy + replace
declared + insert/copy + explicit refill
```

`db_output` remains inferred-mode only. `output_schema`,
`db_not_null_columns`, `db_type_overrides`, static `db_contract` and the
framework-owned timestamp configured by `db_updated_at` keep their 0.5.2
meanings. `True` uses `etl_updated_at`; a string supplies a custom portable
lower-case name.

The first COPY implementation rejects `get_dynamic_db_contract()`. That hook is
arbitrary task code and may traverse a lazy table while deciding its mapping;
the framework cannot prove that the one-shot source remains unconsumed. Static
`db_contract` is supported. Dynamic projection may be reconsidered only with a
separate replayability contract rather than an undocumented convention.

## Public configuration

In the 0.6 implementation, `db_loader` was appended after every field that
existed in `PipelineSpec` 0.5.2 so positional construction retained its
meaning. ADR 0013 supersedes that constructor-compatibility rule in 0.7.0:
`PipelineSpec` is now keyword-only.

```python
@dataclass(frozen=True)
class PipelineSpec:
    ...
    db_not_null_columns: ... = None
    output_schema: ... = None
    db_publication_strategy: ... = None
    db_loader: str = "insert"
```

Validation occurs at two boundaries:

1. `PipelineSpec` accepts the implemented loaders `'insert'` and `'copy'`
   and rejects every other value;
2. structural pipeline validation will reject COPY combined with
   `get_dynamic_db_contract()` before resources are built;
3. the database payload/publisher boundary repeats loader validation so direct
   callers cannot bypass it.

Normal insert tasks require no source change. COPY tasks will state the choice
explicitly so the performance and scratch-disk decision is visible in review.

COPY tuning belongs under `PublisherConfig`, not as loose `run_pipelines()`
arguments:

```python
@dataclass(frozen=True)
class CopyLoadPolicy:
    spool_directory: Path | None = None
    buffer_bytes: int = 1_048_576
    encrypt_spools: bool = True


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

Tasks may explicitly override only spool protection through the append-only
`PipelineSpec.db_copy_spool_encryption: bool | None` field. `None` inherits the
publisher policy; `False` is a visible per-task opt-out. Connection details,
key material and cipher selection are not task configuration.

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
    ...                         # every 0.5.2 field, unchanged
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

`db_contract` projection/renaming and framework timestamp application must be
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
- replacement publication for either schema source;
- explicit declared first-target creation;
- explicit declared compatibility preflight and stable refill;
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

It must preserve the 0.5.2 meanings of:

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
failure semantics remain equivalent to 0.5.2.

It does not create or finish transactions, create tables, manage locks, attach
comments or publish targets.

### `db_copy.py`

`db_copy.py` owns the bounded-memory COPY subsystem:

- `CopyLoadPolicy`;
- row-source consumption;
- local spool creation and ownership;
- bounded buffers;
- spool encoding and decoding;
- default authenticated spool encryption and bounded decryption;
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

A final local COPY-text spool is required in both schema modes.

The source must be consumed and all task-core-owned normalization and
validation must complete before the database preparation transaction opens.
Otherwise a slow or failing source would extend the transaction and recreate
the long-transaction problem solved by ADR 0005.

In inferred mode, a type-neutral predecessor spool also makes the one-shot
source replayable after the schema becomes known. Declared mode already knows
the target schema and writes the final COPY-text spool directly.

### Type-neutral first spool

The first spool body is a private, versioned, length-framed binary format that
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

COPY preparation has two bounded paths before opening the database
transaction. Inferred mode requires two passes:

```text
source
→ normalize each row
→ write type-neutral spool and accumulate inferred state
→ source EOF
→ resolve one ResolvedSchema
→ replay type-neutral spool through a compiled positional serializer
→ perform final nullability/type validation
→ serialize PostgreSQL COPY text into final spool
→ delete type-neutral spool
```

Declared mode knows the complete schema before traversal and uses one pass:

```text
source
→ validate and normalize each field through a compiled family writer
→ serialize directly into a reused row buffer and the final spool
```

Both paths retain the same final-spool protection, current-run cleanup and
predecessor-cleanup guarantees. The declared optimizations remove the
unnecessary intermediate artifact and the generic per-cell normalization /
validation / serialization chain. Native Python values are handled directly;
pandas, NumPy and other scalar wrappers still use the shared normalization
fallback. No source work moves into the database transaction.

After container decoding, the final spool's logical plaintext body contains
PostgreSQL COPY text records. The container is opened in binary mode to prevent
newline translation; it does not mean PostgreSQL binary COPY.

In inferred mode, peak scratch-disk use may temporarily approach the sum of
the type-neutral and final serialized spools. Declared mode needs only the
final spool. COPY replaces `O(rows)` Python memory with `O(serialized rows)`
local scratch disk.

### Spool protection

Both spool bodies are protected by default with independently generated
AES-256-GCM keys. Key material is never intentionally persisted: it exists on
the in-memory write/preparation handle only for the lifetime of that spool.
The implementation does not claim physical zeroization, exclusion from swap,
or exclusion from process dumps.

The outer ownership header remains plaintext and contains no business row
data. It records the version, task/target/run ownership fields, stage,
protection mode and nonce. The exact header bytes are authenticated as GCM
associated data. The body is ciphertext followed by an authentication tag.
Corruption, truncation or a wrong key fails when the bounded reader reaches
EOF; Phase 6 must keep COPY inside the preparation transaction so such a late
failure rolls back the staging load.

The final spool is never decrypted into another temporary file. Phase 6 feeds
`copy_expert()` from the decrypting file-like reader. After process death, a
successor task_core run has no intentionally persisted key with which to decrypt
the abandoned spool; successor cleanup therefore identifies it from the
plaintext ownership header and deletes it without decryption. This is not a
claim against memory forensics, swap or process dumps.

`PipelineSpec.db_copy_spool_encryption=False` explicitly opts one task out.
Plaintext mode uses the same outer container, permissions, ownership checks,
bounded I/O and cleanup lifecycle. It emits a warning and does not create key
material. Cipher-suite selection is deliberately not public configuration.

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
- naive datetime;
- timezone-aware datetime, resolved to `TIMESTAMPTZ`;
- mixed date and naive datetime, resolved to `TIMESTAMP`;
- text;
- bytes;
- explicit type overrides;
- values appearing after the former 5000-row sample boundary.

The existing protection against silent `BIGINT` rounding and `DATE` truncation
remains mandatory. Timezone awareness is also part of the inferred family:
aware-only columns resolve to `TIMESTAMPTZ`, while aware/naive or aware/date
mixtures fail before database work instead of assuming a session timezone.
Streaming accumulation may make the sample-specific implementation
unnecessary, but it must not change the resolved answer.

If heterogeneous inferred values collapse to `TEXT`, COPY may accept them only
when the shared semantic layer can prove a value-preserving textual
representation equivalent to the insert path. Otherwise COPY fails clearly and
the task must pre-convert the column, declare a stable type, or use insert.
It must not invent a new `str(value)` coercion merely to make COPY succeed.

`db_not_null_columns` is enforced while building the final spool. A normalized
missing value fails before the database transaction opens.

## Declared-schema COPY

Declared COPY uses the exact 0.5.2 schema contract:

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
acquires no live-target lock and leaves the current target untouched.

COPY does not change replacement behavior. For a declared payload explicitly
configured for refill, it also does not change target compatibility, incoming
foreign-key preflight, first-target creation or stable-refill behavior.

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

After preparation commits, the existing 0.5.2 state machine remains:

```text
verify every owned staging artifact
→ preflight only explicit refill targets
→ create/fill absent refill targets
→ run source-state publication work
→ enforce A >= n * L + M for the actual existing lock set
→ lock every existing target in sorted order
→ replace: DROP/RENAME
→ refill: TRUNCATE/refill/drop staging
→ replace framework provenance comments
→ commit atomically
```

A task may prepare some tables with insert and others with COPY, and may mix
replacement targets with explicit declared refill targets. The final
publication is still one all-or-nothing transaction.

A retryable `55P03` publication attempt reuses the already committed staging
tables. It does not consume the source again or rebuild COPY spools.

COPY can reduce source preparation and staging-load time. It cannot shorten the
row-proportional part of explicit stable refill. That publication strategy
still executes `TRUNCATE` plus `INSERT FROM staging` while the live target is
locked, including target index and constraint maintenance. A faster staging
transport therefore does not imply a shorter refill reader-blocking interval.
Replacement and refill publication must be measured separately from loader
performance.

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

### Filesystem failures keep their own type

Native filesystem exceptions are not wrapped merely to normalize them into
task_core exception types. When a filesystem exception propagates directly
from a spool I/O operation, it retains its native type; it may be re-raised as
itself with a better message, but not converted. task_core exception types are
reserved for framework validation, ownership, serialization and spool-format
failures.

The exclusive-open path performs one recovery from a missing directory; if the
retry also fails, the native filesystem exception propagates with contextual
information where useful. `OSError` already carries `errno` and `filename`, so
the path is not lost by declining to wrap.

A higher-level lifecycle policy may deliberately handle a filesystem failure
and raise its own semantic error. **Predecessor cleanup is that case.**
`cleanup_spool_paths()` retries a failing `unlink()` and returns the residual
path rather than propagating; `cleanup_predecessor_spools()` then raises
`DbPublishError` because known task-owned residue remains and continuing would
accumulate another spool beside it. The `DbPublishError` there is not a
restatement of the `EACCES` — it is the refusal to proceed, which is a
task_core decision. Such policies act on a returned residual path rather than
inside an `except` block, which is also how the tripwire in
`tests/test_standalone.py` distinguishes them.

That placement is a deliberate constraint, not an accident of how the check
was written: **a task_core semantic error may not be raised directly from
inside a handler catching a filesystem exception.** Interpretation happens
after the failure has been reduced to ordinary data — a returned residual
path, a count, a flag — not in the `except` block itself. The rule is
stricter than "do not normalize filesystem errors", and it is chosen because
it is mechanically checkable: where a semantic decision may be made is
visible in the AST, whereas whether a raise "restates" or "interprets" an
errno is not. Predecessor cleanup already had this shape; the constraint
records it rather than imposing something new.

The tripwire enforces type preservation exactly: a handler may bare-`raise`,
re-raise its own bound name, or reconstruct the single class it caught. It
may not convert one native type into another — telling a caller `EACCES` when
`ENOENT` occurred is as much a false report as wrapping it — and a tuple
handler may only bare-`raise` or re-raise its bound name, since constructing
one of several caught classes could convert the others.

The rule exists because the alternative is a per-errno contract nobody can
hold in their head. An earlier implementation wrapped the second `ENOENT` of
the recreate-and-open retry, which meant `EACCES` on the first attempt and
`EACCES` on the retry raised different exception types. That distinction was
accidental, and documenting it in four places did not make it a semantic worth
having. `ENOENT` on a spool open is not more meaningful to a caller than
`ENOSPC`; both mean the local spool could not be written and the run stops
before any database work.

A caller that wants one type across every failure should catch `OSError`
alongside `DbPublishError`. The two are not a split by blame — predecessor
cleanup is precisely a case where the filesystem fails first and task_core then
refuses to continue. An `OSError` means the filesystem failure remains the
reported failure; a `DbPublishError` means task_core made a higher-level
semantic decision to stop.

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

Amended after 0.6.0 shipped the first half only. What was originally one phase
is now recorded as two, so the shipped state matches the plan on record. The
original phase text is preserved above the split for the same reason other
`docs/decisions/` files keep advice that turned out to be partial or backwards:
the record of what was planned is what makes the divergence legible later.

Originally:

> Add `PipelineSpec.db_loader` after all 0.5.2 fields, payload validation and
> the one-shot row-source adapters. Insert remains the default and current
> tasks remain unchanged.

Split into:

**Phase 3a - configuration surface.** `PipelineSpec.db_loader`,
`DbPayload.db_loader`, the `LOADERS` dispatch registry with 'insert' as its
sole entry, boundary validation at both spec and payload, and the drift check
that fails at import if `DB_LOADERS` names a value with no registry entry.
Shipped in 0.6.0 (commit `1c55a42`). Insert remains the default and current
tasks remain unchanged.

**Phase 3b - one-shot row-source representation.** `DbRowSource` protocol,
`DbPayload.row_source: DbRowSource | None`, the exact `rows`/`row_source`
state matrix, and the one-shot adapters (`_PetlAdapter.to_row_source`,
`_PandasAdapter.to_row_source`) that expose the source without
materializing. The helpers shipped in 0.6.2 (tightened in 0.6.3) while
`db_loader='copy'` was still rejected at every public boundary. Phase 5
completed the internal row-source → spool-preparation chain. Phase 6 then
integrated `copy_expert()` against the publisher's DBAPI connection, lifted
the public `'copy'` rejection, and completed the end-to-end pipeline path.
Before Phase 6, the internal chain was exercisable from tests only.

Also shipped in 0.6.2: `RowProjection` + `_ProjectedRowSource`, the one
transport-neutral mechanism that composes `db_contract` renaming/projection
and framework columns into the final logical row shape -- required by
§Row-source contract L378-380 which stipulates that both transformations
must be row-source transformations rather than dictionary-first mutations.
The INSERT path is deliberately left untouched (Reading A of the design
review); an INSERT/COPY parity test asserts `_ProjectedRowSource` produces
byte-identical output to the current INSERT path so the two cannot
silently drift.

Sequenced ahead of Phase 4's planning skeleton because that skeleton had
to reason about a database-only COPY path that would eventually hand a
one-shot source handle to a loader, and the protocol had to exist before
the branching could refer to it even in dormant form. The rationale for
splitting 3a out of the original Phase 3 stands: without Phase 5 wiring,
the protocol has no non-test consumer, so 3b did not land in its own
release -- it landed as the setup step of the same commit sequence that
shipped Phase 4's planning skeleton.

Insert remains the default and current tasks remain unchanged.

### Phase 4 - make the runner COPY-aware

Remove the mandatory pre-count/cache from the database-only COPY path. Return
the exact prepared row count to the runner. Preserve current stabilization when
another output consumer requires it.

Consumes the Phase 3b `DbRowSource` handle -- the pre-count removal only
makes sense once the runner has a source object it can hand to the loader
without materializing.

Shipped in 0.6.2 (relabelled honestly in 0.6.3) as the *planning skeleton*
`_plan_pipeline_output_handling` in `task_core.runner`, a helper taking
`db_loader` as a parameter. Before 0.6.6, tests exercised the branch directly
because `db_loader='copy'` was still rejected publicly. On the
database-only COPY path the helper returns `precount_via_nrows=False`;
callers are expected to read the row count from `publisher.table_rows`
after publish rather than call `adapter.nrows()` up front. Stabilization
is still triggered when another consumer (Excel, `debug_display`, or
`publish_result`) needs the table traversed more than once. What the 0.6.2
helper did *not* do -- and the 0.6.2 CHANGELOG and this ADR previously
overstated -- was consume a `DbRowSource` object. It branched on the *string*
`'copy'` in `spec.db_loader`. Phase 5 completed the internal producer-consumer
wiring; Phase 6 made that chain reachable from a real pipeline run and
integrated `copy_expert()`.

No database COPY execution is integrated until traversal and row-count tests
are complete.

### Phase 5 - complete the internal row-source → spool-preparation chain

Create `db_copy.py` and implement:

- policy and directory handling;
- type-neutral spool format;
- ownership header and filename grammar;
- streaming normalization and schema state;
- final target-aware COPY text spool;
- authenticated encrypted spool bodies by default, with explicit task opt-out;
- current-run cleanup paths and positive-ownership cleanup primitives.

Wire the runner into it: `runner.py` calls `adapter.to_row_source()`,
composes the `RowProjection` from `db_contract` + framework columns,
constructs the `_ProjectedRowSource`, and hands it to the Phase 5 spool
code. This is the point at which the Phase 3b helpers get a real
non-test consumer.

This phase is testable without PostgreSQL. `db_loader='copy'` remains
rejected at every public boundary throughout Phase 5 -- the internal
chain is exercised via helper-level tests only, not through
`run_pipelines()`.

### Phase 6 - integrate DBAPI COPY and activate 'copy' publicly

Shipped in 0.6.6. The final prepared spool is streamed through
`copy_expert()` using a cursor opened on the publisher's existing
SQLAlchemy/psycopg2 connection and preparation transaction. Staging creation,
verification, comments, transaction ownership and pending-publication
registration remain in `DbPublisher`.

`validate_db_loader` now accepts `'copy'`, the loader is registered in the
publisher dispatch, and `run_pipelines()` builds a one-shot COPY payload
without calling `adapter.nrows()` on a database-only COPY path. The exact row
count captured during spool preparation is verified against staging and
reported through the existing publisher result properties.

### Phase 7 - predecessor spool cleanup

Shipped in 0.6.7. After `begin_run()` acquires the task advisory lock, the
publisher deletes spools that are positively identified as belonging to an
earlier execution of that task. Filename token/stage and plaintext header
token/stage/task must all agree. The pass removes residue left when a process
crash prevented current-run cleanup, but it does not recover task data or
resume an interrupted publication.

Unknown, malformed and foreign files remain untouched. Failure to remove a
positively owned predecessor after bounded retries is fatal rather than being
silently treated as harmless preserved residue.

### Phase 8 - equivalence, failure and performance acceptance

Run the complete unit suite and both live PostgreSQL campaigns before release.

## Tests

### Baseline regression

The 0.5.2 INSERT baseline must remain green:

- 470 automated tests;
- the accepted 0.5.1 real-server publication-strategy campaign;
- the accepted 0.5.1 VPS explicit-refill concurrency and recovery campaign;
- the 0.5.2 VPS declared-types campaign.

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
- keyword-only `PipelineSpec` construction is enforced by ADR 0013;
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
- the framework timestamp configured by `db_updated_at` (default
  `etl_updated_at` or a custom name);
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
8. atomic first declared replacement publication through COPY;
9. stable declared OID and dependent-object preservation after an explicit
   COPY-loaded refill refresh;
10. inferred COPY replacement publication;
11. mixed insert/COPY, inferred/declared and replace/refill multi-table atomicity;
12. type or value error after many source rows with no committed staging table;
13. source error before the database transaction;
14. connection termination during COPY;
15. committed staging recovery after process interruption;
16. complete positively scoped cleanup.

No quoted-identifier acceptance case exists because ADR 0010 removed that
public mode.

### Target concurrency and recovery acceptance

The target acceptance environment is the current localhost PostgreSQL instance.
No separate VPS is required. The campaign must rerun the publication risks with
at least one COPY-loaded staging table:

- `55P03` lock retry;
- observable reader blocking during explicit declared stable refill;
- stable target OID;
- backend termination during COPY preparation;
- backend termination during explicit declared live refill;
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
- identical logging level;
- same source representation where practical;
- the same publication strategy for each direct loader comparison.

Run replacement and explicit refill as separate publication profiles. Do not
combine loader and publication effects into one undifferentiated result.

Record each phase separately:

- source normalization and type-neutral spooling;
- final COPY serialization;
- staging database load;
- staging preparation commit;
- publication lock acquisition;
- publication critical-section duration;
- total preparation wall time;
- total publication wall time;
- total end-to-end wall time;
- rows per second for preparation and end to end;
- peak process memory;
- raw and final scratch-disk bytes;
- PostgreSQL WAL bytes where practical;
- client and server CPU;
- reader blocking duration for explicit refill.

The loader comparison is INSERT versus COPY with publication held constant.
The publication comparison is replacement versus explicit refill with loader
held constant. This separation is mandatory because COPY may materially
improve staging preparation while leaving refill's locked `TRUNCATE` plus
`INSERT FROM staging` interval unchanged.

COPY must demonstrate a material throughput improvement on million-row or
comparably wide workloads before documentation recommends it broadly. One
machine's crossover point does not become an automatic framework threshold.

PostgreSQL parameter tuning is not an ADR 0011 acceptance requirement. The final
release matrix is run against the target localhost server's current configuration.
Tuning experiments may follow as operational evidence, but they neither change
the loader contract nor block formal ADR closure.

### Formal closure commands

Run the external evidence scripts from one closure-run directory:

```text
python ../../scripts/phase8_closure_harness_selftest.py
python ../../scripts/phase8_repository_verification.py
python ../../scripts/phase8_concurrency_closure.py
python ../../scripts/phase8_memory_scaling.py
python ../../scripts/phase8_performance_release.py
```

The convenience orchestrator runs the same five closure layers in sequence:

```text
python ../../scripts/phase8_adr0011_closure.py
```

Formal closure requires 19/19 closure-harness self-tests, 4/4 repository
commands, 13/13 adversarial concurrency/failure cases, 10/10 default memory
cases, 24/24 release measurements and 9/9 aggregate release assertions. Every
campaign must report zero owned staging or spool residue. The memory series must
show approximately flat additional Python memory at 100k/1m/5m, and the final
randomized 1m/10m matrix must prove encrypted COPY is materially faster and uses
materially less peak RSS than INSERT for replacement and refill.

### Formal closure result

The final target-host campaign completed all gates with zero owned staging or
spool residue. Median end-to-end results from three randomized repeats per
combination were:

| Rows | Publication | INSERT | Encrypted COPY |
| ---: | --- | ---: | ---: |
| 1m | `replace` | 33.38 s | 9.87 s |
| 1m | `refill` | 39.63 s | 20.35 s |
| 10m | `replace` | 337.39 s | 106.12 s |
| 10m | `refill` | 542.37 s | 315.44 s |

At 10m rows, median peak RSS was about 3.46 GiB for INSERT and 132 MiB
for encrypted COPY. The lazy PostgreSQL-backed source campaign measured
task_core-added peak RSS of about 4.7 MiB at 100k rows, 9.3 MiB at 1m and
34.3 MiB at 5m while spool bytes scaled with row count. This proves bounded,
sublinear framework memory rather than a literal zero-growth guarantee.

The refill results also isolate the remaining cost boundary. COPY materially
reduces transport into staging, but it cannot reduce the target-side
`TRUNCATE` plus `INSERT FROM staging` rewrite. At 10m rows, INSERT and COPY
refill publication each took about 203 seconds after staging was ready.

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
6. verify the configured replacement or explicit declared-refill behavior;
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

- Loader selection remains an explicit part of task design and independent of schema source and publication strategy.
- `PipelineSpec` gained one field, appended so positional construction kept
  its meaning at the time. ADR 0013 superseded that rule in 0.7.0 and
  `PipelineSpec` is now keyword-only.
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
- COPY can improve staging preparation but cannot remove explicit refill's
  row-proportional locked write or its reader-blocking cost.
- A future loader can be added without embedding transport internals in the
  publisher, but no plugin system is created in advance.

## Verification status

The accepted 0.5.1 PostgreSQL evidence remains applicable to publication
mechanics because 0.5.2 does not change strategy selection, locking, refill,
rollback or cleanup:

- the PostgreSQL 16.11 live-server publication-strategy campaign passed 9/9;
- the PostgreSQL 16.11 VPS concurrency and recovery campaign passed 10/10;
- both campaigns completed with no owned staging artifacts left behind.

The local `task_core` 0.5.2 candidate passes 470 automated tests. It tightens
the INSERT-path declared type contract so SQLAlchemy type parameters cannot be
silently erased during PostgreSQL rendering, and normalizes PostgreSQL-invalid
NUL text into contextual framework rejection.

That baseline was gated on the external 0.5.2 VPS declared-types campaign
passing its full round-trip, replacement, refill, catalog and rejection
matrix, with ambiguous type shapes and NUL text required to fail as
`DbPublishError` before any live target is created or changed. No evidence
from that campaign survives, and none is reconstructible. Rather than treat
a five-release-old gate as outstanding forever, the equivalent matrix was
rerun against the current implementation; see the 0.7.2 entry below. The
accepted live evidence carried forward is therefore the 0.5.1 campaigns
above, the 0.6.10 closure campaign below, and that rerun.

Phases 1 and 2 shipped in 0.6.0 (commit `1c55a42`), together with
Phase 3a. 0.6.2 shipped the Phase 3b row-source/projection helpers and the
Phase 4 traversal-planning skeleton; 0.6.3 corrected overstatements about
that skeleton and tightened one-shot and projection invariants.

0.6.3 corrected the 0.6.2 language that described the
runner as a "real consumer" of `DbRowSource` — it consumes the *string*
`'copy'`, not a `DbRowSource` object — and tightened four correctness
gaps found by external review: `_PetlRawRowSource` walking the
header-advanced iterator instead of the table (so a `db_resource`-backed
lazy chain is not re-executed on iteration), one-shot enforcement on
both raw sources and on `_ProjectedRowSource`, duplicate-column and
framework-collision checks in `RowProjection.build()`, and a
`RowProjection` invariant that a constant may not coincide with a
source-backed position.

`RowProjection` + `_ProjectedRowSource` in `db_publish.py` is the one
transport-neutral mechanism that composes `db_contract` renaming/projection
and framework columns into the final logical row shape (see ADR §Row-source
contract L378-380 which required this to be done as a row-source
transformation rather than a dictionary-first mutation). A parity test
asserts `_ProjectedRowSource` produces byte-identical output to the current
INSERT path (which stays untouched), so the two cannot silently drift;
verified by revert-observe-restore.

0.6.4 shipped the internal Phase 5 spool-preparation chain. 0.6.5 restored
full INSERT/COPY preparation parity, added encrypted spool bodies by default
with explicit per-task opt-out, and hardened current-run and
positive-ownership cleanup primitives.

0.6.6 ships Phase 6: `db_loader='copy'` is public, `run_pipelines()` builds the
one-shot payload, `DbPublisher` prepares the spool before its staging
transaction, and `db_copy.load_copy_into_staging()` streams the authenticated
COPY-text body through psycopg2 `copy_expert()` on the existing connection.
The publisher removes the final spool before successful preparation commit and
retains ownership of verification, comments, rollback and publication.
Unit tests prove exact SQL construction, bounded authenticated reader use,
connection-loss invalidation, current-run cleanup and no runner pre-count.
Live PostgreSQL acceptance is still Phase 8 evidence, not claimed by 0.6.6.

0.6.7 ships Phase 7: `begin_run()` performs positive, task-scoped predecessor
spool cleanup only after acquiring the advisory lock. This removes residue left
when a process crash prevented current-run cleanup; it does not recover task
data or resume work. Unknown, malformed and foreign files remain untouched,
and known-owned residue that cannot be deleted is fatal. COPY SQL also declares
`ENCODING 'UTF8'`, matching the serializer's unconditional UTF-8 bytes. Live
PostgreSQL acceptance remains Phase 8 evidence and is not claimed by 0.6.7.

0.6.8 corrected the first live Phase 8 findings on PostgreSQL 18.4. The
first live acceptance run failed: inferred timezone-aware and naive
datetimes were classified as one family and both resolved to `timestamp
without time zone`, so INSERT applied session-timezone semantics while COPY
discarded the offset for the same target type. 0.6.8 resolves aware-only
columns to `TIMESTAMPTZ`, rejects columns mixing aware datetimes with naive
datetimes or bare dates before any database work, and extends sample
verification to datetime awareness so a post-sample mismatch triggers
re-inference rather than a silent naive timestamp. The failed first run is
preserved as evidence rather than discarded.

0.6.9 and 0.6.10 optimized COPY preparation before closure. Declared mode
stopped creating the neutral predecessor spool and writes the final
COPY-text spool in one pass; both modes compile the row serializer once per
payload; and 0.6.10 added one compiled direct field writer per declared
column, so ordinary native Python values no longer traverse the generic
normalization, validation and serialization chain. Non-native scalar
wrappers still fall back to the shared kernel. Publication, spool
protection and bounded-memory behavior are unchanged, and Phase 8 was rerun
against the optimized implementation.

0.6.10 completed Phase 8 on the target localhost PostgreSQL 18.4 instance.
The complete repository, adversarial concurrency/failure, lazy-source memory
and randomized 1m/10m release campaigns all passed. ADR 0011 is therefore
closed. Version 0.6.11 was a post-closure cleanup hardening change: after owned
spool files were removed, task_core best-effort removed its empty implicit
default spool root with `rmdir()`. That was reversed two releases later; see
the final entry below.

ADR 0011 required the implementation to demonstrate:

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

The 0.6.10 closure campaign demonstrated all ten; §Formal closure result
carries the measurements.

0.7.2 reran live acceptance on the same target localhost PostgreSQL 18.4
instance, covering the two things closure did not. First, the §Final
serialization corpus now round-trips through a real COPY rather than only
through a byte-comparison between two code paths: 23 columns across two rows,
zero failures, including a literal `\N` stored as text rather than SQL NULL,
tabs, newlines, carriage returns, empty string, Unicode, non-finite floats,
bytes and both naive and aware datetimes. Second, the declared-types matrix
that the lost 0.5.2 campaign was meant to cover passed 9/9 for both loaders:
declared round-trip, exact catalog types, stable refill preserving the target
OID and its attached indexes, NUL text rejected as `DbPublishError` with the
live target neither created nor changed, and no staging residue.

0.7.2 also exercised the post-closure spool-root cleanup, which shipped after
the 13-case concurrency campaign and had been covered only by unit tests. Eight
processes sharing the framework-owned default root completed 240 concurrent
COPY cycles with no failures, no residue and the empty root removed. A separate
deterministic probe established the shape of the documented recreate-and-retry:
it is single-shot. One removal between directory resolution and the exclusive
open is survived; a second, landing between the recreate and the retried open,
escapes as `FileNotFoundError`. That matches what the code comment claims
("Recreate the directory once"), fails loudly before any database work, and
cannot corrupt or partially publish data. Whether to make the retry bounded
rather than single-shot is open.

0.7.3 removed the 0.6.11 root removal. Because `resolve_spool_directory()` and
the exclusive spool open are two separate operations, a root that task_core
itself deletes is a root one task can remove after another has resolved it —
a race the framework created against itself. Removing that deletion eliminates
the framework-controlled source of the race; deletion by something outside
task_core remains possible and is what the retained retry is for.
`open_spool_for_write()` carried a
single recreate-and-retry for exactly this, and it measurably could not close
it: one removal between resolve and open is survived, while a second, landing
between the recreate and the retried open, escaped as a bare
`FileNotFoundError`.

Widening the retry into a loop was rejected. It would have made the framework
more tolerant of a hazard the framework alone produced, and no loop bound is
correct against an adversary that deletes continuously. Removing the root
removal eliminates the race at its source and needs no retry budget to reason
about. The cost is one empty directory under the platform temporary directory,
which is not residue — ADR §Spool ownership and cleanup scopes ownership to
spool *files* — and which may persist until external
temporary-directory maintenance removes it.

The single recreate-and-retry is retained, now serving only its honest case:
deletion by something outside task_core, such as a temporary-directory reaper
or an operator. If it also fails, the native filesystem exception propagates;
see §Filesystem failures keep their own type.

`cleanup_default_spool_directory()` is removed rather than deprecated.

Measured on the target host with the same instrumented eight-process harness
in both configurations. With root removal, a peer deleted the root out from
under a resolving task 7 times and the recreate-and-retry fired 6 times across
48 cycles. Without it, three runs totalling 144 cycles recorded zero of each,
with the root retained and no owned spool files left behind. The race is not
merely survived more often; it no longer arises.

The feature is not complete when COPY merely loads a table. It is complete when
it loads large data materially faster without introducing a new way to publish
incomplete, stale, corrupted or unowned data.
