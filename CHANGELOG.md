# Changelog

Notable changes to `task_core`. Versions are the value of
`task_core.__version__`.

The format is loosely [Keep a Changelog](https://keepachangelog.com/).
This file starts at 0.2.11; earlier history is reconstructed below in a
single entry from the previous README, which recorded changes
chronologically rather than by release.


## 0.6.1

Follow-up patch closing four gaps external review found against 0.6.0. No
change any `db_loader='insert'` caller (all callers today) can observe: the
public API is byte-identical, only an internal validation moves earlier and
tests are added.

### Fixed
- `db_loader` revalidation now runs before staging DDL, not after. 0.6.0
  placed the payload-boundary revalidation immediately before the `LOADERS`
  dispatch, which meant `CREATE TABLE` for the staging table and the
  publisher's transaction had already opened before an invalid loader could
  be rejected. ADR 0011 §Preparation flows step 2 and §Failure semantics
  both require a configuration error to fail before any database work; the
  fix moves the check up to sit alongside `validate_publication_strategy`.

### Changed
- ADR 0011 §Implementation sequence Phase 3 amended to record the 3a/3b
  split. 0.6.0 shipped Phase 3a (the `db_loader` configuration surface);
  Phase 3b (the one-shot `DbRowSource` protocol and adapter rewrites) is
  deferred to immediately before Phase 5 begins, since COPY is its only
  consumer. The original Phase 3 text is preserved in the amendment.

### Tests
- **`test_mutated_payload_loader_is_revalidated_at_publish_boundary`**
  strengthened to assert that no `CREATE TABLE` statement runs and no
  transaction opens when the payload boundary rejects the loader --
  earlier the test satisfied itself with the exception type and message
  and would have passed either way. Verified by revert-observe-restore.
- **`Test7DbInsertBoundary`** in `tests/test_docs.py` -- three AST-based
  structural checks required by ADR 0011 §Tests: `db_insert` does not
  import `DbPublisher`, never begins/commits/rolls back transactions, and
  never creates an engine or opens a second connection. Verified by
  temporary sentinel that made the transaction check fail as expected.
- **`Test4OldPositionalPipelineSpecConstructionKeepsItsMeaning`** in
  `tests/test_types.py` -- the 0.5.2 positional call sequence still binds
  field for field. Pins the API-hygiene rule that every new field must be
  appended, which the source comments at `types.py:208`, `:213` and `:221`
  claim but nothing else enforces.


## 0.6.0

Adds the `db_loader` configuration surface described by ADR 0011 without
adding the COPY loader itself. Minor rather than patch: `PipelineSpec` and
`DbPayload` grow a new field, `from_petl()`/`from_pandas()` grow a new keyword
argument, and both `db_publish` and `db_insert` become independently
importable modules following the split described under Changed. Keyword
construction of `PipelineSpec(...)` and `DbPayload(...)` needs no source
change; only callers that reach for the now-moved private symbols do.

### Added
- **`PipelineSpec.db_loader`** -- `'insert'` (the default and only
  implemented value). `'copy'` is rejected by name at construction with a
  message pointing at ADR 0011. Any other value is rejected generically.
  Same rule enforced at the `DbPayload` boundary and at every
  `from_petl()`/`from_pandas()` adapter constructor so direct callers cannot
  bypass validation.
- `task_core.types.DB_LOADERS` and `validate_db_loader()`. Mirrors the
  `PUBLICATION_STRATEGIES`/`validate_publication_strategy()` pair so the
  spec path and the direct-payload path cannot drift apart.
- Loader dispatch table `LOADERS` in `db_publish.py`, keyed on the payload's
  `db_loader`. An import-time drift check turns any future addition to
  `DB_LOADERS` without a matching registry entry into a `RuntimeError` at
  import, rather than a `KeyError` at first publish.

### Changed
- Extracted the stateless schema/value kernel out of `db_publish.py` into
  `db_values.py` -- exception classes, dataclasses, value predicates,
  declared and inferred schema resolvers. `db_publish.py` re-exports every
  moved name, so import spellings are unchanged. Dependency direction is
  one way (`db_publish` -> `db_values`).
- Extracted the staging-table INSERT loader out of `db_publish.py` into
  `db_insert.py` -- one function, `load_rows_into_staging(conn,
  staging_table, rows, chunk_size)`, plus its `_chunked` helper. The
  publisher still owns the transaction, the table creation, the
  post-load integrity check, and the ownership comment. Any test that
  monkeypatched `task_core.db_publish._chunked` now needs to patch
  `task_core.db_insert._chunked`; the old attribute no longer exists.

### Notes
- `'copy'` is *absent from* `DB_LOADERS` rather than reserved-and-rejected
  inside it -- an accepted vocabulary value that raises `NotImplementedError`
  is its own small lie. It joins the tuple when the loader lands. `'copy'`
  is nonetheless called out by name in the validator so a task author who
  tries it gets the accurate reason rather than the generic one.
- See `docs/decisions/0011` (amended before implementation by `0012`).


## 0.5.2

### Fixed
- Rejected declared SQLAlchemy type shapes that PostgreSQL rendering would
  silently change or reject late: `Float` precision must be integer `1..53`,
  `String` length must be a positive integer, and `Numeric` precision must be
  integer `1..1000` with the supported subset `0 <= scale <= precision`.
- Rejected `Numeric(scale=...)` without precision instead of validating an
  apparent scale that PostgreSQL would not store.
- Rejected bounded `LargeBinary` and text collation because the current
  PostgreSQL declared/refill contract does not preserve or compare those
  parameters.
- Rejected NUL text during declared row validation with a contextual
  `DbPublishError` instead of leaking a lower-level driver `ValueError`.

### Migration
- Review external scripts containing `output_schema=`. Valid declarations need
  no change; see `docs/migrating-to-0.5.2.md` for the rejected ambiguous shapes.
  The bundled HR and OPS tasks use inferred schemas and require no edits.


## 0.5.1

### Added
- **`PipelineSpec.db_publication_strategy`** — `'replace'` (the default
  for both schema sources) or `'refill'`. Publication strategy and schema
  source are now independent: one says where the table's shape comes
  from, the other how new data replaces old.

### Changed
- **Declaring `output_schema` no longer forces refill.** It selected both
  at once, which made the fastest combination — declared + replace —
  unreachable: one database write instead of two, and a catalog-time lock
  instead of one proportional to row count. It also meant a
  reader-blocking window scaled to row count was imposed by a schema
  preference that has nothing to do with reader impact, and that an
  edited declaration could not take effect without manual migration.

  A declared output now publishes by replace unless it asks for refill.
  **Views fail loudly** — `DROP` errors on dependents — but grants,
  ownership and triggers are lost silently, so any declared target with
  dependencies attached needs `db_publication_strategy='refill'`.
  Nothing in this repository used `output_schema`.

- `refill` requires `output_schema` and is rejected at spec construction
  without it. Refill truncates and inserts into the existing table, so the
  target's physical schema must be stable across runs; an inferred schema
  changes whenever the data does. Rejecting the combination keeps it
  unrepresentable rather than turning it into a job that works until a
  column widens.

### Fixed
- Preserved the complete 0.5.0 positional meaning of `PipelineSpec` by
  appending `db_publication_strategy` after `output_schema`. Keyword arguments
  remain the recommended form.
- Applied publication-strategy validation to direct `DbPayload`, `from_petl()`
  and `from_pandas()` callers, and revalidated mutable payloads at `publish()`.
  Unknown strategies no longer fall through to replacement, and inferred
  refill is rejected at every boundary.
- Enforced `acquisition_timeout_ms >= n * lock_timeout_ms + margin_ms` for the
  actual existing target set before `LOCK TABLE`. A multi-target policy can no
  longer turn ordinary contention into terminal `57014` merely because
  cumulative waits exhaust the aggregate statement timeout.
- Included column-default presence (`pg_attribute.atthasdef`) in exact refill
  compatibility. Defaults are not declared in this release, so a target
  default is rejected instead of silently ignored.
- Updated README, architecture, task-authoring guidance and ADR 0011 to treat
  schema source, loader and publication strategy as separate concerns.

### Migration
- Review every running script containing `output_schema=`. Add
  `db_publication_strategy='refill'` only where the ordinary table object and
  attached views, grants, indexes, ownership, triggers or RLS must survive.
  See `docs/migrating-to-0.5.1.md`.

### Notes
- `partition` is deliberately absent from the strategy vocabulary rather
  than reserved and rejected. It is added when it is built.
- See `docs/decisions/0012`, which amends `0009`.


## 0.5.0

Adds a second database schema contract while preserving inferred publication
as the default. See ADR 0009.

### Added

- `OutputColumn` and `PipelineSpec.output_schema` for a complete ordered
  PostgreSQL output contract. Supplying `output_schema` disables inference;
  missing or unexpected columns, incompatible values and non-null violations
  fail during staging preparation.
- `PipelineSpec.db_not_null_columns` for selected inferred columns. Inferred
  types and column discovery remain unchanged; only listed columns gain a real
  staging `NOT NULL` constraint.
- Strict declared validation for Boolean, integer, floating-point,
  `NUMERIC`/`Decimal`, text, binary, date and timezone-aware or naive timestamp
  families. No implicit string parsing, float-to-`NUMERIC`, datetime-to-date or
  timezone assumption is performed.

### Changed

- Declared outputs publish to a stable ordinary table. First publication
  creates and fills the target atomically; later publications verify exact
  PostgreSQL catalog compatibility and use transactional `TRUNCATE` plus
  `INSERT FROM` staging. The target OID, views, indexes, grants, ownership and
  triggers remain attached to the same object.
- Existing inferred outputs retain the staged `DROP`/`RENAME` replacement path.
- `db_output` remains inferred-mode only and is mutually exclusive with
  `output_schema`. Declared output order comes solely from `output_schema`;
  produced columns may arrive in another order and are reordered after exact
  set validation.
- `db_updated_at=True` continues to append `etl_updated_at`, now explicitly
  represented as framework-owned `TIMESTAMPTZ NOT NULL` in both schema modes.

### Safety

- Declared stable targets reject views, materialized views, foreign tables,
  partitioned tables, incompatible physical schemas and external incoming
  foreign keys before live-target locking. `TRUNCATE CASCADE` and automatic
  schema migration are not used.
- All first-target creation, schema/dependency checks and source-state work
  complete before the first existing live-target lock. The locked declared
  critical section contains only `TRUNCATE`, refill, provenance comment,
  staging drop and commit.


## 0.4.1

A narrowly scoped publication-correctness patch. No COPY, declared-schema,
`TRUNCATE`, partition-swap or unrelated refactoring work is included.

### Fixed

- Added one contained exact relation lookup over `pg_class` and
  `pg_namespace`, returning OID and `relkind`. It is used only by prepared
  artifact verification and live-target locking. Views, materialized views,
  foreign tables, partitioned tables and other non-ordinary relations are
  rejected explicitly instead of being treated as missing targets.
- Preserved the minimum timeout-ordering invariant
  `acquisition_timeout_ms >= lock_timeout_ms + 50 ms`, so ordinary lock
  contention can surface as retryable `55P03` before terminal `57014`. The
  50 ms is only an ordering margin; sizing remains `k × L + M ≤ A`, with
  total reader blocking constrained by `A + P ≤ B`.
- Completed all source-state and preparatory database work before the first
  live-target lock. Existing targets are deduplicated and locked in one
  deterministic `(schema, table)` order immediately before `DROP`/`RENAME`;
  after the first lock, only swap, required comments and commit remain.
- Retry jitter is derived from the remaining absolute monotonic horizon while
  preserving the configured minimum sleep and the budget for a useful next
  acquisition attempt. A long random draw can no longer discard usable retry
  time.

### Changed

- Removed `db_identifier_mode` and arbitrary quoted-identifier support without
  a compatibility shim. Schemas, tables and published columns must satisfy the
  existing portable lower-case contract `^[a-z_][a-z0-9_]*$`. Generated SQL
  continues to quote identifiers defensively. See ADR 0010.

## 0.4.0

One deliberate public API break, executing the consolidation recorded in
`docs/decisions/0005`.

### Added
- **`PublicationLockPolicy`** — bounds the `ACCESS EXCLUSIVE` wait during
  publication. All targets are locked in one sorted statement under
  `lock_timeout` and `statement_timeout`; the whole publication is retried
  on `55P03` within a wall-clock horizon that gates *completion*, so
  per-attempt budgets are ceilings and a final attempt may run with less.

  Without this, PostgreSQL's queueing means a publisher waiting on one
  long reader blocks every reader arriving afterwards — and the previous
  per-table acquisition compounded it by holding locks on already-swapped
  tables while queuing for the next. See `docs/decisions/0008`.

  `57014` is terminal, including operator cancellation: it is not uniquely
  `statement_timeout`, and the scaffold should not argue with a human who
  stopped it. `40P01` is terminal and logged at ERROR.

- **`PublisherConfig`** — `publisher_factory`, `identifier_policy` and
  `publication_lock_policy` in one frozen object.

### Changed
- **`run_pipelines()` takes `publisher_config` instead of
  `publisher_factory` and `db_max_identifier_bytes`.** Both loose
  parameters are removed with no compatibility alias. Per-task facts —
  `creds`, `pg_schema` — stay direct arguments.

  ```python
  run_pipelines(..., publisher_config=PublisherConfig(
      publication_lock_policy=PublicationLockPolicy(retry_horizon_seconds=300),
  ))
  ```

  Nothing changes for a task that does not need to tune publication.

### Removed
- The rationale claiming the source-state write is ordered before the
  swaps to keep it out of the exclusive window. With pre-locking the
  window opens at `LOCK`, so the claim was false and has been deleted
  rather than left standing.


### Fixed during review, before release
These correct 0.4.0 itself rather than following it; the package never
shipped with them outstanding.

- `PublisherConfig` rejects a `None` policy or a non-callable factory, and
  `PublicationLockPolicy` rejects NaN and infinity — both previously
  restored the independent defaulting these objects exist to remove.

- **`commit()` could leave the publication transaction open.** It caught
  only `DBAPIError`, so any other failure — an exhausted horizon, an
  invariant violation, `KeyboardInterrupt` — returned with the transaction
  still open, after `_publish_once()` had opened it, verified the staging
  artifacts and possibly run the source-state plan. Every unsuccessful
  attempt now rolls back before the failure is classified, and only DBAPI
  errors are considered for retry.
- The `40P01` message no longer blames the target locks. The publication
  plan runs before locking, so a deadlock is reachable with zero lock
  attempts, and naming the locks would misdirect the investigation.
- The timeout-margin check rejected a difference of exactly the margin
  while its message said "by at least" it. Exactly the margin is now
  accepted.

### Documentation
- `docs/decisions/0008` records verification status. The bounded-wait
  behaviour is confirmed against PostgreSQL 16.14: reader C blocked behind
  the queued publisher as expected, then resumed in 2.0s **while reader A
  was still open** — so its delay was bounded by the publisher's budget
  rather than by the long reader's lifetime, which is the property the
  decision exists to produce. The same run confirmed quoted-target
  locking, `55P03` rather than `57014` on contention, source-state work
  preceding the lock, and clean rollback on horizon exhaustion.

  The multi-table and deadlock paths are confirmed too. Two contended
  targets produced one sorted `LOCK` carrying both, and the ADR now
  records what the run showed: a single statement **bounds** the
  compounding rather than removing it — PostgreSQL acquires in sequence,
  and a reader arriving on an already-acquired target does block until
  rollback.

  It also records the consequence that surfaced: multi-table contention
  tends to exhaust the aggregate `statement_timeout`, which raises the
  terminal `57014` rather than the retryable `55P03`, so such publications
  are likelier to fail cleanly than to retry.

  ADR 0008 now states the sizing rule as `A ≥ k·L + M` subject to
  `A + P ≤ B`, distinguishing `n` existing targets from the `k ≤ n`
  expected to contend, and `A` — the acquisition budget — from `B`, the
  total reader blocking. Acquired locks are held through the swap and
  commit (`P`), so `A` bounds only acquisition waiting; describing it as
  the total ceiling was wrong by exactly the critical section.
  `lock_timeout_ms` is the per-conflict wait limit.
  `acquisition_timeout_ms` is the aggregate budget for acquiring the
  complete sorted target set. Total reader blocking also includes the
  post-acquisition `DROP`, `RENAME`, comment and commit critical section.
  For `k` contended targets, size `A` as about
  `k × lock_timeout_ms` plus margin, while preserving `A + P ≤ B`.
  Legitimate responses to a budget conflict are to lower `L`, publish fewer
  targets together, accept terminal `57014`, or raise `A` only together
  with a correspondingly larger accepted total reader-blocking budget `B`.
  Lowering `L` is not automatically right either: a short per-conflict
  limit can make the retry loop the normal publication mechanism.

  `_lock_publication_targets()` warns when `A < n·L + M` — *worst case*
  deliberately, since the publisher knows `n` and cannot know `k`; and
  terminal `57014` *may* follow rather than must, since an individual wait
  can still reach `L` first. When `(A − M) // n` is zero the warning says
  no positive `lock_timeout_ms` fits rather than recommending 1 ms.
  Emitted once per run. The requirement includes the margin, so the
  defaults cover nine sequentially contended targets rather than ten.

  `40P01` was provoked from the publication plan with zero lock attempts,
  confirming the phase-neutral diagnostic: the older wording would have
  blamed a phase that had not yet run.
- ADR 0008's real-server criterion was wrong: it asked that a third reader
  never block, which contradicts the queueing behaviour the ADR itself
  describes. The correct observation is that the third reader's delay is
  *bounded* by the publisher's remaining budget.
- ADR 0008 no longer claims locks are acquired atomically; PostgreSQL
  takes them in sequence, and the all-or-none outcome is application-level,
  via rollback.
- `README.md` no longer says one publication transaction spans the run.
- `run_pipelines()`'s docstring no longer documents the removed
  `publisher_factory` argument.



- `docs/decisions/0006` records verification status. Its three rules were
  argued from PostgreSQL semantics; they have now been checked against a
  real server, including the connection-loss path that a shared instance
  could not safely exercise. Terminating the lock-owning backend releases
  the lock, the surviving publisher refuses to reconnect, and a successor
  removes the orphan under its own lock — the interlock this decision
  describes, observed in that order rather than inferred.

  What remains unconfirmed is recorded alongside it: one server version,
  and no coverage of a backend killed by the OS or of a network partition
  that leaves the server believing the session is alive.

### Migration
- `run_pipelines(publisher_factory=X)` becomes
  `run_pipelines(publisher_config=PublisherConfig(publisher_factory=X))`.
- `run_pipelines(db_max_identifier_bytes=N)` becomes
  `publisher_config=PublisherConfig(identifier_policy=IdentifierPolicy(max_identifier_bytes=N))`.
- Custom publishers gain a `publication_lock_policy` constructor keyword.
- All fail loudly.


## 0.3.9

### Fixed
- **PostgreSQL publication failed while attaching table comments.**
  `_set_comment()` embedded compact JSON inside `sa.text()`. SQLAlchemy scans
  `TextClause` contents for bind parameters even inside SQL string literals,
  so numeric JSON such as `"v":1` was interpreted as a missing bind parameter
  named `1`. Every real PostgreSQL publication therefore failed after loading
  and validating its first staging table, before that preparation transaction
  could commit.

  Table comments now use SQLAlchemy's PostgreSQL-aware
  `sa.schema.SetTableComment` DDL construct. The dialect renders the comment as
  a literal without parsing JSON colons as bind markers. This fixes both
  staging ownership comments and live-table provenance comments.

- Added a regression test that compiles a published comment containing numeric
  fields (`"v":1` and `"rows":7`) against the PostgreSQL dialect and verifies
  that no bind parameters are produced.

### Verification
- Complete scaffold suite: 390 tests passed, including 158 subtests.
- Real PostgreSQL smoke run: `ops_task` prepared and atomically published all
  five output tables, updated source state in the publication transaction, and
  left zero staging tables behind.


## 0.3.8

### Fixed
- **Advisory-lock acquisition was reentrant.** PostgreSQL counts session
  advisory locks, so acquiring the same one twice requires releasing it
  twice — a second acquisition would leave the server holding a lock after
  `release_task_lock()` while the publisher reported itself unlocked.
  A repeat now raises `DbPublishInvariantError` before any SQL is issued.

  Loud rather than silently idempotent: a second `begin_run()` also
  repeats predecessor cleanup, so it signals incorrect lifecycle use
  rather than a harmless retry. Releasing and reacquiring still works.

  The runner calls `begin_run()` once and closes afterwards, so this only
  ever affected direct callers and the extension seam.

- The constructor's `task_name` error said an unusable name "fails only at
  publication", which described the behaviour before validation was added.
  It now says "would otherwise fail only at publication".


## 0.3.7

### Fixed
- **The advisory lock recorded presence, not identity.** `_lock_held` was
  a boolean while the lock methods took an arbitrary task name, so a
  publisher holding task A's lock dropped task B's staging table on
  request — confirmed directly. `release_task_lock('task_b')` likewise
  cleared the flag while task A stayed locked for the session.

  `try_acquire_task_lock()`, `release_task_lock()` and
  `cleanup_predecessor_artifacts()` no longer take a task name; they use
  `self.task_name`, the publisher records which task it actually locked,
  and `_require_task_lock()` verifies the held identity.

- **`task_name` is now required and must be non-empty.** `None` was
  permitted on the theory that such a publisher does not participate in
  locking or ownership — false: `begin_run()` derived a key from `''` and
  staging wrote `"task": ""` into metadata `parse_staging_comment()`
  rejects, so the run prepared successfully and declared its own artifact
  unowned at publication. The test asserting `None` was allowed carried
  the same false claim and is rewritten.

- **`skip_reason` for a lock collision is now the token
  `'task_already_running'`**, matching `'sources_unchanged'` rather than
  being a sentence. `skip_reason` exists to be compared against.

- `README.md` documents both skip reasons; it previously described
  `skipped` as meaning sources were unchanged.

### Migration
- Custom publishers: `DbPublisher(...)` requires `task_name`, and the
  three lock methods lost their task argument. Both fail loudly.


## 0.3.6

### Changed
- **`output_excel` now defaults to `False`**, matching `output_db`. A task
  that declares no outputs produces none, and each output is switched on
  deliberately.

  It defaulted to `True`, which contradicted `docs/decisions/0007` in the
  one place a reader would notice: an aid you opt into should not be
  produced by a task that never asked for it.

  Patch, not minor: one default, zero affected call sites in this
  repository. The version number is not what warns anyone here — the
  migration note below is.

  **Migration.** A caller relying on the old default stops receiving
  workbooks with no error — nothing raises, the files simply are not
  written. Add `output_excel=True` explicitly. Every call site in this
  repository already passes it (57 of 57, checked with an AST walk before
  the change), so `tasks/` and `examples/` are unaffected.


## 0.3.5

### Documentation
- **`docs/decisions/0007` states plainly that Excel output is a local
  debugging aid, not a publication target.** No staging, no temporary
  files, no renames, no cleanup of abandoned artifacts, and no
  transactional relationship with database publication — permanently, not
  pending implementation.

  The asymmetry with the database path had been recorded as an open
  boundary, which read as an unfinished half of the publication design and
  kept reopening the question. It is not: a published table is consumed by
  things that must never see partial data, while a workbook is opened by a
  person. Giving files transactional guarantees would import temporary
  naming, ownership metadata, orphan cleanup and a scavenger for artifacts
  left by killed runs — on a filesystem that may be a remote share — to
  serve a consumer who is looking at them by eye.

  The ADR records what follows: a failed run may leave workbooks from
  pipelines that succeeded, those files may disagree with the database,
  and nothing downstream may read them programmatically. It also records
  the two cheaper alternatives that were rejected and why.

- `README.md`, `docs/architecture.md` and `docs/task-authoring.md` state
  the same rule where each audience meets it.


## 0.3.4

Invariants that held only because callers happened to do the right thing,
and two rules advertised as exact that were not.

### Fixed
- **`cleanup_predecessor_artifacts()` enforces the task lock itself.** It
  drops tables, and a direct caller could delete another live run's
  artifacts — confirmed directly with `lock_held` False. The runner path
  was safe by ordering; the invariant was not.
- **`task_name` is validated at construction.** An empty one derived a
  lock key and staged successfully, then wrote an ownership comment that
  `parse_staging_comment()` rejects — so the run reported its own artifact
  as unowned and failed at `commit()`, after every pipeline had run. It
  need not be a portable identifier, only a non-empty stable string.
- **The staging-name rule is literally exact.** `$` also matches
  immediately before a trailing newline and a quoted PostgreSQL identifier
  may contain one, so `x__stg_deadbeef_deadbeef\n` satisfied it. Now `\Z`.
- **`IdentifierPolicy(True)` was accepted**, producing an effective
  one-byte limit, because `bool` subclasses `int`. Now type-checked.
- **`_verify_columns()`'s empty-result branch raises instead of
  returning.** Its comment claimed the table might not exist yet; it
  cannot, because this runs immediately after `create table if not exists`
  on the same connection. An empty result means the check cannot see the
  table it is about to write to — an identifier-case or `search_path`
  anomaly — which is exactly what it exists to refuse. The test asserting
  the old behaviour was written from the same false premise and is
  rewritten.
- The predecessor-scan `LIKE` escapes all three underscores; the leading
  two were single-character wildcards, so the scan also matched names like
  `xastg_...`. Harmless — the strict regex filtered them — but the pattern
  now says what it means.

### Changed
- `rollback()`'s `break` after a failed `DROP` records why: the
  transaction is aborted, so continuing would cascade failures rather than
  drop more tables. "Drop as many as possible" is the plausible-looking
  edit that does not work without an intervening rollback.
- `advisory_lock_key()` notes the collision consequence — two different
  tasks serializing against each other, safe but confusing to diagnose.

### Notes
- `docs/decisions/0005` records the plan for `publisher_factory`: the next
  parameter that wants in should instead become one frozen
  `PublisherConfig`. Doing it now would be a gratuitous second break.


## 0.3.3

### Fixed
- **Cleanup could reconnect after session loss.** `rollback()` and
  `release_task_lock()` use the connection directly and so bypassed
  `ensure_connection()`'s invalidation check — confirmed directly,
  `rollback()` executed `DROP TABLE` and `COMMIT` on an invalidated
  connection while `_connection_lost` stayed False. Both now detect the
  loss and do nothing; the session's death releases the lock and the next
  run removes the artifacts under its own.
- **A failed source-state read-phase commit was swallowed**, so
  `sources_unchanged()` could report a successful comparison — and
  therefore a skip — across a database call that failed. It now raises
  `SourceCheckError` with the original as cause.
- **Cleanup did not enforce the strict staging-name rule.** A table whose
  name merely contained the infix could be dropped on the strength of a
  valid comment. It now requires the exact `__stg_<8 hex>_<8 hex>` shape,
  and that the name and comment agree on run token, schema and target
  token. The readable prefix is not recomputed, since it may have been
  truncated under a different identifier limit.
- **The ownership version field was compared by equality**, so `"v": true`
  and `"v": 1.0` both passed — Python considers both equal to `1`. Now
  type-checked.
- **`commit()` required the task lock only when a swap was pending**, so a
  publication plan holding only the source-state update could be committed
  by a direct caller without claiming the task.

### Changed
- Type inference runs before the preparation transaction opens.
  `_build_table()` scans the payload and may be O(rows); holding a
  transaction across it works against bounding preparation to database
  work.
- Connection fakes in the test suite implement `in_transaction()` and
  `invalidated`. One of them lacked `in_transaction()` and the resulting
  `AttributeError` was hidden by the swallowed exception above — the fake
  and the defect concealed each other.


## 0.3.2

Six findings against 0.3.1, each confirmed directly before fixing. The
first two undermined the staged model's correctness argument rather than
merely tightening it.

### Fixed
- **A transaction still spanned the run when source tracking was enabled.**
  Nothing committed between the source-state read and the pipeline loop, so
  the autobegun transaction survived until the first `publish()` — the
  whole run for a source-check-only task, and an hours-long transaction for
  an hours-long first pipeline. `sources_unchanged()` now ends its own read
  phase, which is where the phase is owned.
- **A lost session was not actually detected.** `mark_connection_lost()`
  existed but production never called it, so the terminal state was
  unreachable — and SQLAlchemy transparently reconnects an invalidated
  `Connection`, continuing on a session holding none of this run's advisory
  locks. `conn.invalidated` is now checked before every reuse, and
  `publish()`/`commit()` refuse on PostgreSQL without the lock, so the
  contract binds direct callers too.
- **Quoted identifiers broke at final verification.** `to_regclass()` parses
  its argument as an identifier expression and down-cases anything
  unquoted, so a staging name produced under `db_identifier_mode='quoted'`
  prepared correctly and was then reported missing. Verification now matches
  `nspname`/`relname` by exact value.
- **Preparation erased whatever sat at the generated name.**
  `drop(checkfirst=True)` bypassed the cleanup safety rule entirely — a
  table holding unrelated data was silently replaced, reproduced directly.
  It now creates and lets `relation already exists` fail loudly.
- **Ownership metadata was validated too loosely.** Requiring only marker,
  version and the presence of `task`/`run` meant a comment missing
  `target_schema`, `target_table` and `created_at` still authorized a drop.
  All documented fields are now required and type-checked; extra fields
  remain tolerated for forward compatibility.
- **`publish()` accepted a payload for another schema.** Cleanup scans one
  schema, so such a payload would leave an orphan no later run scans.
- **The server identifier limit was read after cleanup DDL.** `begin_run()`
  now resolves it first, honouring the stated before-first-DDL contract.

### Known boundary
- Excel output is written immediately inside the pipeline loop, so a later
  failure can leave new workbooks while DB publication is rolled back or
  left at its previous state. *(Recorded here as an open gap; settled in
  0.3.5 as a deliberate decision — see `docs/decisions/0007`.)*


## 0.3.1

Deferred cleanups, none of which changes behaviour a task would notice.

### Changed
- **`file_access` class renamed to `source_access`** and re-exported under
  the new name. The facade exported a class named after its own module, so
  `task_core.file_access` resolved to the *class* and the submodule could
  not be reached by attribute — the same trap as `types.py` shadowing the
  stdlib. Nothing imported the class by name; every caller obtains one from
  `build_source_access()`. The old name is not kept as an alias, because an
  alias would recreate the collision.
- **The adapter-registry guard is `if ... raise`, not `assert`.** `python -O`
  strips asserts, so the "fails at import time" guarantee silently
  disappeared in exactly the mode a production runner is most likely to
  use. Confirmed both ways: the guard now fires identically with and
  without `-O`.
- **`binding.py`'s deferred `context.py` import is hoisted.** It sat inside
  `build_resource_context()` with no comment explaining why; hoisting
  creates no cycle, and the function runs once per run so there was no
  import-cost argument either. The module docstring now lists the
  dependency and records why the deferral was unnecessary.
- **`requirements.txt` carries lower bounds where a lower version
  demonstrably breaks** — `sqlalchemy>=2.0` (`Connection.commit()` does not
  exist in 1.4; 10 call sites), `pandas>=2.0`, `openpyxl>=3.1`,
  `petl>=1.7` — and records the verified versions in a comment rather than
  inventing floors from them. It also states plainly what lower bounds do
  *not* do: they say nothing about the upgrade hazard that actually
  threatens this codebase, which would need upper bounds or a lockfile.

### Tests
- A structural guard rejects any facade export that shadows its own
  submodule, rather than checking the one name that did.

### Notes
- The 0.2.12 note about the identifier limit reaching preflight but not
  the publisher is resolved: `IdentifierPolicy` (0.3.0) is passed to both,
  and `run_pipelines(db_max_identifier_bytes=20)` is now enforced end to
  end.


## 0.3.0

Minor rather than patch: `publisher_factory`'s signature changes, which is
a deliberate one-time break to an advertised extension seam.

### Added
- **Staged publication.** Each DB target is prepared in its own committed
  transaction — staging table created, loaded, verified, marked with
  ownership metadata, committed — and a single short publication
  transaction then swaps them all. No transaction spans the run any more.
  See `docs/decisions/0005`.
- **Task advisory lock.** A session-scoped `pg_try_advisory_lock` claimed
  before fingerprinting. A second concurrent run of the same task exits
  immediately with `skipped=True` and
  `skip_reason='another run of this task holds the advisory lock'`,
  logged at WARNING.
- **Predecessor cleanup.** Staging artifacts left by a dead previous run
  are dropped under the lock, with no age threshold needed. Only artifacts
  positively identified by ownership metadata are dropped; unknown
  ownership is never dropped. See `docs/decisions/0006`.
- **Ownership metadata** in `COMMENT ON TABLE`, replaced at swap time with
  live-table provenance recording which task and run produced the data.
- `IdentifierPolicy`, a frozen policy object shared by preflight and the
  publisher, replacing two independently defaulted integers.
- `PublicationPlan`, carrying the source-state write into the publication
  transaction without changing `commit()`'s signature.
- Preparation verifies exact ordered column names and that rows loaded
  equals rows in the payload.

### Changed
- **`publisher_factory` is now called with `identifier_policy`,
  `publication_plan` and `task_name`** in addition to `creds`, `schema`
  and `logger`. Custom factories must accept them.
- The protocol gains exactly one member, `begin_run()`, which claims the
  task and cleans predecessor artifacts as one precondition.
- `rollback()` drops this run's committed staging tables rather than
  undoing a transaction, and never raises — a cleanup failure must not
  replace the exception that caused the abort.
- `close()` releases the advisory lock explicitly rather than relying on
  session end.
- `ensure_connection()` refuses to reconnect after the connection is lost,
  rather than silently continuing without the lock.

### Removed
- `discard_pending_read()`. It managed a lifecycle state the staged model
  makes impossible: the source-state read is a bounded phase that commits,
  so there is no pending read to discard. Its tests and documentation go
  with it.

### Fixed
- A test double replaced `committed_rows` on commit instead of merging, so
  a second commit with nothing pending erased everything the first had
  committed. Harmless while a run had one commit; wrong the moment the
  staged model gave it several.


## 0.2.13

### Changed
- `SourceStateStore._verify_columns()` branches on the dialect instead of
  catching every exception. The catch-all covered backends without
  `information_schema`, but also swallowed permission failures, connection
  failures and any future incompatibility in the query — in each case the
  fail-early guarantee silently disappeared while the run went on to fail
  later at the upsert anyway. Its scope stays deliberately narrow:
  required column names only, not types, nullability, primary key or
  order.

### Fixed
- `README.md` no longer implies both outputs are produced. Excel and
  PostgreSQL outputs are independent — a task may write either, both, or
  neither.
- `README.md` documents the `RunResult` a scheduler should inspect, and
  drops an inaccurate line count for the example.
- `docs/architecture.md` version claim, its pipeline example's signature
  (bound resources are keyword-only), and its "five methods" reference to
  the six-method adapter interface.
- A second positional-resource example in `docs/task-authoring.md`, missed
  when the first was corrected.
- The 0.2.11 changelog note about portable enforcement now records that
  0.2.12 implemented it.

### Tests
- Documentation tests now check the architecture document's version claim
  and reject positional bound-resource signatures in any documented
  example — both classes of drift that had already occurred.
- File reads in documentation and source-inspection tests use
  `Path.read_text()`; the suite emitted 110 `ResourceWarning`s.
- The quick-start test captures the example's stdout instead of printing
  it through the suite.


## 0.2.12

### Added
- `SourceStateStore` refuses to continue when it cannot inspect the
  existing table's columns on PostgreSQL, rather than silently skipping
  the compatibility check. Its scope is deliberately column-name presence
  only — not types, nullability, primary key or order.
- `SourceStateStore` verifies the server's real `max_identifier_length`
  before its own DDL. A source-check-only run never called `publish()` and
  therefore never verified the limit at all.
- `SourceStateStore.ensure_table()` compares the existing table's columns
  against what it reads and writes. `create table if not exists` silently
  accepts a table left by an older version, which then failed at the first
  write — mid-run, after every pipeline had executed.

### Changed
- `IDENTIFIER_MODES` moved to `types.py` beside `PORTABLE_IDENTIFIER_RE`.
  `PipelineSpec` and payload validation now share one definition instead of
  each carrying its own literal tuple.
- `server_identifier_limit()` extracted as a module-level function so
  `source_state.py` can use it without the publisher protocol growing
  another member.

### Fixed
- Runtime validation applies the portable pattern to `payload.table_name`
  under `portable` mode and to `payload.schema` under either mode. A
  directly constructed `DbPayload` — which never passes through preflight —
  could publish to a non-portable table in the default strict mode.

### Notes
- The identifier limit still reaches preflight but not the publisher
  constructor, so `run_pipelines(db_max_identifier_bytes=...)` does not
  govern runtime validation. Deferred deliberately: the fix is a shared
  naming-policy object passed to both, which arrives with the staged
  publication model rather than as a second breaking change to
  `publisher_factory`.

### Changed
- Documentation restructured. The former single 1,975-line `README.md`,
  organised chronologically by when each finding was made, is replaced by
  a short front-door `README.md`, `docs/architecture.md` describing the
  system as it works now, `docs/task-authoring.md` covering usage, and
  `docs/decisions/` holding durable rationale as ADRs. Development history
  now lives in git and in this file.

### Added
- `examples/local_task.py` — a complete task that runs with no share, no
  database and nothing outside the project's own requirements, and is the
  README's quick start.
  `tests/test_docs.py` executes it, so a documented example that cannot be
  run is now a test failure.

### Fixed
- **The documented "minimal task" was not runnable.** It called an
  undefined `read_sheet()`, pointed at an SMB path, and required
  credentials — nobody who had merely cloned the repository could execute
  it. Replaced by the runnable example above.
- **Bound pipelines take keyword-only resources.** The documented example
  showed `run(cls, ctx, source)`; the scaffold requires
  `run(cls, ctx, *, source)` and rejects the other form at validation.
- **The transaction description was wrong.** Both `README.md` and
  `docs/architecture.md` said one transaction spans the whole run. The
  source-state read runs in its own implicit transaction which
  `discard_pending_read()` rolls back; the publication transaction begins
  at the first `publish()`.
- **The layering diagram placed `table_adapters.py` at level 1** while it
  imports from `db_publish.py` at level 2. Now level 2, and the diagram is
  parsed out of the document and checked against real imports.
- The adapter interface is described as six methods, not five.
- Python requirement corrected to 3.11 or newer (enforced at import),
  verified on 3.12.3 and 3.13.5.
- `creds` documented as required only when the run actually uses
  PostgreSQL, not whenever `output_db=True`.
- Source-change checking with `output_db=False` is ignored with an
  informational log, not an error as previously documented.
- `requirements.txt` separates `task_core`'s own dependencies from those
  needed only by the reference tasks (`babel`).
- `task_core`, the docs, the tests and the example no longer name the
  in-house helper module that `tasks/` imports. The scaffold does not ship
  it, does not depend on it, and should not document it.
- ADR 0004 no longer claims portable enforcement on `payload.table_name`
  and `payload.schema`, which was not implemented at the time. Implemented
  in 0.2.12; the ADR now describes it as current behaviour.
- `docs/architecture.md` corrects a long-standing documentation error:
  `PipelineSpec.db_output` is **declarative only**. The scaffold validates
  it and reads it during preflight, but never applies it — pipelines
  project their own columns, conventionally with
  `.cut(*cls.spec.db_output)`. `db_contract`, by contrast, *is* applied by
  the scaffold.


## 0.2.11

### Added
- Source-state table treated as a reserved publication target. Preflight
  validates its schema and table identifiers and rejects any pipeline
  declaring them as its own `db_table`.
- Direct coverage for the staging swap against a real engine.

### Changed
- Staging finalization moved inside `DbPublisher.commit()` and made
  private. The public publisher protocol is back to `publish`, `commit`,
  `rollback`, `close`.
- `SHOW max_identifier_length` failures on PostgreSQL now raise rather
  than falling back silently; non-PostgreSQL backends still fall back.
- Identifier matching uses `fullmatch()` rather than `match()`.
- `DbPayload.identifier_mode` is validated against a shared
  `IDENTIFIER_MODES` constant.
- Excel target comparison resolves to an absolute path; `db_table`
  comparison no longer strips whitespace.
- Type inference no longer guesses `Text` when the whole sample is null.

### Fixed
- `publish()` followed by `commit()` without the runner reported success
  while publishing nothing.
- An alternate publisher written against the previous protocol failed at
  the end of an otherwise successful run.


## 0.2.10

### Added
- Publication through per-run staging tables. `publish()` loads a staging
  table; `commit()` swaps it into place. Live tables stay readable for the
  whole run instead of being locked from first publish to commit.
- Two-tier identifier validation: connection-free preflight before
  `build_context()`, and runtime verification against the server's own
  `max_identifier_length`.
- `PipelineSpec.db_identifier_mode` (`'portable'` | `'quoted'`).
- `PORTABLE_IDENTIFIER_RE` in `types.py`, shared with `source_state.py`.

### Fixed
- Duplicate `db_table` detection no longer casefolds. SQLAlchemy quotes
  mixed-case identifiers, which defeats PostgreSQL's folding, so `Sales`
  and `sales` are genuinely different tables.


## 0.2.9

### Added
- Rejection of duplicate output targets. Two active pipelines declaring
  the same `excel_name` or `db_table` silently overwrote each other; the
  DB case destroyed the first pipeline's rows inside the committed
  transaction and reported success.


## 0.2.8

### Fixed
- Zero-dimensional numpy arrays are treated as scalars, not containers.
  Closes an asymmetry where `np.array(np.nan)` normalized to `None` but
  `np.array(pd.NaT)` did not.


## 0.2.7

### Fixed
- `gc.collect()` applied to the retained-workbook path, not only to
  short-lived metadata reads. `excel_resource.close()` drops its workbook
  references before triggering the collect.
- `db_resource.close()` is exception-safe and clears its table cache.


## 0.2.6

### Fixed
- Sampled type inference verified against the remaining rows for
  `BigInteger` and `Date`, the two types PostgreSQL silently widens.
  Previously a column whose first 5000 rows were integers and whose later
  rows held a decimal inferred `bigint`, and PostgreSQL's assignment cast
  rounded the value on insert without error.


## 0.2.5 and earlier

Reconstructed from the previous README, which recorded findings as they
were made rather than by release. Not exhaustive; git holds the detail.

Established in this period:

- **`run_pipelines()` orchestration** — pipeline sequencing, Excel export,
  DB publication, result plumbing.
- **Lazy, cached resources** with the identity guarantee that a
  fingerprinted resource is the same object later injected into the
  pipeline.
- **`task_context` ownership and close-once semantics.**
- **Cleanup redesign** — `attempt_all_cleanup()` runs every step and
  collects failures; the caller decides based on an explicitly tracked
  primary error whether to log or raise them. A cleanup failure never
  replaces the exception that broke the run.
- **Source-change checking** with fingerprints stored in PostgreSQL,
  written in the same transaction as the published tables.
- **Table adapters** — petl and pandas behind one five-method interface,
  with `stabilize()` for lazy tables traversed more than once.
- **`bind()` and `build_resource_context()`** — declarative resource
  wiring with structural validation of every declared binding.
- **SMB/DFS file access**, including `gc.collect()` on workbook release
  and exclusion of hidden, system and Excel temporary files.
- **openpyxl and pandas compatibility handling**, including pandas 3's
  `StringDtype` default converting `None` to `nan`.
