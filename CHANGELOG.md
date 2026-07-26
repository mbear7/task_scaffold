# Changelog

Notable changes to `task_core`. Versions are the value of
`task_core.__version__`.

The format is loosely [Keep a Changelog](https://keepachangelog.com/).
This file starts at 0.2.11; earlier history is reconstructed below in a
single entry from the previous README, which recorded changes
chronologically rather than by release.


## Unreleased


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
- Excel output is still written immediately inside the pipeline loop, so a
  later failure can leave new workbooks while DB publication is rolled back
  or left at its previous state. The staged model addresses DB publication
  duration and atomicity; staged filesystem publication is not implemented.


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
