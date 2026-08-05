# Changelog

Notable changes to `task_core`. Versions are the value of
`task_core.__version__`.

The format is loosely [Keep a Changelog](https://keepachangelog.com/).
This file starts at 0.2.11; earlier history is reconstructed below in a
single entry from the previous README, which recorded changes
chronologically rather than by release.



## 0.6.14

Refines the standalone PostgreSQL schema generator without changing task_core
runtime behavior or public API.

### Changed
- When `--schema` is omitted, `tools/generate_output_schema.py` now resolves the
  unqualified table through PostgreSQL's active `search_path` instead of
  forcing `public`. An explicit `--schema` or edited `SCHEMA_NAME` still takes
  precedence, and generated code records the schema actually resolved.
- `SCHEMA_NAME` now defaults to `None`, so notebook/editor execution follows
  connection options such as `options='-c search_path=bsr,public'` from
  `pgcreds` unless the user explicitly selects a schema.
- Generated declarations omit redundant `nullable=True`; only `NOT NULL`
  columns emit `nullable=False`.

### Tests
- Added search-path resolution, explicit-schema precedence, resolved-schema
  output, missing-relation diagnostics, nullable-default rendering and
  standalone `pgcreds` `options` coverage. The tool suite now contains 39
  deterministic tests and runs normally and under `python -O`.


## 0.6.13

Performs a safe internal dead-code and comment-hygiene pass without changing
any public task_core surface. `RunResult` and its convenience properties remain
unchanged. The preserved alternative implementations in `tasks/hr_task.py` are
intentionally untouched.

### Changed
- Removed unused runtime imports and one unused COPY constant.
- Stopped re-exporting private `db_values` implementation helpers from
  `db_publish`; internal callers and tests now import the private kernel from its
  owning module. Public exceptions, policies and publisher classes remain where
  they were.
- Removed the private `_build_copy_sql` helper from `db_copy.__all__` while
  retaining the helper itself for internal use and direct tests.
- Reworded implementation-phase and removed-file comments so they describe the
  current architecture rather than completed ADR construction phases.
- Removed unreachable statements and unused test-only counters/imports.

### Tests
- Added source-hygiene tripwires covering the private module boundary, public
  export list, removed dead symbols and current comments.


## 0.6.12

Adds a repository-local PostgreSQL schema generator for moving an existing
task_core-created table from inferred output to an explicit declared contract
without hand-writing dozens or hundreds of `OutputColumn` entries.

### Added
- **`tools/generate_output_schema.py`.** The standalone script performs
  read-only PostgreSQL catalog introspection and emits paste-ready Python for
  either a class-level `PipelineSpec(output_schema=...)` argument or an
  `OUTPUT_SCHEMA` class constant. It preserves physical column order, declared
  type parameters and nullability for the task_core-supported PostgreSQL type
  subset.
- **Command-line and notebook operation.** Standalone execution provides
  `--help`, schema/table, connection override, output-style, exclusion and
  output-file arguments. Notebook/editor execution requires no command line:
  edit `TABLE_NAME` and `SCHEMA_NAME` in the configuration block at the top of
  the file and run it.
- **Credential precedence.** Command-line values override inline `DB_*`
  settings, which override an importable `pgcreds.pgcreds` mapping. Unspecified
  pgcreds options such as `sslmode` are preserved.
- **Fail-complete diagnostics.** Unsupported types, domains, enums, identity or
  generated columns, non-default collations and nonportable identifiers are
  reported together and no partial schema code is emitted. Column defaults are
  reported as warnings because `OutputColumn` does not represent them; refill
  preserves an existing default while replace recreates the table without it.
- **Framework-column exclusion.** Repeatable `--exclude-column` and the inline
  `EXCLUDE_COLUMNS` setting allow framework-owned columns such as
  `etl_updated_at` to be omitted from the generated user-owned schema.

### Tests
- Added 35 deterministic tests covering catalog shape, credential precedence,
  standalone help, notebook defaults, connection and file cleanup, exact
  SQLAlchemy/PostgreSQL type rendering, aggregate unsupported-column
  diagnostics, exclusions and AST-valid class indentation.

### Documentation
- Consolidated the obsolete standalone 0.5.1 and 0.5.2 migration notes into
  the current authoring contract and this changelog. Removed the two files and
  all stale links; separate migration guides are now reserved for exceptional
  migrations that need more than concise current guidance and release history.

## 0.6.11

Closes ADR 0011 after the complete target-host acceptance campaign and hardens
the lifecycle of the framework-owned default COPY spool directory.

### Changed
- **ADR 0011 is accepted and complete.** The final PostgreSQL 18.4 campaign
  passed 19/19 harness self-tests, 4/4 repository commands, 13/13 adversarial
  concurrency/failure cases, 10/10 memory-scaling checks, 24/24 randomized
  1m/10m release measurements and 9/9 aggregate assertions with zero owned
  staging or spool residue.
- **The empty implicit spool root is removed best-effort.** After owned spool
  files are deleted, task_core attempts atomic `rmdir()` on
  `task_core-copy-spool`. Nonempty roots are preserved, transient failures are
  retried and logged, and an operator-configured `spool_directory` is never
  removed, even when it resolves to the default path. Spool creation
  recreates a directory that a concurrent empty-root cleanup removed between
  resolution and the exclusive file open.
- **Task-authoring guidance records the final release matrix.** It now states
  explicitly that COPY optimizes source-to-staging transport while refill still
  rewrites the live table, can block readers for minutes and can amplify WAL.

### Tests
- Added direct filesystem tests for default-root removal, missing/nonempty
  roots, configured-directory preservation, retry behavior and logging.
- Added a publisher integration regression proving a successful default-policy
  COPY removes the empty framework-owned root.


## 0.6.10

Optimizes the declared one-pass COPY row loop identified by the 0.6.9 live
cProfile campaign. Publication, spool protection, bounded-memory behavior and
the inferred two-pass path are unchanged.

### Changed
- **Declared COPY uses compiled direct field writers.** One writer per output
  column now combines native missing handling, declared validation and
  COPY-text encoding directly into the reusable row buffer. Ordinary Python
  `bool`, integer, `Decimal`, float, text, bytes, date and datetime values no
  longer pass through the generic pandas-aware normalization stack.
- **Scalar-wrapper compatibility remains explicit.** pandas, NumPy and other
  non-native scalar wrappers still fall back to the shared `_normalize_value()`
  kernel before the same declared constraints are enforced.
- **Declared COPY no longer allocates a normalized row tuple.** Source width is
  checked first and declared wire-order fields are written directly from the
  source row.

### Documentation and acceptance
- ADR 0011 now names the current localhost PostgreSQL 18.4 instance as the
  target acceptance environment. A separate VPS and PostgreSQL parameter tuning
  are not closure requirements.
- The external evidence pack adds a 19-case offline harness self-test, dedicated
  repository verification, 13-case localhost concurrency/failure coverage,
  10-case lazy-source memory scaling and a final 24-case 1m/10m release matrix
  with nine aggregate acceptance assertions. Runtime behavior is unchanged.

### Tests
- Closed retained Excel resources inside each temporary-directory scope in the
  15 repository tests that materialize workbook data. The previous fixtures
  relied on Linux allowing deletion of an open file; on Windows,
  `TemporaryDirectory` correctly failed with `WinError 32` before the deferred
  `addCleanup()` callback could run. Runtime resource behavior is unchanged.
- Added a documentation regression that preserves the narrowed ADR 0011 closure
  scope, the five executable closure layers and their mandatory case counts.
- Corrected three closure-harness assumptions found by the second target run: handled retry exhaustion now expects current-run staging cleanup, terminal connection loss is sampled after invalidation detection, and successor cleanup runs in `finally` so one failed case cannot contaminate the next. Runtime code is unchanged.
- Corrected the memory-scaling evidence harness found by the third target run: project-root discovery now precedes `task_core` import, worker subprocesses receive `TC_PROJECT_ROOT`, Windows RSS bindings declare their ctypes signature, sampler shutdown is safe before startup, and every worker records and validates package provenance. Runtime code is unchanged.
- Corrected the lazy-source adapter in the memory-scaling evidence harness found by the fourth target run. The previous callable passed to `petl.fromdb()` returned a psycopg2 connection, while PETL's callable form expects a fresh DB-API cursor and therefore called `execute()` on the connection. The campaign now uses the framework's managed `db_resource` through `task_context`, requests a named server-side cursor, and closes the source connection through normal context cleanup. Four harness regressions raise the offline self-test to 19 cases. Runtime code is unchanged.
- Added a regression proving native declared values do not call the generic
  normalizer, generic declared validator or generic family serializer. The test
  fails against 0.6.9 at `_normalize_copy_row()` for the intended reason.
- Added coverage proving NumPy/pandas scalar wrappers use the normalization
  fallback and that native float/Decimal NaN markers retain declared NULL
  semantics.
- A source-only 100k-row, five-column diagnostic reduced median final-spool
  preparation from 0.748 s to 0.261 s for plaintext and from 0.782 s to
  0.265 s for AES-256-GCM in this environment. These are local diagnostic
  measurements, not live PostgreSQL release acceptance.


## 0.6.9

Optimizes COPY preparation without changing publication or spool-protection
semantics. Declared schemas now use a direct one-pass final-spool path; both
schema modes use a compiled positional serializer in the row loop.

### Changed
- **Declared COPY no longer creates a neutral predecessor spool.** Because the
  complete target schema is already known, normalized rows are validated and
  serialized directly into the final COPY-text spool before the database
  transaction opens. Source traversal, cleanup, encryption and publication
  guarantees remain unchanged.
- **COPY row serialization is compiled once per payload.** Source positions and
  scalar families are resolved before the row loop. Preparation no longer
  rebuilds a dictionary, output list and type-family lookup for every row.
- **Task-authoring performance guidance is now explicit.**
  `docs/task-authoring.md` contains a dedicated chapter covering loader
  selection, declared versus inferred cost, memory and scratch-disk trade-offs,
  refill behavior, PostgreSQL tuning boundaries and benchmark methodology.

### Tests
- Added regressions proving declared preparation opens only the final copytext
  spool and inferred preparation does not call the mapping-based public row
  serializer per row. Both tests fail against the 0.6.8 implementation for the
  intended reason.
- Phase 8 correctness, failure-injection and final performance acceptance must
  be rerun against this optimized implementation before a public-release
  checkpoint.


## 0.6.8

Corrects the first live Phase 8 acceptance findings from PostgreSQL 18.4.
Phase 8 remains open until the corrected correctness, failure-injection and
performance campaigns pass.

### Fixed
- **Timezone-aware inferred datetimes now resolve to `TIMESTAMPTZ`.** INSERT
  and COPY previously classified naive and aware Python datetimes as one
  family and resolved both to `timestamp without time zone`. PostgreSQL then
  applied session-timezone semantics on INSERT while COPY discarded the
  offset for the same target type, breaking value equivalence.
- **Ambiguous inferred temporal columns fail before database work.** A column
  mixing timezone-aware datetimes with naive datetimes or bare dates is now
  rejected with a contextual inference error. The scaffold does not invent a
  timezone for values that do not carry one.
- **Sample verification includes datetime awareness.** An aware/naive
  mismatch appearing after the inference sample boundary triggers full
  re-inference and rejection instead of silently publishing a naive timestamp.

### Tests
- Added materialized and streaming inference regressions for aware-only,
  naive/date, mixed-awareness and post-sample temporal columns.
- Added COPY preparation regressions proving aware inferred columns resolve to
  `TIMESTAMPTZ` and ambiguous rows fail while removing current-run spools.
- Updated the external Phase 8 correctness script to assert the exact inferred
  timestamp types and to suppress localized PostgreSQL `NOTICE` messages in
  the LATIN1-client test. The first live run remains preserved as failed
  evidence; a clean rerun is required against 0.6.8.


## 0.6.7

Phase 7 of ADR 0011 wires predecessor COPY-spool cleanup into the task
lifecycle under the PostgreSQL advisory lock. This release also declares the
spool serializer's UTF-8 encoding explicitly in the COPY statement.

### Added
- **Predecessor spool cleanup under the task lock.** After `begin_run()` wins
  the task advisory lock, `DbPublisher` deletes spools that are positively
  identified as belonging to an earlier execution of that task. Filename
  token/stage and plaintext header token/stage/task must all agree. This
  includes residue left when a process crash prevented current-run cleanup;
  it does not recover task data or resume an interrupted publication.
- **Fatal known-owned residue.** If bounded unlink retries cannot remove a
  positively owned predecessor spool, startup fails instead of silently
  accumulating another spool beside known task data. Unknown, malformed and
  foreign files remain untouched.

### Fixed
- **COPY encoding is explicit.** COPY SQL includes `ENCODING 'UTF8'`, matching
  the serializer's unconditional UTF-8 byte output rather than depending on
  the connection's current client encoding.
- **Custom-publisher contract wording is exact.** The documentation no longer
  claims that `PublisherConfig` made the factory constructor permanently
  stable. Since 0.6.6, strict custom factories must accept the documented
  `copy_load_policy` keyword. No compatibility shim is provided.

### Tests
- Added lifecycle tests proving predecessor spools are deleted only after the
  task advisory lock is acquired and remain untouched when lock acquisition
  fails.
- Added regression coverage for fatal known-owned cleanup failure and explicit
  UTF-8 COPY SQL. Each independent test was reverted against its fix and
  confirmed to fail for the intended reason before the fix was restored.
- Live PostgreSQL acceptance and the performance campaign remain Phase 8 and
  are not claimed by this release candidate.


## 0.6.6

Phase 6 of ADR 0011 connects the prepared encrypted COPY spool to PostgreSQL
through psycopg2 `copy_expert()` and makes `db_loader='copy'` a public loader.
The change preserves `DbPublisher` ownership of the existing connection,
transaction, staging DDL, verification, comments, rollback and publication.

### Added
- **Same-connection DBAPI COPY transport.** `db_copy.load_copy_into_staging()`
  opens only a cursor on the SQLAlchemy connection supplied by the publisher,
  streams `PreparedCopySource.open_reader()` into `copy_expert()`, and returns
  the exact row count captured during Phase 5 preparation. It creates no
  engine or second connection and owns no transaction boundary.
- **Public COPY pipeline path.** `DB_LOADERS` and publisher dispatch now include
  `'copy'`; `run_pipelines()` builds a one-shot row-source payload, skips
  `nrows()` on database-only COPY output, and obtains the exact count from the
  publisher after preparation.
- **COPY payload preparation inside `DbPublisher`.** The publisher prepares the
  spool before opening the staging transaction, creates staging from the
  authoritative prepared schema, loads and verifies the exact count, removes
  the final current-run spool, and registers the existing pending-publication
  artifact.

### Failure handling
- A raw driver connection reported closed during COPY explicitly invalidates
  the SQLAlchemy connection so the existing fatal no-reconnect rule remains
  effective.
- COPY, authentication and cursor errors preserve the primary exception;
  current-run spool cleanup is attempted on every path, and successful
  preparation cannot commit while its final spool remains undeleted.

### Tests
- Added exact COPY SQL/quoting and bounded-reader tests, lost-connection
  invalidation, cursor-close precedence, publisher success/failure cleanup,
  declared reordering through the public path, and a runner test proving COPY
  does not call `nrows()`.
- Complete unit suite: 759 tests. Live PostgreSQL acceptance remains Phase 8
  and is not claimed by this release.


## 0.6.5

Phase 5 of ADR 0011 was corrected and hardened before DBAPI COPY integration.
The internal COPY-preparation path now matches the accepted INSERT data
contract for normalization, inferred widening, overrides, nullability and
declared-column ordering. Spool bodies are encrypted by default; public
`db_loader='copy'` activation remains deferred to Phase 6.

### Fixed
- **COPY preparation now normalizes every source value exactly once during
  the neutral pass**, using the same `_normalize_value` kernel as INSERT.
  NumPy/Pandas scalars become native Python values and missing markers become
  `None` before inference and spooling.
- **Inferred COPY serialization now preserves widening semantics instead of
  applying declared-mode validation to every inferred cell.** Mixed numeric,
  date/datetime and fallback-to-text families produce the same resolved schema
  and value semantics as the INSERT baseline; declared mode remains strict.
- **`db_type_overrides` and `db_not_null_columns` now reach COPY preparation.**
  Declared schemas may differ from source order and are emitted in declaration
  order, matching INSERT.
- **`prepare_copy_source()` now returns `PreparedCopySource`**, carrying the
  final path, resolved columns, exact one-pass row count, on-disk byte count,
  ownership identity and bounded reader configuration.
- **COPY policy propagation is complete.** The runner-side composition uses
  `PublisherConfig.copy_load_policy`; a task-level encryption override wins
  only for that pipeline.

### Security and cleanup
- **Both neutral and final spool bodies use AES-256-GCM by default.** Each
  spool receives a fresh key that is never intentionally persisted by
  task_core; only the plaintext ownership header (including the nonce),
  ciphertext and authentication tag are stored. The final
  spool is exposed through a bounded decrypting reader, so no decrypted
  temporary file is created.
- **Task-level opt-out:** `PipelineSpec.db_copy_spool_encryption=False` writes
  plaintext bodies using the same versioned container and cleanup rules. The
  opt-out emits a warning. `CopyLoadPolicy.encrypt_spools=True` is the
  deployment default.
- **Cleanup now retries transient unlink failures and logs every residual path
  exactly.** Header/filename token and stage must agree before predecessor
  cleanup considers a file positively owned. File-creation/header failures
  attempt to remove the path immediately.
- Added the `cryptography` runtime dependency for streaming AES-GCM.

### Tests
- Added end-to-end COPY-preparation parity for pandas/NumPy normalization,
  inferred widening, type overrides, non-null constraints, declared ordering,
  exact row counts and policy propagation.
- Added encrypted/plaintext equivalence, wrong-key, corruption, truncation,
  ownership-consistency and cleanup fault-injection coverage.


## 0.6.4

Phase 5 of ADR 0011 landed as one flat commit: the internal row-source →
spool-preparation chain for `db_loader='copy'`, plus the runner wiring
that composes and hands off a `_ProjectedRowSource`. `db_loader='copy'`
remains rejected at every public boundary; the new module and its
composition are exercised only by helper-level tests. Phase 6 will lift
the public rejection and integrate `copy_expert()`. Callers using the
default `db_loader='insert'` observe no API or behavioral change.

### Added
- **`task_core/db_copy.py`** -- new level-2 module, the COPY-loader
  spool-preparation subsystem. Public shape (still test-only):
  `CopyLoadPolicy` (moved from `db_publish.py` so its home matches its
  layer, re-exported), `SpoolFormatError`, `SPOOL_STAGES`,
  `SPOOL_FILENAME_RE`, `compose_ownership_token`,
  `compose_spool_filename`, `parse_spool_filename`, `write_spool_header`
  / `read_spool_header`, `resolve_spool_directory`, `SpoolIdentity`,
  `prepare_copy_source`, `cleanup_spool_paths`. Internal:
  type-neutral binary spool grammar (write + read), streaming inference
  accumulator (`_InferenceStreamState` in `db_values.py`), target-aware
  COPY-text serializer, and current-run cleanup helpers. The 0.6.5 correction
  completed INSERT/COPY semantic parity and hardened cleanup. See ADR 0011
  §Implementation sequence Phase 5.
- **`_prepare_copy_source_for_pipeline`** in `task_core/export.py` --
  runner-side composition of the row-source chain: reads
  `adapter.to_row_source()`, resolves framework columns from
  `spec.db_updated_at` via `_build_framework_columns`, builds a
  `RowProjection` for `db_contract` + framework, wraps in
  `_ProjectedRowSource`, and hands the iterator to
  `prepare_copy_source()`. Runner branches into it via the Phase 4
  planning skeleton. Publicly unreachable in 0.6.4 because
  `db_loader='copy'` is still rejected; helper-level tests exercise the
  full composition.
- **`framework_columns` kwarg on `prepare_copy_source()`** -- pins
  resolved framework-column types by name after inferred-mode
  accumulation, mirroring the INSERT path's `_resolve_payload_schema`
  bypass at `db_values.py:722-725`. Inference resolves the datetime
  family to naive `sa.DateTime()`; without the bypass, an aware
  `etl_updated_at` value would fail pass-2 validation against the
  inferred naive column type. With it, declared-mode value validation
  stays byte-identical between the two loaders.

### Fixed
- **`_validate_declared_value` at `db_values.py:706` now receives
  `payload.table_name`, not the full `DbPayload`.** The 5.x refactor
  that made `_validate_declared_value` stateless (so the same kernel
  serves both the INSERT-path `_resolve_payload_schema` pass and the
  COPY-path pass-2 in `db_copy.serialize_row_to_copytext`) updated
  every internal `_declared_value_error` call but missed the top-level
  call site. The regressed contextual error message interpolated the
  whole payload repr instead of `'target'`, breaking the ADR-required
  `'{table}': output row N column 'C'.*NUL character` contract that
  `test_nul_character_in_declared_text_is_rejected_contextually`
  asserts. Fixed and verified by revert-observe-restore.

### Tests
- **`Test8DbCopyBoundary`** in `tests/test_docs.py` -- the ADR §Tests
  module-boundary tripwires for the new loader, mirroring
  `Test7DbInsertBoundary`: no `DbPublisher` import, no transaction
  primitives (`begin`, `commit`, `rollback`), no
  `create_engine`/`URL`/`connect`. AST-scan rather than call-count
  mocks. Verified by revert-observe-restore against an injected
  `.begin()` call.
- **`Test9DbValuesBoundary`** in `tests/test_docs.py` -- the ADR §Tests
  boundary that the stateless kernel imports neither the publisher nor
  either loader (`task_core.db_publish`, `task_core.db_insert`,
  `task_core.db_copy`). Verified by revert-observe-restore against an
  injected `from task_core.db_insert import ...`.
- **`tests/test_db_copy.py`** -- direct helper coverage for the new
  module: policy handling, directory resolution, filename grammar +
  header, ownership-token digest, neutral spool round-trip,
  target-aware COPY-text serializer (`NULL`, empty string, literal
  `\\N`, tabs/newlines/carriage returns/backslashes, Unicode, Decimal
  precision + scale, Boolean and integer boundaries, non-finite
  floats, date + both timestamp timezone modes, binary, row-width
  mismatch, unsupported non-scalar), streaming schema-state parity
  with the INSERT-path `_infer_column_type` corpus, success + failure
  cleanup paths, and the `prepare_copy_source()` orchestrator.
- **`Test17eComposeCopySourceForPipeline`** in `tests/test_db_publish.py`
  -- runner-side composition helper: patches `validate_db_loader` so
  `db_loader='copy'` can construct a spec, then asserts
  `_prepare_copy_source_for_pipeline` returns a copytext spool path
  and the resolved column list matches declared/inferred expectations
  including framework-column type pinning.
- **`test_declared_output_schema_appends_framework_columns_in_order`**
  in `tests/test_db_publish.py` extended to assert the framework
  column's resolved SQLAlchemy type carries `timezone=True`, so the
  INSERT and COPY paths resolve framework columns identically.

### Changed
- **Level diagram in `docs/architecture.md`** adds `db_copy.py` at
  level 2 alongside `db_publish.py`, `source_state.py`,
  `table_adapters.py`, and `export.py`. The lateral-dependencies note
  records that `db_publish.py` re-exports `CopyLoadPolicy` from
  `db_copy.py` and that `db_copy` does not import back. Enforced by
  `Test5TheDocumentedLevelMapMatchesRealImports`.
- **ADR 0011 §Verification status** notes that 0.6.4 shipped Phase 5
  (both the internal chain and the runner wiring), with `db_loader='copy'`
  still publicly rejected pending Phase 6.


## 0.6.3

Correction patch for four gaps external review found against 0.6.2, and
a downgrade of language in ADR 0011 + the 0.6.2 CHANGELOG that overstated
what shipped. `db_loader='copy'` remains rejected at every public
boundary; the helpers added in 0.6.2 are still dormant production code
exercised only by direct helper tests. Callers using the default
`db_loader='insert'` observe no API or behavioral change.

### Fixed
- **`_PetlRawRowSource` now walks the header-advanced iterator, not the
  petl table.** In 0.6.2 the class held a reference to the table itself,
  which meant `iter_rows()` called `iter(tbl)` a second time -- re-running
  the underlying lazy chain, and for a `db_resource`-backed table
  re-executing the SQL query. The row source now receives the iterator
  that `to_row_source()` already advanced past the header row, so the
  table is walked exactly once. Verified by revert-observe-restore
  against a fake petl table whose `__iter__` increments a counter.
- **One-shot enforcement on `_PetlRawRowSource`, `_PandasRawRowSource`,
  and `_ProjectedRowSource`.** ADR 0011 §Row-source contract requires
  exactly one traversal; the 0.6.2 sources would silently spool a second
  time on a second `iter_rows()` call, which for a real COPY consumer
  would either duplicate work or (worse) run a second COPY against the
  same target. A second call now raises `PipelineContractError` /
  `DbPublishError`.
- **`RowProjection.build()` now rejects colliding column configurations.**
  0.6.2 accepted a `db_contract` mapping two source columns to the same
  target, duplicate framework column names, framework columns whose name
  matched a projected column, and duplicate source columns -- any of
  which would produce a `RowProjection` that silently discarded rows'
  worth of data downstream. All four cases now raise `DbPublishError`
  at build time. The INSERT path had this validation via
  `_apply_db_contract_columns` in `db_values.py`; the row-source path
  now has parity.
- **`RowProjection.__post_init__` invariant: a constant may not coincide
  with a source-backed position.** 0.6.2's constructor accepted a
  `constants` index that pointed at a position whose `source_indices`
  entry was `>= 0`, silently overwriting the source value. Now raises
  `DbPublishInvariantError`.

### Changed
- **CHANGELOG 0.6.2 language downgraded.** The 0.6.2 entry described the
  runner's `_plan_pipeline_output_handling` as "a real consumer" of the
  `DbRowSource` protocol. It is not: the helper branches on the *string*
  `'copy'` in `spec.db_loader`; nothing in `runner.py` today calls
  `to_row_source()`, constructs a `RowProjection`, or hands a
  `_ProjectedRowSource` to a loader. The real producer-consumer chain
  lands with Phase 5 (`db_copy.py`). This entry corrects the record
  without rewriting the 0.6.2 entry itself, per the project rule that
  superseded prose is preserved so the record shows what was believed
  when.
- **ADR 0011 §Verification status** and §Phase 4 amended to match: 0.6.2
  shipped the Phase 3b helpers and the Phase 4 planning skeleton; the
  wired producer-consumer path lands with Phase 5.

### Tests
- **`test_petl_bare_source_second_iter_rows_call_raises`** and
  **`test_pandas_bare_source_second_iter_rows_call_raises`** in
  `tests/test_table_adapters.py` -- replace the 0.6.2
  `test_bare_source_is_one_shot`, which only checked exhaustion, not the
  raise-on-reentry contract.
- **`test_petl_to_row_source_does_not_double_iterate_underlying_table`**
  -- a `FakePetlTable` records `__iter__` call count; asserts it is
  called exactly once across header extraction and full row iteration.
  Verified by revert-observe-restore against the 0.6.2 shape.
- **`Test17cRowSourceProjection`** gains six tests covering the five
  collision cases and the projected-source one-shot contract. All
  verified by revert-observe-restore.
- **`Test17dRunnerCopyBranching`** gains three tests covering the
  fall-through cases where `db_loader='copy'` is set but the pipeline
  does not actually reach the database-only COPY branch (no
  `output_db`, or `output_db` set but no `db_table`).


## 0.6.2

Phase 3b + Phase 4 of ADR 0011, landed together so the row-source
contract has a real consumer from the moment it exists. `db_loader='copy'`
remains rejected at every public boundary -- the new machinery is dormant
production code, exercised by direct helper tests. Callers using the
default `db_loader='insert'` observe no API or behavioral change.

(Note: the "real consumer" claim above and the parallel wording in the
Changed section were corrected in 0.6.3. See the 0.6.3 entry for the
downgrade rationale; the 0.6.2 wording is preserved here so the record
shows what was believed at 0.6.2.)

### Added
- **`DbRowSource`** protocol in `task_core.types` -- level-0,
  engine-neutral, `runtime_checkable`. Yields positional row sequences
  (not dicts) so a future COPY transport can spool them straight to
  PostgreSQL without walking a dict per row. See ADR 0011 §Row-source
  contract.
- **`DbPayload.row_source`** field, appended after every 0.6.0 field to
  preserve positional-construction stability. Only meaningful when
  `db_loader='copy'`; `None` on the insert path.
- **`validate_payload_source_state(loader, rows, row_source)`** in
  `task_core.types`. Enforces the exact ADR state matrix at
  `DbPayload.__post_init__` after `validate_db_loader`. `insert` requires
  materialized rows and forbids a row source; `copy` requires a row
  source and forbids materialized rows.
- **`RowProjection`** (frozen dataclass) + **`_ProjectedRowSource`** in
  `task_core.db_publish`. One transport-neutral mechanism composes
  `db_contract` renaming/projection and framework columns (currently
  just the run-started-at timestamp) into the final logical row shape.
  Timestamp is bound at construction and injected once per row -- not
  recomputed. Framework column position is derived from
  `len(contract_projected_columns)`, not hardcoded to "last".
- **`_PetlAdapter.to_row_source(tbl)`** and
  **`_PandasAdapter.to_row_source(df)`** -- return `(columns_tuple,
  DbRowSource)` yielding bare positional rows. `db_contract` and
  framework columns are added by `_ProjectedRowSource` in the
  orchestrator, not per adapter, so the two engines never accumulate
  duplicated row-shaping semantics.
- **`_plan_pipeline_output_handling`** in `task_core.runner` -- the
  Phase 4 branching helper. On the COPY path, `adapter.nrows()` and
  `adapter.stabilize()` are skipped for the DB consumer alone (both
  would traverse a one-shot bounded-memory source and defeat the
  contract); the row count comes from the publisher after streaming.
  Takes `db_loader` as a parameter so tests can exercise the branch
  directly even though no shipped pipeline can reach `db_loader='copy'`
  in 0.6.2.

### Tests
- **`Test5DbRowSourceProtocolShape`** and **`Test6PayloadSourceStateMatrix`**
  in `tests/test_types.py` -- the protocol is `runtime_checkable`; the
  state matrix rejects both invalid `insert` legs and both invalid
  `copy` legs, and honors `error_type=` for the payload boundary.
- **`Test17cRowSourceProjection`** in `tests/test_db_publish.py` --
  identity, contract, framework composition, framework position
  derivation, timestamp-once-per-run, source-width mismatch, missing
  contract source, immutability, and a **parity test** proving
  `_ProjectedRowSource` output matches the current INSERT path
  (`from_petl` + `apply_db_updated_at`) column-by-column. Parity test
  verified by revert-observe-restore (skipped the framework-column loop;
  test failed on the column-list assertion).
- **`Test17dRunnerCopyBranching`** -- the Phase 4 helper's insert vs
  copy decisions, with and without other consumers (Excel,
  debug_display), and the return type invariant.
- **`Test6AdapterToRowSource`** in `tests/test_table_adapters.py` --
  petl/pandas header extraction, positional row yield, one-shot
  semantics, empty-table rejection, and the `itertuples(index=False,
  name=None)` shape.

### Changed
- **`DbPayload.rows`** typed as `list[dict[str, Any]] | None`. Only
  reachable as `None` for `db_loader='copy'`, which is still rejected
  publicly; insert callers observe no change.
- **Adapter interface** grew a seventh method `to_row_source`, updated
  in `Test1DocumentedApiMatchesTheCode`.
- **ADR 0011 §Verification status** records that 0.6.2 shipped Phase 3b
  and Phase 4.


## 0.6.1

Follow-up patch closing four gaps external review found against 0.6.0.
Existing insert callers observe no API or behavioral change: only an
internal validation moves earlier and tests are added.

### Fixed
- `db_loader` revalidation now runs before staging DDL, not after. 0.6.0
  placed the payload-boundary revalidation immediately before the `LOADERS`
  dispatch, which meant `CREATE TABLE` for the staging table and the
  publisher's transaction had already opened before an invalid loader could
  be rejected. ADR 0011 §Preparation flows step 2 and §Failure semantics
  both require a configuration error to fail before source processing, the
  preparation transaction, or staging DDL; the fix moves the check up to
  sit alongside `validate_publication_strategy`. (`_require_task_lock()`
  still runs earlier -- that is preserved on purpose, so a direct caller
  who has not acquired the lock keeps failing with the lock error rather
  than the loader error.)

### Changed
- ADR 0011 §Implementation sequence Phase 3 amended to record the 3a/3b
  split. 0.6.0 shipped Phase 3a (the `db_loader` configuration surface);
  Phase 3b (the one-shot `DbRowSource` protocol and adapter rewrites) is
  sequenced immediately before Phase 4, which is its first consumer --
  Phase 4's runner redesign consumes the row-source handle. The original
  Phase 3 text is preserved in the amendment.

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
- Review external scripts containing `output_schema=` against the declared-type
  rules above and the current authoring guide. Valid declarations need no
  change. The bundled HR and OPS tasks use inferred schemas and require no
  edits.


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
  The current checklist is in
  [`docs/task-authoring.md`](docs/task-authoring.md#review-existing-declared-schema-scripts).

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
