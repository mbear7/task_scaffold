# Changelog

Notable changes to `task_core`. Versions are the value of
`task_core.__version__`.

The format is loosely [Keep a Changelog](https://keepachangelog.com/).
This file starts at 0.2.11; earlier history is reconstructed below in a
single entry from the previous README, which recorded changes
chronologically rather than by release.


## Unreleased


## 0.2.12

### Added
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
  and `payload.schema`, which is not yet implemented.
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
