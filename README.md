# task_core

A small, mixed-engine (pandas/petl) ETL task scaffold, plus real tasks
built on it.

## Layout

```
task_core/            the scaffold itself -- standalone, depends on no
                       external, ad hoc utility module and no task, ever
  binding.py           resource declaration, bind(), structural validation
  context.py           task_context -- resource caching, results, shared state
  db_publish.py         DB payload construction and publishing -- internal
                        to task_core; nothing outside task_core imports it
  excel_metadata.py    native Excel outline/indent/style XML extraction,
                        and align_row_metadata -- everything Excel-metadata-
                        related lives here, together
  runner.py            run_pipelines() -- the actual execution loop
  resources/            resource construction: Excel (single/multi-file), DB
  ...
tests/                 task_core's own test suite (unittest, no pytest)
tasks/                 every real task, each in its own file, each owning
                       whatever task-specific helper code it needs
  hr_task.py            HR funnel/staff/recruiter reporting
  ops_task.py           ops reporting
  hr_petl_task.py       standalone petl-engine example, parallel to hr_task.py
```

Tasks utilize `task_core`; they are never part of it, at any point --
`tests/test_standalone.py` checks this automatically for `task_core`
and `tests/` themselves. `tasks/` exists as its own package specifically
so this boundary is structural, not just a naming convention, and so it
scales cleanly as more tasks are added.

This isn't just true today with three tasks present -- `task_core` and
`tests/` genuinely don't care whether `tasks/` has anything in it at all.
Verified directly, not just reasoned about: copied only `task_core/` and
`tests/` into three separate, fresh directories -- one with no `tasks/`
present whatsoever, one with an empty `tasks/` directory, one with an
empty `tasks/` *package* (`__init__.py`, still zero task files) -- and
`task_core` imported and the full test suite passed identically in all
three. The underlying reason isn't coincidence: `task_core` has no
reference to `tasks/` anywhere in its own source, and
`tests/test_standalone.py` never inspects `tasks/` at all, only
`task_core/` and `tests/` themselves. Adding or removing a task later
can't put either at risk.

Run a task as a module, from the project root -- not as a standalone
script, which can't find `task_core`:

```
python3 -m tasks.hr_task
```

## task_core is standalone; tasks depend on real, external things

`task_core` itself depends on no external, ad hoc utility module for
anything -- its Excel-metadata extraction is native (`excel_metadata.py`),
including `align_row_metadata`, which lives there too rather than in a
separate generic-utilities file, since it's genuinely part of the same
concern. `etl` is the real `petl` package, imported directly, everywhere.

`db_publish.py` moved inside `task_core/` for the same underlying reason,
once checked rather than assumed: `funnel_pandas.py`, its original other
consumer, was fully replaced by `hr_task.py` earlier in this project, and
grep across every file confirms `task_core`'s own three files
(`runner.py`, `table_adapters.py`, `resources/db.py`) are now its only
importers -- nothing external depends on it being a separate, shared peer
module anymore.

Tasks are different: they're expected to depend on real, external things,
the same way they already depend on `pgcreds`. `tasks/hr_task.py`,
`tasks/ops_task.py`, and `tasks/hr_petl_task.py` all import from
`petl_util` directly (`MONTH_MAP`, `make_cal`/`to_date`,
`table_skip`/`drop_blank_cols` respectively) -- the real one, maintained
separately by the project owner, not shipped in this repository.

This project went through an intermediate state worth being honest about
rather than quietly erasing: at one point, these functions were inlined
directly into each task file, reasoned about at the time as "making
things more reliable by removing the dependency." That conflated two
different things. `task_core` needing to be standalone and reliable is
real and correct, and stays that way. Task files duplicating code that
already has a single, real, maintained source in `petl_util.py` doesn't
make anything more reliable -- it creates drift risk against that real
source instead. A fix to the real `make_cal` would never reach a
duplicated copy sitting in this repo. Reverted once this became clear:
tasks import from `petl_util` again, and nothing in this project
redefines what it already, authoritatively provides.

`excel_resource.get_table()`/`.get_map()` (used throughout `ops_task.py`'s
real pipelines) are verbatim ports of the real, confirmed-correct
`load_table`/`tbl2dict` (supplied directly by the project owner), living
in `task_core/resources/excel.py` -- these are different from the
`petl_util` case: `task_core` itself calls them, so they had to become
`task_core`'s own code, tested against a real Excel Table object -- the
first time this specific functionality has ever been exercised anywhere
in this project's history.

One more thing found only once `hr_petl_task.py` could finally be run:
`read_ssch2_sheet` had the same `first_row = min(metadata) if metadata
else 1` misalignment bug independently found and fixed in `hr_task.py`'s
`read_ssch_sheet` weeks earlier in this project -- confirmed with the same
"genuinely untouched leading row" construction that caught it there, and
fixed the same way (`first_row = 1`).

`hr_petl_task.py`'s `ssch2` is migrated to the resource-binding model too
now -- the last pipeline in this project still on the pre-`binding.py`
pattern (`_files_resource_key`, a hand-rolled `build_context()`). Its own
`run()`/`process_folder()` split was the same bare, one-line delegation
already folded in `hr_task.py`'s `ssch` -- folded here the same way, for
the same reason. Re-verified afterward with the exact construction that
caught the `first_row` bug above, confirming the migration didn't
reintroduce it.

`ops_task.py` is fully migrated to the resource-binding model now too --
all 6 pipelines, `nsi_911`/`mdm`/`tickets_1c`/`ca` sharing one `ops_xlsx`
resource, `db_strat` on the generic DB-shaped `resource()` factory
(stays untracked and stays out of `RUN_SEQUENCE`, matching its own
already-documented state -- no agreed fingerprint query with the strat_db
view owner yet), `cal` needing no binding at all. Run end to end for the
first time in this project's history to verify it, with real, comprehensive
test data covering every named Excel Table these pipelines actually read.

## No run() that only calls process()

`hr_task.py`'s `funnel_closed`, `funnel_open`, `declined_close`,
`declined_open`, and `ssch` each used to have a `run()` that did nothing
but call a separately-named `process()`/`process_folder()` -- a bare,
one-line hop adding nothing, confirmed by checking each one's actual body
rather than assumed from the general shape: `staff` and `recruiters` look
similar but weren't touched, since their `run()` does real work (shared
month-bound publishing/fetching) beyond the process call, not just
delegate to it. For the five that were genuinely bare, `process`/
`process_folder` is now `run` itself -- confirmed each was called from
exactly nowhere else in the file before folding, so nothing else needed
to change. `process()`'s own internal structure (`prepare_base`,
`build_month_scope`, `repair_stage_block`, `collapse_duplicate_groups`)
stayed exactly as separate, named methods -- those are genuinely
reusable, independently meaningful steps, not indirection for its own
sake, and folding `run()` into them wasn't the same kind of question as
folding `process()` into `run()`.

## Running tests

No external test framework -- `pytest` isn't assumed to be available, so
this uses `unittest` (standard library).

```
python3 -m unittest discover -s tests -v
```

`tests/test_binding.py` covers the resource-binding model itself.
`tests/test_standalone.py` statically verifies `task_core` and `tests/`
never import anything task-level -- any file under `tasks/`, or any
future task file. Tasks utilize `task_core`; they are never part of it,
at any point, and this is checked automatically now rather than relying
on nothing having been added by mistake.

Three more, following an external review that identified real,
verified gaps `test_binding.py` alone didn't cover:

`tests/test_context_lifecycle.py` -- the resource lifecycle guarantees
`task_context` itself makes: a resource fingerprinted during source-change
checking is the exact same object later injected into the pipeline that
processes it (the most architecturally central test in this project's
suite -- it's what a lazy, cached resource model is actually for),
close-exactly-once, an inactive resource is never constructed at all, all
resources close even when a pipeline raises, and clear diagnostics for
missing results/shared values/resources rather than raw, unexplained
`KeyError`s.

`tests/test_source_change_runner.py` -- source-change execution paths
(unchanged sources skip execution, `force_run=True` overrides that,
changed sources execute, enabling the check with no tracked sources fails
clearly) and runner transaction atomicity (two DB-output pipelines commit
together; a later pipeline failing rolls back an earlier one's already-
published output too; a publisher and its resources close in every path,
success or failure). These turned out to be the same underlying
mechanism, not two separate ones: source-state update and DB payload
publishing both happen inside the same, single `publisher.commit()` at
the very end of `run_pipelines()`, so one rollback protects both
together -- kept in one file for that reason, not split by which module
happens to implement which half. Exercises `source_state.py`'s real code
(not a parallel reimplementation of its logic) against an in-memory fake
`conn` -- see the file's own docstring for why this needed a small,
sandbox-only extension to the local `sqlalchemy` stub.

`tests/test_excel_metadata.py` -- real, generated `.xlsx` fixtures (mocks
alone can't validate genuinely XML-specific behavior: sparse rows,
outline levels, sheet-name resolution). Includes a permanent regression
test for the exact `first_row` misalignment bug found twice, independently,
through manual verification earlier in this project's history (once in
`hr_task.py`, again in `hr_petl_task.py`) -- captured here so a future
regression is caught automatically instead of needing to be rediscovered
by hand a third time.

Every test added this round was verified to have genuine teeth, not just
asserted to pass: each one was checked against a deliberately broken
version of the real code it protects, confirmed to fail with a clear,
relevant message, then confirmed the code was cleanly restored afterward.

Tests are task_core-only: every pipeline and resource in `tests/` is a
minimal stub built inline. No file under `tasks/` is ever imported by the
test suite -- that's real usage, not test fixtures, and it's intentional
that this project has no persistent test file for any individual task;
verification for those happens by actually running them, in-session, with
real data, not by growing a parallel test suite around each one.

## Dependencies

See `requirements.txt`. `lxml`/`petl`/`smbclient`/`sqlalchemy`/`psycopg2`
are real `task_core` dependencies. `petl_util` itself is not in
`requirements.txt` at all -- it's a real, external, task-level dependency
this project doesn't ship or control, maintained separately by the
project owner, the same way `pgcreds` isn't shipped either. `babel` is
listed because `petl_util` needs it, not because any file in this project
imports it directly.

If your working copy has a `/petl/`, `/sqlalchemy/`, or `/petl_util.py`
sitting at the project root, these are local-only development
workarounds, never part of the real project (`.gitignore` excludes all
three). They exist because the environment this project was actually
built and verified in has no network access to `pip install` real
packages -- `petl`/`sqlalchemy` are local copies of the genuine, real
packages (not reimplementations), and `petl_util.py` is a minimal,
sandbox-only stand-in for the real, external module described above,
sufficient only to run and verify this project's own tasks and tests in
that environment. A normal working copy, with real network access,
should have none of these three -- just `pip install -r requirements.txt`
and, separately, whatever your own real `petl_util.py` actually is.

