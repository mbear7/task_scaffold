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

Also run *tests and interactive debugging* from the project root, never
from inside `task_core/` itself: with the working directory inside the
package, `task_core/types.py` shadows Python's own stdlib `types` module
and the interpreter fails during startup imports with a circular-import
error that looks like package corruption (`cannot import name
'GenericAlias' from partially initialized module 'types'`). Nothing is
corrupt -- just cd back to the project root.

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

## Formatting: one thought, one line

Formatting tracks logical units, not syntax trees. A single logical step
stays on a single line even when that line is long --
`.convert('grade', lambda x: round_half_up(x, 1) if x not in (None, '') else None)`
is one thought, and exploding it across five lines makes the reader
reassemble it while destroying the shape of the pipeline around it (a
petl chain's readability *is* its one-transformation-per-line shape).
Expanded formatting is used only when at least one of these applies:

- the expression has multiple logical steps
- nesting makes it hard to scan
- error handling is involved
- intermediate names materially improve meaning
- the line becomes genuinely difficult to read

The same test governs helper extraction: extract when the name carries
meaning the expression can't, or when there are real multiple call sites
with drift risk -- never merely to shorten a line. A once-called helper
whose body is shorter than its signature is indirection, not clarity.

Scope: this convention binds `tasks/` fully -- dense, pipeline-shaped
code read by someone who knows the domain. `task_core` sits deliberately
further toward the explicit end (infrastructure is read by people
debugging it without context), but the same criteria still apply there;
only the threshold differs. Corollary: no auto-formatter (black etc.)
runs on this repository -- default configurations mechanically produce
exactly the exploded style this convention exists to prevent, and would
win every argument by being automated.

## Cleanup priority: masking a real failure vs. hiding a real one

Found by external review: `task_context.close()` and `run_pipelines()`'s
publisher-close step both *always* logged cleanup failures and never
raised them. That was deliberate, and correctly solved a real problem --
a cleanup failure replacing a genuine pipeline failure -- but it solved
it too broadly. A cleanup failure on an otherwise-*successful* run was
just as silently swallowed as one during a real failure, meaning a task
could report success while genuinely leaking an SMB stream or a DB
connection, forever, invisibly.

This went through two rounds of external review before landing on the
current design, and the first fix's own rejected approach is worth
recording, briefly, precisely because it looked reasonable and wasn't:
it decided whether to log or raise a cleanup failure by checking
`sys.exc_info()` -- whether Python currently has *any* exception being
handled, anywhere up the call stack. The second review found this
unreliable, and confirmed it directly: a caller of `run_pipelines()`
sitting inside its own, unrelated `except:` block (`except ValueError:
run_pipelines(...)`) makes `sys.exc_info()` non-`None` for the call's
entire duration, even when the task itself completes with no error at
all -- a resource cleanup failure during that genuinely-successful task
incorrectly looked like it had something ambient to avoid masking, and
got logged instead of raised, silently hiding a real, leaked resource
anyway. There is no reliable way to infer "does *this task* have a
primary failure" from interpreter state; only `run_pipelines()`'s own
`try`/`except` genuinely knows.

### Explicit tracking, in `run_pipelines()` itself

`run_pipelines()` sets a plain local, `primary_error = None`, and
assigns it inside its own outer `except BaseException as e:` block --
nowhere else, and never inferred from anything ambient. Its `finally:`
block attempts every cleanup step (publisher close, then context close,
via a small local `try_step()` helper that catches and collects rather
than raising immediately) and only then decides what to do with
whatever it collected: if `primary_error is not None`, every collected
cleanup failure is logged, and the function's own `raise` (from the
`except` block above) is what actually propagates -- untouched. If
`primary_error is None`, whatever was collected is raised instead: a
single exception if only one thing failed, an `ExceptionGroup` if more
than one did.

The operational consequence of raising on an otherwise-successful run is
deliberate and safe: if the DB commit already landed and only cleanup
then failed, the task surfaces as failed to its orchestrator -- but the
source state committed in that same transaction, so the orchestrator's
retry finds sources unchanged and *skips*. No double-publish, and the
leaked-resource failure still gets seen instead of swallowed. That
retry-skips property is what makes the aggressive raise policy
operationally safe, and it's load-bearing on purpose.

The `except` clause is `except BaseException`, not `except Exception` --
found by a third round of review: `KeyboardInterrupt`, `SystemExit`, and
`GeneratorExit` are `BaseException` subclasses, not `Exception` ones, so
an `Exception`-scoped clause here never caught them, leaving
`primary_error` at `None` during a genuine interruption -- a cleanup
failure during that interruption then incorrectly looked like the only
failure there was to report, and replaced the interruption itself in
what actually propagated. Confirmed directly before fixing. This does
not affect the inner pipeline-loop wrapper a few lines up, which still
only ever catches `Exception` when deciding whether to wrap a failure
into `PipelineError` -- it already left `KeyboardInterrupt` alone,
correctly, since that wrapper was never in its path either.

### `task_core/cleanup.py`: `attempt_all_cleanup()`

A small, level-1 module holding one function, used by both
`task_context.close()` (its own multi-resource loop) and
`DbPublisher.close()` (its own connection/engine steps, below). It takes
no suppress/log parameter of any kind: every item it's given is
attempted regardless of an earlier one failing, and it always raises at
the end if anything failed -- a single exception, or an `ExceptionGroup`
for more than one. The decision of whether to let that propagate or
catch and log it belongs entirely to whichever caller actually has a
primary-failure signal to consult, which is why `task_context.close()`
itself, called on its own, always raises a genuine cleanup failure too
(`run_pipelines()` is the one that catches this and decides, via
`try_step()`, described above). Deduplication by object identity --
the same resource object cached under two different loader keys is
closed exactly once, not twice -- happens in `context.py`'s own
`close()`, building the list it hands to `attempt_all_cleanup()`, not
inside `attempt_all_cleanup()` itself.

Lives in its own module rather than inside `context.py` or `runner.py`
directly: `runner.py` has an existing, deliberate boundary (it
duck-types `ctx`, and only imports `context.py` under `TYPE_CHECKING`,
never at runtime) that putting this logic in `context.py` would have
broken just to share it.

### `DbPublisher.close()` isolates its own two steps

Found by external review, confirmed directly: `close()` used to call
`self._conn.close()` then `self._engine.dispose()` as two unguarded,
sequential statements. If the connection failed to close, engine
disposal was never even attempted -- and since `run_pipelines()` has
already made its one cleanup attempt at the publisher level by the time
this runs, there was no retry, a genuine, permanent leak of the engine's
connection pool. `close()` now treats these as two independent steps
through `attempt_all_cleanup()`, the same helper `task_context.close()`
uses, rather than a third, separate implementation of the same pattern.

### Diagnostics: cleanup logs don't duplicate the primary traceback

A cleanup exception collected in `run_pipelines()`'s `finally:` block is
caught while `primary_error` is already the active exception, so Python
automatically chains it as that cleanup exception's `__context__` --
and since `primary_error` is already logged separately, every cleanup
log entry was repeating the entire primary traceback too. Found by
external review; not a correctness bug, but a real diagnostics-noise
one. Fixed by setting `__suppress_context__ = True` on each cleanup
exception before logging it -- confirmed directly, not assumed, that
this is respected by `logging`'s own `exc_info=(...)` formatting, the
same mechanism `raise ... from None` uses for uncaught tracebacks. This
mutates the exception object permanently, not scoped to that one log
call in any literal sense (a fair correction from a further review round
to this section's own, earlier wording) -- in practice this has no real
consequence beyond the logging itself, since every exception this
touches is discarded once `run_pipelines()` returns or raises, never
reused or re-raised as itself anywhere else.

That fix covered a single cleanup exception correctly, but not a
*grouped* one: a further review round found that `__suppress_context__`
set only on the outer `BaseExceptionGroup`/`ExceptionGroup` left every
exception nested inside it still showing its own, full `__context__`
chain back to `primary_error` -- confirmed directly, with two resources
each failing to close, that the second one's traceback in the log still
carried the complete primary traceback with it. Fixed with a small,
recursive helper (`_suppress_context_recursively()`) that walks into
`.exceptions` for any nested group, arbitrarily deep, rather than a
single, top-level-only assignment.

### Cleanup itself needed the same `BaseException` treatment

Widening `run_pipelines()`'s own outer boundary to `BaseException`
(above) protects a `KeyboardInterrupt`/`SystemExit` raised by the
*pipeline* -- but a further review round found the two places that
actually perform cleanup, `cleanup.py`'s `attempt_all_cleanup()` and
`run_pipelines()`'s own `try_step()`, both still caught only `Exception`.
An interruption raised *during a resource's own `close()`* wasn't
protected at all: it could still stop every subsequent resource from
getting its own close attempt, and still replace whatever primary
pipeline failure was already propagating -- confirmed directly, with a
`RuntimeError` pipeline failure and a `KeyboardInterrupt` from one
resource's `close()`, that the `KeyboardInterrupt` both propagated in
place of the real failure and pre-empted a second resource's own cleanup
entirely.

Both now catch `BaseException`. The one further wrinkle: constructing
`ExceptionGroup` directly with a genuine `BaseException`-only member
(`KeyboardInterrupt` etc.) raises `TypeError` -- confirmed directly --
so every group-construction site uses `BaseExceptionGroup` instead. This
isn't a compromise: `BaseExceptionGroup` automatically becomes a plain
`ExceptionGroup` instance when every member it's given happens to be an
`Exception` subclass (confirmed directly, not assumed), so ordinary,
all-`Exception` multi-resource cleanup failures -- the common case --
are entirely unaffected and remain catchable via `except ExceptionGroup:`
specifically, not just the broader `BaseExceptionGroup`.

Every claim above -- explicit tracking over interpreter-state inference,
`attempt_all_cleanup()`'s actual no-suppress-parameter shape, `close()`'s
step isolation, `BaseException` handling in both the outer boundary and
cleanup itself, recursive traceback suppression, and the
`BaseExceptionGroup`/`ExceptionGroup` auto-downcast this all depends on
-- is covered by a test in `tests/test_cleanup.py` or
`tests/test_db_publish.py` that was checked against a deliberately
reverted version of the real fix and confirmed to fail, not just written
and trusted.

## NaN in Excel export: a real fix that silently regressed once already

`normalize_for_excel()` (`table_adapters.py`) rejects a genuine NaN the
same way it rejects a tz-aware datetime: writing NaN as-is produces
structurally malformed worksheet XML -- a numeric-typed cell with an
empty `<v></v>` (the petl adapter's `toxlsx()`), or a string-typed cell
with no content at all (the pandas adapter's `to_excel()`) -- confirmed
directly by reading the raw XML both adapters produce, not by trusting
what comes back through `openpyxl.load_workbook()`, which is lenient
enough on read to silently paper over both shapes as a clean `None`
either way. That leniency on read is exactly why the malformation went
unnoticed in the first place, and exactly why it isn't a reliable signal
that real, desktop Excel would open the file cleanly without a repair
prompt.

This fix was made, verified by hand, and then quietly lost: a later
rebuild of this project started from a point before the fix existed,
and nothing caught the loss, because the fix had never been captured as
a persistent, automated test -- only checked once, by hand, in the
session where it was made. `tests/test_table_adapters.py` (previously
nonexistent -- `table_adapters.py` had no test coverage of any kind
before this) exists specifically to close that gap: every test that
touches Excel export reads the real, on-disk worksheet XML out of the
`.xlsx` file's own zip archive directly, the same way the original
verification did, not an in-memory table/DataFrame value and not a
value read back through `openpyxl` -- because an in-memory check is
exactly the kind of check that wouldn't have caught either the original
bug or its later regression.

Worth being direct about a limitation found while building that
coverage, not smoothed over: the pandas adapter has a second, deeper
issue this fix does not resolve. `pandas.DataFrame.to_excel()` writes
that same structurally malformed cell for *any* missing value by
default (`na_rep=''`), confirmed directly with a completely NaN-free,
pre-existing `None` -- this fix cannot make the pandas adapter's Excel
output as clean as the petl adapter's; fixing that would mean bypassing
pandas's own native Excel writer entirely, well outside this fix's
scope. What this fix guarantees for the pandas adapter, and what
`tests/test_table_adapters.py` holds it to specifically: NaN must not be
*worse* than a genuine, pre-existing `None` would already be -- the two
must produce byte-identical XML, which they do.

A second thing worth being equally direct about, found only by proving
that new coverage has genuine teeth, not assumed: the pandas adapter's
own numeric/datetime-dtype column-conversion loop (separate from
`normalize_for_excel()` itself) currently changes nothing observable --
confirmed directly that `pandas.to_excel()` already treats an untouched,
native NaN identically to a pre-converted `None`, with or without that
loop. It's kept anyway, as defensive, forward-looking code against a
future pandas version changing that equivalence -- not because the
tests currently prove it does anything today. Recording this here so it
isn't mistaken for proven, tested behavior it isn't.

### `pd.NA` specifically broke the same check in both places

`normalize_for_excel()` and `_normalize_value()` (`db_publish.py`) both
used a bare `value != value` check to detect a missing value -- correct
for a plain NaN, but genuinely wrong for `pd.NA` specifically: `pd.NA !=
pd.NA` doesn't return `True` the way a NaN comparison does, it raises
`TypeError` ("boolean value of NA is ambiguous"), confirmed directly.
Both callers wrapped that check in a broad `except Exception: pass`,
which silently swallowed the `TypeError` and let a raw `pd.NA` fall
through *unconverted* -- neither function ever actually turned it into
`None`. Found during an optimization review flagging a stale-looking
`pd.NA` comment; tracing it precisely turned up a real, live bug, not
a documentation nit -- confirmed directly against both functions in
isolation, since a prior "verification" of `_normalize_value()` only
ever tested it through `from_pandas()`'s full path, which has an
earlier `.astype(object).where(...)` step that already converts `pd.NA`
to `None` before `_normalize_value()` ever sees it. Testing only through
that path meant the gap in `_normalize_value()` itself was never
actually exercised.

`is_missing()` (`db_publish.py`) is the shared fix, used by both --
`table_adapters.py` already imports `from_pandas`/`from_petl` from
`db_publish.py`, so this follows that same, existing dependency
direction rather than introducing a new one. `pd.isna()` isn't a fully
safe drop-in for the bare comparison on its own, though: for a
multi-element list/array-like value it returns an array rather than a
scalar, which itself raises inside a plain `if`/`bool()` for anything
but a single-element array. `is_missing()` handles this the same way
both callers already handled the original check -- a broad `except
Exception: pass` -- verified directly against every case this needs to
distinguish: `pd.NA`, `np.nan`, `None`, `pd.NaT`, ordinary scalars, and
multi-/single-/zero-element lists.

## Possible future enhancement: streaming DB rows instead of materializing every row dict

Not implemented, deliberately -- recorded here as a considered, understood
option, not a task in progress. The original optimization proposal's own
stated condition for picking this up: only after stage timings or memory
measurements show payload construction is genuinely material, not
speculatively.

Current shape: `DbPayload.rows` is a concrete `list[dict]`, built fully
before `DbPublisher.publish()` ever slices it into chunks via
`_chunked(payload.rows, chunk_size)`. For a large table, memory can
simultaneously hold the original pandas DataFrame or cached petl rows,
plus this full second representation as row dicts, plus each temporary
chunk list carved out of it.

The proposed shape: a payload carrying columns, a small sample of rows
for type inference, and a single-use chunk iterator instead of a fully
materialized list -- buffer enough rows to infer types and create the
table, then normalize and insert the rest chunk by chunk, counting rows
while inserting rather than via a separate pass. `db_contract` and
`db_updated_at` can both still be applied per row under this shape.

Two things worth recording now, found while first evaluating this, that
a future implementation would need to resolve, not just the change
itself:

- If `db_contract`/`db_type_overrides` is already provided, there's no
  need to buffer any rows for type inference at all -- a streaming
  implementation needs a genuine fork in behavior here, not a
  buffer-then-stream version applied uniformly regardless of whether
  inference is even needed.
- A one-shot iterator can't be rewound. If a publish fails partway
  through, the current materialized-list version could, in principle,
  support a retry without re-fetching from the source; a streaming
  version could not. Not a reason to avoid this by itself, but a real
  capability the current design still has open that this would close.

PostgreSQL `COPY` is a further, separate step worth naming but not
pursuing yet either -- likely to outperform generic SQLAlchemy
`executemany` for very large tables, but brings its own
PostgreSQL-specific serialization and error-handling complexity.
Streaming chunks would be the reasonable first step if this is ever
picked up; `COPY` only after that, and only if actual load times justify
the added complexity.

## Repeated traversal of a pipeline's output: a correctness risk, not just a performance one

After `pipeline_cls.run(ctx)` returns, `run_pipelines()` traverses the
result separately for `nrows()` (always), then potentially `display()`,
`to_excel()`, and `to_db_payload()` -- and, when `publish_result` is
set, a *downstream* pipeline may later call `ctx.get_result()` and
traverse what it gets back. `ctx.set_result()` itself, confirmed
directly, only stores the object (`self._results[name] = tbl`, nothing
else) -- it's the later `get_result()` call and whatever a downstream
pipeline does with what it returns that creates the repeated-traversal
risk `publish_result` is included in the stabilization check for, not
`set_result()` itself doing any traversal of its own. For
a lazy petl transformation chain, each traversal re-runs the entire
chain from scratch -- confirmed directly, not assumed, with a counting
transform. For a `db_resource`-backed table specifically this is worse
than a performance cost: `petl`'s own `DbView` re-executes its
underlying SQL query on every traversal, confirmed directly by tracing
`DbView.__iter__` down to `_iter_dbapi_connection`, which opens a fresh
cursor and reissues the query from scratch each time. A changing source
table could therefore produce a different row count from `nrows()` than
whatever ends up published -- silently, with no error at all.

`adapter.stabilize(tbl, repeated)` addresses this: for the petl adapter,
wraps `tbl` in `etl.cache()` when `repeated` is `True`, which
materializes rows into memory as they're first requested via iteration
and serves every later traversal from that cache instead of re-running
whatever produced them -- confirmed transparent to every caller here,
none of which know or care whether the table they're iterating is
cached. For the pandas adapter, always returns the DataFrame unchanged
-- a DataFrame is already fully materialized in memory, never a lazy
chain the way a petl table can be, and `validate()` already enforces
that `out_tbl` is a genuine `pd.DataFrame`, not some lazier
pandas-adjacent object that might need more.

`run_pipelines()` calls `stabilize()` -- when `publish_result`,
`debug_display`, an Excel export, or a DB publish means something
beyond the always-run `nrows()` will also traverse `out_tbl` -- and it
does so *before* calling `nrows()`, not after. This ordering isn't a
style choice: confirmed directly that `stabilize()` has zero effect
unless it runs before the first traversal, since that first traversal
is what populates whatever caching it applies. Wrapping a table in
`etl.cache()` after `nrows()` has already consumed it as an ordinary,
uncached traversal leaves every later traversal still fully re-running
-- proven with a dedicated test that exercises `run_pipelines()`'s own
sequencing specifically, not just `stabilize()` in isolation, since a
future change to that ordering wouldn't be caught by a test that calls
the adapter methods directly in the correct order itself.

### A real memory trade-off, not a defect

For DB-only pipelines specifically, `stabilize()` currently creates two
complete in-memory representations: the `CacheView`'s own materialized
rows, plus `to_db_payload()` building a full, separate `list[dict]` from
what the cache already holds. Found by external review, independently
reproduced with the same 100,000-row, one-column synthetic table, not
merely trusted: without `stabilize()`, 200,000 transformation calls,
~1.6s elapsed, ~19MB peak traced memory; with it, 100,000 calls, ~1.1s
elapsed, ~25MB peak -- roughly a 30% memory increase for half the
transformation work and a meaningfully faster run. The trade is real,
and likely grows with wider rows, since the review's own numbers and
this reproduction's independent numbers agree closely on both sides of
that trade.

This is accepted, deliberately, as a genuine cost of the correctness
guarantee `stabilize()` exists for -- not something to remove, especially
for `db_resource`, where an unstabilized second traversal can silently
return different rows than the first. A narrower refinement remains
possible for the specific case where a DB publish is the *only* thing
beyond `nrows()` that would traverse `out_tbl`: build the payload first
and use its own `len(payload.rows)` instead of a separate `nrows()`
call, needing neither a second traversal nor a petl cache at all for
that one case. Not implemented here -- worth a deliberate decision on
its own, not folded into what was otherwise a straightforward defect
fix, the same way `db_publish.py`'s own streaming-rows question (above)
was intentionally left for a measured reason to revisit it, rather than
spliced in speculatively.

## Two regressions from Excel metadata becoming lazy, found by external review

**Overlapping workbook opens.** `get_table()`/`get_map()` called
`self._ensure_workbook()` -- opening and retaining the main workbook for
the resource's whole lifetime -- *before* evaluating `self.tables[name]`,
which on first access triggers `xlsx_info()`'s own, completely separate
workbook open. Before `.tables` became lazy, metadata loading always
fully opened, closed, and ran its own `gc.collect()` *before* the
retained workbook was ever opened -- sequential, never overlapping.
Confirmed directly, not merely inferred from reading the two call
sites: a `source_access` that raises if `xlsx_info()` runs while a
retained workbook is still open genuinely fired. Fixed by evaluating
`self.tables[name]` first, restoring the original ordering -- metadata
loading completes, closes, and collects before the retained workbook
ever opens, exactly as it did before laziness was introduced, with only
*when* that metadata load happens deferred, never *how* it happens
relative to the rest of the resource's lifecycle.

**`excel_resource`'s constructor stopped accepting `sheets`/`tables`.**
`excel_resource` is genuinely exported from `task_core`'s own public
facade (`from task_core import excel_resource` works), not merely an
internal implementation detail -- confirmed directly before treating
this as worth fixing rather than only documenting as an intentional
break. The pre-existing constructor accepted `sheets`/`tables` as
required positional arguments; making them lazy silently dropped both
from the signature entirely, breaking any caller providing them
directly with a `TypeError`. Restored as optional, defaulting to `None`
-- the same "not yet loaded" sentinel already used internally -- so a
caller providing both directly still works exactly as before, and
short-circuits the lazy load entirely rather than silently discarding
what was explicitly given.

**`gc.collect()` now runs on every path through `xlsx_info()`, not just
success.** Confirmed directly: a genuine production need, not
speculative caution -- `wb.close()` and the surrounding context managers
alone were not enough to reliably release XLSX/ZIP handles on SMB/DFS,
and `gc.collect()` is what actually resolves the openpyxl object cycles
responsible. It stays exactly where it was, after `open_binary()`'s own
context has exited, not moved into the inner workbook-close `finally:`
-- deliberately, so it runs once the workbook is closed *and* the
underlying stream has itself also fully exited, not while either could
still be holding a reference. What changed: the whole workbook-open
block is now wrapped in its own outer `try`/`finally`, so `gc.collect()`
runs whether metadata loading succeeds or fails -- confirmed directly
that a failure (`load_workbook()` itself raising, or the table-metadata
comprehension after it) previously skipped `gc.collect()` entirely,
leaving exactly the same stuck-handle risk unaddressed on the one path
where it's hardest to notice, since the caller's attention is on the
exception, not on whether cleanup afterward actually ran.

## Excel metadata is genuinely lazy now

`build_excel_resource()` used to call `xlsx_info()` unconditionally --
with `tables=True`, its own default, the full scan across every sheet
for named Excel Table definitions -- during resource construction,
before any skip decision could be made. `collect_source_fingerprints()`
deliberately reuses `get_resource()` rather than a throwaway instance
(so a task that does end up running doesn't reopen the workbook a
second time), which meant that same eager scan ran during every
fingerprint collection too -- confirmed directly -- even for a task
about to be skipped entirely because its source hadn't changed. And
`source_fingerprint()` itself, confirmed directly, never reads `.sheets`
or `.tables` at all -- only pre-captured selection metadata (path, size,
modification time) gathered before `xlsx_info()` would ever need to run.

`.sheets` and `.tables` are lazy properties now, populated together by
one shared `xlsx_info()` call the first time either is accessed --
confirmed directly this is fully transparent to every existing caller,
including `hr_task.py`'s own direct `.sheets` access in two places,
which needed no changes at all. Building a resource, or fingerprinting
it for a skip decision, no longer opens the workbook at all -- confirmed
directly, not merely by absence of an error, with a real file_access
subclass that counts `xlsx_info()` calls: zero during construction and
fingerprinting, exactly one the first time `.sheets` or `.tables` is
actually accessed, and that same one call serves both regardless of
which is accessed first or how many times either is accessed again
afterward.

Sharing one lazy load between `.sheets` and `.tables` -- rather than two
independent ones, each triggering its own, separately-scoped `xlsx_info()`
call -- was a deliberate choice, not the only option considered. Two
independent properties would let a `.sheets`-only access (like
`hr_task.py`'s own two call sites) trigger a cheaper `tables=False`
open. But real business cases were confirmed to need both `.sheets` and
`.tables` from the same source file in the same run, and splitting them
would mean a second, separate workbook open the first time whichever
property is accessed second -- worse than today's single eager open for
exactly the tasks that need both. Sharing one load preserves today's
guarantee (one open, both available) exactly as it was, deferring only
*when* that one open happens, not whether it happens -- the smaller,
`.sheets`-only win was given up deliberately in favor of not risking a
real regression for a case already confirmed to exist.

## Three smaller improvements from the original optimization proposal

**`read_excel_row_metadata()` is cached by `(sheet, mode, column)` now,**
for both `excel_resource` and `file_set_resource` (keyed additionally by
`selected_file` for the latter -- confirmed directly that's a genuinely
hashable, frozen dataclass, safe as a dict key). This reverses what the
method's own comment had explicitly said before: "call once per sheet
and cache the result if the pipeline needs it more than once" --
deliberately placing that burden on the caller, a real, documented
design choice, not an oversight. Overridden anyway: the resource
represents one immutable selected workbook, so the same key always
means the same answer, and caching internally removes a real, easy-to-
forget cost -- opening its own handle via `open_binary()`, independent
of whatever handle the resource may already be holding open, is a
second full network read on every single call otherwise, not just the
first.

**`get_sheet_rows()` and `get_sheet_raw_rows()` share one materialization
now.** Confirmed directly both previously called the identical
`list(ws.values)` independently, into two separate caches
(`_sheet_cache`, `_raw_cache`) -- genuinely reading and storing the same
sheet's rows twice if both were ever used for the same sheet.
`get_sheet_rows()` now calls `get_sheet_raw_rows()` itself rather than
reading the worksheet a second time, confirmed directly this holds even
when only `get_sheet_rows()` is ever called (`_raw_cache` gets populated
either way), and that both methods' return values are built from the
exact same underlying list object, not merely equal ones. The cache key
for `get_sheet_rows()` also drops `header` -- `header=True/False` was
already a confirmed no-op (a petl table's first row is always its
header via `etl.header()`/`etl.data()`, regardless of the argument), so
the two were already producing identical values as two separate,
non-identical cache entries; now genuinely one, with repeated calls
returning the same object, not just an equal one.

**`db_resource.get_table()` has an opt-in `server_side_cursor` parameter
now,** for a large PostgreSQL read the plain `etl.fromdb(conn, sql)`
path otherwise buffers entirely client-side. Defaults to the existing
behavior, completely unchanged, confirmed directly (no named-cursor
calls at all on the default path) -- this changes real client/server
memory behavior for a large read, not something to silently opt every
caller into. Uses a named psycopg2 cursor (a fresh `uuid4` per call, not
a fixed name, since a named cursor must be unique within its own
transaction) with `itersize` controlling how many rows fetch per server
round-trip, confirmed directly `itersize` is genuinely set before
`execute()` runs, not merely assigned to the attribute at some point.
`petl`'s own `_iter_dbapi_cursor()` (`petl/io/db.py`) already
anticipates this directly -- its own comment mentions server-side
cursors by name -- and calls `cursor.execute(sql)` itself, so this hands
it an unexecuted, named cursor rather than executing first.

Worth being explicit about a real caveat, not just a footnote: `petl`
itself logs a warning here ("using a DB-API cursor with `fromdb()` is
not recommended..."), confirmed directly it genuinely fires -- a
cursor, unlike a connection, can only be iterated once, its result set
exhausted after the first full traversal. `run_pipelines()`'s own
`stabilize()` (see "Repeated traversal of a pipeline's output" above)
already protects any pipeline output *returned* from `run()` against
repeated traversal, but only from that point on -- it does not protect
a table this resource hands back that a pipeline then traverses more
than once *within its own `run()`*, before ever returning anything.
That case needs its own, explicit `stabilize()`/`list()` if a pipeline
genuinely needs more than one pass over a server-side-cursor result.

`resources/db.py` had no persistent test coverage of any kind before
this -- `tests/test_db_resource.py` is new, verified against a complete
DB-API 2.0 cursor fake (`execute`, `executemany`, `fetchone`,
`fetchmany`, `fetchall`) -- confirmed directly that `petl`'s own
duck-typing rejects a partial fake outright, not silently accepts one,
so a fake missing any of these wouldn't actually exercise this code
path at all.

## Four findings from a further round of external review

**`is_missing()` still corrupted one-element containers.** `bool()` on a
multi-element array raises (the existing `except Exception: pass`
fallback catches this correctly), but `bool()` on a *single-element*
array succeeds rather than raising -- so `is_missing([None])` silently
returned `True`, and both `_normalize_value([None])` and
`normalize_for_excel([None])` collapsed a genuine, non-missing list down
to a bare `None`. Confirmed directly before fixing. A container is
never itself "the missing marker" regardless of its own size, only ever
something that might hold missing values inside it -- a separate
question `is_missing()` was never meant to answer. Fixed with
`pd.api.types.is_scalar()`, checking `pd.isna()`'s own result is
genuinely a single boolean before trusting it at all. Worth being
honest about directly: this function's own docstring had claimed
"verified directly against every case this needs to distinguish" --
that verification was itself incomplete, having tested `[5]` and
multi-/empty-element lists, but never a one-element container actually
holding a missing value, which is the one shape that slips past
`bool()`'s own leniency. The docstring says so now, rather than quietly
correct it without comment.

**Workbook overlap was only partially fixed the first time.** The
earlier fix reordered `get_table()`/`get_map()`'s own two internal
steps (`.tables[name]` before `_ensure_workbook()`), which correctly
prevented those two methods from triggering the overlap *themselves*.
It didn't protect against `_ensure_workbook()` already having been
triggered by a *different*, earlier method call -- `get_sheet_rows()`
retaining the workbook first, then `get_table()` triggering `.tables`'
own, separate `xlsx_info()` open on top of it, still overlapped.
Confirmed directly with the same `source_access` that rejects
`xlsx_info()` while a retained workbook is active. Fixed by moving the
ordering guarantee into `_ensure_workbook()` itself -- the one, shared
choke point every method that needs the retained workbook goes through
-- which protects every call path, not just the two originally touched.
Since that made the earlier, narrower fix in `get_table()`/`get_map()`
genuinely redundant, those were simplified back to their original,
cleaner form rather than left in place as a misleading vestige
suggesting the ordering still mattered there specifically; reverting
the centralized fix and confirming all five tests in that class fail
(not just the two originally covering `get_table()`/`get_map()`)
confirmed the simplification was safe.

**The server-side cursor implementation used the wrong `petl` form.**
Passing a single, already-created named cursor directly to
`etl.fromdb()` meant the resulting `DbView` held exactly one, one-shot
cursor for its whole lifetime -- a named cursor can only be iterated
once, so any repeated traversal of that same `DbView` (including two
different callers landing on the same cache entry) either silently got
an empty result the second time, or would be sharing the exact same
cursor object. `petl`'s own `DbView.__iter__` (`petl/io/db.py`) already
has a distinct dispatch branch for a *callable*, confirmed directly
reading its source: it calls the callable fresh on every traversal to
get a brand-new cursor each time, and explicitly closes that cursor
afterward. This also means `petl`'s own "using a DB-API cursor with
`fromdb()` is not recommended" warning never fires for this branch at
all -- confirmed directly that warning is specific to the direct-cursor
dispatch this replaces, not this one. `itersize` is now also part of
the cache key when `server_side_cursor=True` -- a caller explicitly
requesting a different `itersize` is a deliberate performance request,
and silently overriding it because another caller requested the same
query first was a genuine surprise, not a harmless cache hit; folded to
`None` in the key when `server_side_cursor=False`, so the original
reasoning (`itersize` is irrelevant there) still correctly holds for the
case it actually applies to.

Worth recording plainly: the first version of this section's own test
for "no `petl` warning fires" was itself silently broken. `petl` emits
that warning via `logger.warning()` (`petl/io/db.py`'s own
`logging.getLogger(__name__)`), not Python's `warnings` module, so
`warnings.catch_warnings()` never actually intercepted it, regardless of
which implementation ran underneath -- the test passed vacuously either
way. Found only by reverting the fix to prove the test had teeth, and
watching it pass when it should have failed. Fixed with `assertNoLogs`
against `petl`'s own, exact logger name (`'petl.io.db'`), confirmed
directly this genuinely fails against the reverted implementation before
trusting it.

**`get_sheet_rows()`/`get_sheet_raw_rows()` sharing one materialization
introduced a real, if narrow, mutability regression.** Once both were
built from the exact same underlying list object, mutating what
`get_sheet_raw_rows()` returned (`raw[1] = (9, 9)`) silently changed
`get_sheet_rows()`'s own, already-cached table too -- confirmed
directly. Before the original shared-materialization fix, the two
methods had completely independent materializations, so this was never
possible; sharing one introduced a real behavioral change that hadn't
been there before. Fixed by separating the *internal*, canonical
materialization (`_raw_rows_for_sheet()`, never exposed directly) from
`get_sheet_raw_rows()`'s own *public* return value, which now returns a
fresh, independent copy on every call. The underlying `list(ws.values)`
read still only happens once per sheet -- confirmed directly via the
internal cache object's own identity staying constant across repeated
calls -- only the copy on the way out is new, a cheap, shallow `list()`
copy, negligible next to the actual costs (SMB reads, workbook opens)
this project's caching has been about throughout.

## Four more findings, and why each was re-verified rather than trusted

Worth recording plainly before the findings themselves: three of these
four fixes, and their tests, were already sitting correctly in the
working directory when this round started -- but had never made it into
a delivered package, because the working directory and what got packaged
were never reconciled against each other.

That is the incident behind the packaging rule this project now follows:
after building the zip, extract it fresh into a separate, clean location
and `diff -r` it against the working directory before treating it as
delivered. A successful `zip` exit code is not the confirmation; zero
`diff` output is. A fix that exists only locally is not a fix.

Each finding below was re-verified from scratch here regardless --
reverted, confirmed the relevant test fails for the right reason, then
restored -- on the same footing as if it had never been checked before,
rather than trusted merely because it was already present in the tree.

**`_normalize_value()` still scalarized some non-scalar numpy/pandas
containers, through a different mechanism than `is_missing()`'s own
fix.** `is_missing()` was already correct, but `_normalize_value()` has
its own, separate duck-typed conversion logic right after that check --
`to_pydatetime()`/`.item()` -- that still silently collapsed a
one-element container down to its sole element. `numpy`'s own `.item()`
is genuinely designed to do exactly that for an array of size 1
(confirmed directly: `np.array([5]).item()` succeeds and returns the
plain `int` `5`, silently discarding the array itself), raising only for
anything larger, which the existing `except Exception: pass` already
caught. Fixed with the same `pd.api.types.is_scalar()` idea as
`is_missing()`'s own fix, applied one level earlier: stopping the whole
conversion block for anything that isn't genuinely a scalar in the first
place, confirmed directly this doesn't stop a genuine numpy/pandas
scalar (`np.int64`, `pd.Timestamp`) from still correctly normalizing.

**`read_excel_row_metadata()` returned its mutable cached dictionary
directly, in both `excel_resource` and `file_set_resource`.** The same
class of regression already found and fixed for `get_sheet_raw_rows()`
above, here too: a caller mutating what this returned would silently
corrupt the cached answer for a later call with the same key. Fixed the
same way -- returns a fresh, shallow `dict()` copy on every call now,
confirmed directly the underlying read still only happens once per key.

**`excel_resource.close()` did not clear `_row_metadata_cache`.** Added
alongside the resource's other five caches, all cleared together in
`close()`'s own `finally:` block.

**In `xlsx_info()`, `del wb` was skipped if `wb.close()` itself
raised.** Both statements sat in the same `finally:` block as two
separate lines -- an exception from the first (confirmed directly, not
assumed, as a general Python semantic in isolation first) skips the
rest of that same block, so `del wb` never ran. That left `wb` as a
live, reachable local variable at the exact point the outer
`gc.collect()` ran afterward, on precisely the path (a failing
`close()`) where cleanup is hardest to notice. Read alongside "What
`del wb` does and does not guarantee" below, which qualifies this: the
`del` removes the local, but on a raising `close()` the live traceback
still holds the workbook through the failing method's own frame, so this
is a real structural improvement rather than a guarantee that the object
becomes collectible there. Fixed by giving `wb.close()` its own,
inner `try`/`finally` so `del wb` is unconditional. Verified via direct
frame introspection at `gc.collect()`'s own call site, not a
weakref-based check -- a real `openpyxl` `Workbook` likely has its own
internal reference cycles (confirmed directly this project needed
`gc.collect()` at all specifically because ordinary refcounting alone
wasn't enough), so a weakref could still report the object alive for an
unrelated reason regardless of whether `del wb` specifically ran.

## Sampled DB type inference silently narrowed a column, and PostgreSQL hid it

`_infer_column_type()` (`db_publish.py`) infers a column's SQL type from
the first `type_infer_sample_size` rows -- 5000 by default. Confirmed
directly, not reasoned about: a column whose first 5000 rows are ints and
whose row 5001 is `3.5` infers `BigInteger`, where a full scan infers
`Numeric`. The payload is already fully materialized as `list[dict]` by
the time this runs, so the remaining rows were sitting right there; the
sample was buying CPU time, not a streaming guarantee.

For most narrowings that is merely a loud failure: `'N/A'` or `True` into
a `bigint` column both error at insert time, the transaction rolls back,
and the task fails visibly. For exactly two of them PostgreSQL applies an
*assignment cast* instead and accepts the row -- confirmed directly
against a real PostgreSQL instance by the project owner, not assumed from
documentation:

```sql
create temp table t (v bigint);
insert into t values (3.5);                    -- stores 4. No error.

create temp table d (v date);
insert into d values (timestamp '2024-01-01 13:30');
                                               -- stores 2024-01-01.
                                               -- The time is gone.
```

Both are data-dependent, appear only once a table grows past the sample,
and leave the task reporting success with a correct row count. Nothing in
the log distinguishes a rounded column from a correct one. The
`date`/`datetime` case was not part of the original finding at all -- it
turned up only while enumerating which inferred types could be widened
without complaint, and is the same bug wearing different clothes.

### Why not simply scan every row

Measured before deciding, on a 1,000,000-row, 20-column table (12 text,
4 int, 2 numeric, 2 date/time), which is the shape these tasks actually
publish:

| approach | cost |
| --- | --- |
| sample 5000 only (the buggy original) | ~35ms |
| scan every row | ~7.2s |
| **sample 5000, then verify (shipped)** | **~1.2s** |

A ~200x penalty on every publish, forever, to correct something the
sample already gets right in the overwhelming majority of cases, is not a
reasonable default -- and it was the first proposal here, rejected on
these numbers rather than on taste. The 5000-row threshold was never the
defect. What was wrong is what happened when the threshold turned out to
be wrong.

A third approach was also built and measured rather than dismissed: one
pass over `rows` accumulating families for every column at once, retiring
each column as soon as its answer can no longer change (`text` is
absorbing -- verified exhaustively across all 127 non-empty subsets of
the seven families that every set containing `text` resolves to `Text`).
That reached ~2.5s -- still worse than verification, and a substantially
larger change to a function every publish depends on. Not pursued.

### What ships

The sample stays exactly as it was and remains the inference window.
After it produces an answer, and *only* when that answer is one of the
two silently-widenable types (`BigInteger`, `Date`), the remaining rows
are swept with a bare `type(value) is int` / `type(value) is date` check
-- no `_value_family()` match statement, no family set. If nothing
violates the sampled answer, it stands. If something does, the column is
re-inferred across every row, which is the rare path.

`type(...) is`, not `isinstance(...)`, and exactness matters in both
directions: `bool` is a subclass of `int`, and `datetime` is a subclass of
`date`. An `isinstance`-based check would wave a `datetime` through as
consistent with a `Date` column -- which is precisely the silent
truncation this exists to catch. `islice`, not `rows[sample_size:]`,
since the slice would copy the entire unsampled remainder (995,000 dicts
on the table above) purely to iterate it once.

`sample_size=None` keeps its original meaning exactly -- scan every row --
with the verification pass skipped, because a full scan has nothing left
to verify against.

Confirmed directly that this returns the identical answer to a full scan
for a late float, a late `Decimal`, a late `datetime`, a late string, and
a late `bool`, as well as for clean int and clean date columns with no
late value at all.

### A note on the tests, and one that was vacuous

`Test8SampledTypeInferenceIsVerifiedAgainstUnsampledRows`
(`tests/test_db_publish.py`) covers this. Five of its correctness tests
were proven to have teeth the usual way: reverting `_infer_column_type()`
to the sampled-only scan makes each fail with a clean, on-point assertion
(`'BigInteger' != 'Numeric'`, `'Date' != 'DateTime'`), not a crash or an
unrelated error.

`test_verification_only_scans_rows_beyond_the_sample` guards a different
property -- that a non-narrowable column never pays for the sweep at all
-- so removing the fix leaves it passing correctly. It was proven
separately, against a different break: making the verification run for
every inferred type rather than only the two narrowable ones, which it
catches (`100 != 10` reads).

Worth recording plainly, because it was nearly shipped: the first version
of `test_nones_beyond_the_sample_do_not_trigger_rewidening` asserted only
the resulting *type*. That test was vacuous. A re-inference triggered by a
`None` still returns `BigInteger`, so the assertion could never fail
regardless of which implementation ran -- the same class of silently-
passing test as the `warnings.catch_warnings()` case recorded above,
found the same way, by deliberately breaking the code and watching the
test pass when it should not have. What actually separates correct from
broken here is the *cost*, not the outcome, so it counts row reads
instead, and now fails (`71 != 60`) when `None` is treated as a violation.

### Still open

This narrows the gap; it does not close the underlying question. Column
types remain data-dependent, and `publish()` does `DROP` + `CREATE` on
every run, so a column that gains its first float genuinely does change
from `bigint` to `numeric` between two runs -- correctly, now, but
visibly to anything downstream that joins on it. For any published table
with real consumers, pinning types via `db_contract` / `db_type_overrides`
remains the actual answer; inference is a convenience for exploratory
tables, and should be described that way rather than relied on. Logging
the inferred schema per publish, so drift is visible rather than silent,
is a reasonable next step and is not implemented here.

## A test double that was more permissive than the library it stood in for

Found only once genuine `sqlalchemy` (2.0.43) and the genuine `psycopg2`
Python source became available in the sandbox, and worth recording as its
own class of defect rather than folded into a list of fixes: every test
here passed, and the shipped code was correct, but one specific line of
`runner.py` was protected by nothing at all.

`run_pipelines()` calls `publisher.discard_pending_read()` between the
source-state read and the pipeline loop. Deleting that line outright and
running the full suite produced **217 passed, 0 failed** -- confirmed
directly, not suspected.

What the real library does, confirmed directly against SQLAlchemy 2.0.43
driving a real SQLite engine (which needs no external driver, so this is
checkable in the sandbox from now on):

```
conn.execute(...)   -> autobegin; conn.in_transaction() is True
conn.begin()        -> InvalidRequestError: This connection has already
                       initialized a SQLAlchemy Transaction() object via
                       begin() or autobegin; can't call begin() here
                       unless rollback() or commit() is called first.
```

`SourceStateStore`'s `ensure_table()`/`read_state()` go through
`publisher.ensure_connection()`, never `_ensure_transaction()`, so a
source-check-enabled run reaches the pipeline loop with an implicit
transaction already open. The first `publish()` calls
`_ensure_transaction()` -> `conn.begin()`. Without the discard, **every
source-check-enabled run that also publishes a table fails on its first
publish**, in production, immediately -- while the suite stayed green.
Reproduced end to end with a real `DbPublisher` on a real SQLAlchemy
SQLite engine before changing anything.

The cause was `tests/test_db_publish.py`'s own `FakeSqlaConnection.begin()`:

```python
def begin(self):
    self._in_transaction = True
    return FakeSqlaTransaction(self)
```

It accepted what the real library rejects. That file's docstring claims
the fake "models SQLAlchemy 2.0's real, documented autobegin behavior
directly ... not a simplification for convenience," and for `execute()`,
`commit()`, `rollback()`, and `close()` that was accurate -- the claim was
only ever checked for the behaviors the bug it was written for depended
on. `begin()`'s own constraint was never part of that check. A fake is
only as good as the specific behaviors someone thought to verify; the
unverified ones default to *permissive*, which is the direction that
hides bugs rather than inventing them.

### The same isolation mistake, made again

The first fix was `Test9DiscardPendingReadEnablesTheFirstExplicitTransaction`
in `tests/test_db_publish.py`, covering the mechanism directly. Deleting
`runner.py`'s call again: **222 passed, 0 failed.** Testing the mechanism
in isolation says nothing about whether the runner invokes it at the right
moment -- exactly the distinction already recorded in this README for
`stabilize()` ("proven with a dedicated test that exercises
`run_pipelines()`'s own sequencing specifically, not just `stabilize()` in
isolation"). That lesson was written down here and then repeated anyway,
which is worth being plain about rather than presenting the eventual test
as if it had been the obvious first move.

What actually closes it is `Test6RunnerClearsThePendingSourceStateReadBeforePublishing`
(`tests/test_source_change_runner.py`), which drives `run_pipelines()`
end to end with source checking enabled and a `db_table` pipeline, and
asserts the ordering (`['discard', 'publish']`), not merely that the call
happened. `FakeDbPublisher` now models just enough for that to be
observable: its connection tracks the implicit transaction any `execute()`
opens, `discard_pending_read()` clears it, and `publish()` raises while
it is still open. That is deliberately kept at orchestration granularity
-- it does not replicate `DbPublisher`'s internals, which
`tests/test_db_publish.py` owns; it only makes an ordering property of the
runner visible to the file whose job is the runner's orchestration.

With both in place, deleting the line now fails 2 tests with the real
error message, raised at `runner.py`'s own `publisher.publish(payload)`.

### What this says about the other fakes

Nothing here proves the remaining fakes are wrong. It does show the
standing verification is one-directional: each fake was checked against
the behavior its originating bug needed, and unchecked behavior silently
defaults to accepting more than the real library will. Now that genuine
`sqlalchemy` is available and its SQLite dialect needs no driver,
DbPublisher-level transaction semantics can be checked against the real
thing directly rather than against a model of it -- worth doing for
`commit()`/`rollback()`/`close()` as well, and not done here.

`psycopg2` remains stubbed regardless: the uploaded package carries
`_psycopg.cp313-win_amd64.pyd`, a Windows / CPython 3.13 binary that
cannot load on the Linux / CPython 3.12 sandbox. Its pure-Python source is
genuine and used unmodified; only that one C module is stood in for, which
is enough for SQLAlchemy's psycopg2 dialect to read its DBAPI attributes
and for `make_engine()`/`URL.create()` to be exercised for the first time
(confirmed: `postgresql+psycopg2://u:***@h:5432/db?options=-c+search_path%3Dbsr`,
`NullPool` applied, empty query when no `options` credential is given).

That binary is also the only evidence in this project about the runtime it
actually ships to, and it disagrees with this README: "built and verified
against Python 3.12.3 specifically" versus a CPython 3.13 Windows wheel.
Not resolved here -- the wheel may simply be from whichever machine ran
`pip download` -- but worth confirming, since 3.12 and 3.13 differ in
places this codebase touches.

## The gc.collect() hardening was on the wrong workbook

`xlsx_info()` has run `gc.collect()` on every path for a while now, on a
confirmed production finding: openpyxl object cycles keep XLSX/ZIP handles
open on SMB/DFS after an explicit `wb.close()`, and ordinary refcounting
does not release them. `open_workbook()` had none. Confirmed directly by
counting calls, not inferred:

```
gc.collect() during xlsx_info()      : 1
gc.collect() during open_workbook()  : 0
gc.collect() during resource.close() : 0
```

That is backwards relative to the risk. `xlsx_info()`'s workbook is open
for milliseconds. `open_workbook()` serves the workbook
`excel_resource._ensure_workbook()` RETAINS for the resource's entire
lifetime -- in SMB non-buffered mode holding the remote stream open until
`close()`. If the underlying finding is real, and it was confirmed from
real production experience, the retained workbook is where it costs most,
and it was the one path with no protection at all. The hardening had been
applied where the bug was originally found rather than where it
concentrates.

`open_workbook()` now mirrors `xlsx_info()` deliberately rather than
approximately: `del wb` in its own inner `finally:`, so a raising
`wb.close()` cannot skip it; `gc.collect()` in an outer `finally:`, so it
runs whether the body succeeded, the body raised, or `wb.close()` itself
raised -- and still only after `open_binary()`'s own context has exited,
never while the underlying stream could hold a reference. The `try:` opens
*after* `wb = load_workbook(...)`, so a failing `load_workbook()` never
reaches `del wb` with `wb` unbound.

### Adding the collect alone would have achieved nothing

The second half matters as much as the first, and would have been easy to
miss. `excel_resource.close()` cleared `self._wb` in its `finally:` --
*after* `__exit__()` returned. Since `gc.collect()` now runs from inside
that `__exit__()` call, the resource's own attribute was still pointing at
the workbook at the exact instant of the collect. Confirmed directly with
`gc.get_referrers()` before changing anything: the resource's `__dict__`
was in the list. That is precisely how a skipped `del wb` defeats
`xlsx_info()`'s own collect, reproduced one level up.

`close()` now drops `_wb` and `_workbook_cm` *before* calling `__exit__()`,
via the same swap-then-close shape `DbPublisher.close()` already uses --
which also makes a second `close()` after a failed one a clean no-op
instead of a retry of the same failing context. Re-checked afterwards:
the only remaining referrers at collect time are openpyxl's own internals
(a worksheet's `_parent`, a named style's `_wb` back-reference, a
`ReadOnlyWorksheet`) -- exactly the cycles `gc.collect()` exists to break,
and nothing belonging to `task_core`.

Confirmed directly that none of the resource's six caches transitively
reference the workbook or any worksheet, so clearing them after the
collect costs it nothing; they are still cleared in a `finally:` so a
failing `__exit__()` cannot leave stale cached data for a workbook that is
gone.

Verification for the `del wb` half is by frame introspection at the
collect's own call site, not a weakref -- a real openpyxl `Workbook` has
internal reference cycles (that being why `gc.collect()` is needed at
all), so a weakref could report the object alive for an unrelated reason
regardless of whether the `del` ran.

### What `del wb` does and does not guarantee

Raised by external review and confirmed directly, because the wording
here and in the `xlsx_info()` section above could otherwise be read as
claiming more than is true: `del wb` guarantees the workbook is no longer
a *local variable* of the function running `gc.collect()`. It does not
guarantee the workbook is collectible at that moment.

On the one path where `wb.close()` itself raises, the exception is still
propagating when the outer `finally:` runs its `gc.collect()`, and its
live traceback holds the frame of the failing `close()` -- whose own
`self` is the workbook. Reproduced directly with a genuine bound method,
walking the caught exception's traceback afterwards:

```
    <module>          holds wb via ['wb']
    __exit__          holds wb via -
    open_workbook     holds wb via -          <- del wb worked
    failing_close     holds wb via ['self']   <- traceback keeps it alive
```

Removing that last reference would mean clearing or discarding the
traceback, trading away the diagnostics that make a failing close
debuggable at all -- a bad trade on what is already an error path. So
`del wb` is the correct structural fix and stays, but on a *failing*
`close()` the collect should not be expected to release the handle. It
does its job on every other path, which is where the original production
finding actually lives. The tests assert only what is true -- that `wb`
is gone as a local -- and deliberately do not assert collectibility.

## db_resource.close() never got the treatment its neighbours did

Two defects in four lines, both confirmed directly before fixing:

```python
def close(self):
    if self._conn is not None:
        self._conn.close()
        self._conn = None
```

A raising `conn.close()` left `_conn` still set, so the resource reported
itself open and a second `close()` retried the same failing connection --
where `excel_resource.close()` already cleared its own state in a
`finally:` under exactly this failure. And `_table_cache` was never
cleared on *any* path, success included.

That second one is not cosmetic, and it turned on a fact worth checking
rather than assuming. An initial reachability sweep suggested the cache
did not hold the connection at all; that sweep was wrong -- an
`id()`-keyed graph walk whose short-lived intermediate objects can have
their ids reused, silently marking a live object as already-visited.
Reading the attribute directly settled it: `petl`'s `DbView` stores the
connection on its own `.dbo`, and after `close()` a cached view's `.dbo`
was still the closed connection object. So a later `get_table()` with the
same key returned a table bound to a dead connection -- failing at
traversal time, far from the call that caused it -- and kept the
connection reachable after the resource had reported itself closed.

`close()` is now swap-then-close with the cache cleared in a `finally:`.
The exception is still allowed to propagate deliberately:
`task_context.close()` routes every resource through
`cleanup.attempt_all_cleanup()`, which needs a genuine failure to surface
so `run_pipelines()` can decide whether to log or raise it. Swallowing it
here would hide a real leaked connection -- the exact failure mode the
cleanup redesign exists to eliminate.

### Teeth

`Test18RetainedWorkbookGetsTheSameGcTreatmentAsMetadata` and
`Test19ExcelResourceCloseDropsTheWorkbookBeforeCollecting`
(`tests/test_file_resources.py`), and
`Test3CloseIsExceptionSafeAndClearsTheTableCache`
(`tests/test_db_resource.py`). Each half of each fix was reverted
separately, not the fixes as a whole, so the coverage is known to be
specific rather than merely correlated:

| reverted | caught by |
| --- | --- |
| `open_workbook()`'s collect + `del wb` | 3 tests |
| only the inner `try/finally` around `del wb`, collect left in place | exactly 1 test |
| only `close()`'s ordering (`_wb` cleared after `__exit__` again) | exactly 1 test |
| `db_resource.close()` back to the two unguarded statements | 4 tests |

The one that reports as an `ERROR` rather than a `FAIL`
(`test_a_second_close_after_a_failure_is_a_no_op`) was checked to fail for
the right reason and not incidentally: it raises
`RuntimeError('connection close failed')` from the second `close()`
retrying the same connection, which is the behavior it guards.

## Zero-dimensional arrays, and an asymmetry hiding behind them

Raised by external review as a narrow normalization-policy question, and
correct: the scalar guard added for one-element containers used
`pd.api.types.is_scalar()`, which reports False for `np.array(5)`. True
as a statement about types -- it is an `ndarray` -- and wrong for what
`is_missing()` and `_normalize_value()` actually need to decide. A
zero-dimensional array wraps exactly one scalar and has no container
semantics to preserve, so `np.array(5)` reached the DB driver as an array
instead of the int `5`, while `np.array([5])` is a genuine one-element
container that must stay one. `.ndim == 0` is exactly that line.

Investigating it turned up a second, older asymmetry the review did not
reach, confirmed directly across dtypes: `pd.isna()` returns a plain
numpy bool for a *typed* zero-dim array and a zero-dim *array* for an
object-dtype one.

| value | dtype | `pd.isna()` | old `is_missing` |
| --- | --- | --- | --- |
| `np.array(np.nan)` | float64 | `np.True_` | True |
| `np.array(np.datetime64('NaT'))` | datetime64 | `np.True_` | True |
| `np.array(pd.NaT)` | object | `array(True)` | **False** |
| `np.array(None)` | object | `array(True)` | **False** |
| `np.array(pd.NA)` | object | `array(True)` | **False** |

Five values that all hold nothing, behaving two different ways based only
on the dtype numpy happened to infer.

### Why not the review's own one-liner

The review suggested relaxing the guard in `_normalize_value()` alone:

```python
if not pd.api.types.is_scalar(value) and getattr(value, 'ndim', None) != 0:
    return value
```

That fixes `np.array(5)` and makes the asymmetry worse. With
`is_missing()` left as it was, `np.array(pd.NaT)` stops being an array
and becomes a bare `pd.NaT` handed to the driver -- trading a value that
fails loudly for one that may not. Confirmed directly before choosing a
different shape.

What ships is one shared predicate, `_is_scalar_like()`, used by both --
`is_missing()` applies it to `pd.isna()`'s own *result*, `_normalize_value()`
to the value itself. All five rows above now normalize to `None`,
`np.array(5)` normalizes to `5`, and every one-element container
(`np.array([5])`, `np.array([None])`, `pd.Series([5])`, `[5]`, `[None]`)
is preserved exactly as before -- which is the property the original
scalar guard exists for and the one most at risk from this change.

numpy's own scalars (`np.int64`, `np.float64`) also report `.ndim == 0`,
but are already `is_scalar()`, so the first branch short-circuits and
nothing changes for them -- confirmed directly rather than assumed.

`normalize_for_excel()` picks up the missing-value half automatically
(it shares `is_missing()`) and not the unwrapping half, which is correct
scope: it has never done scalar conversion, only missing-value and
timezone handling.

Covered by `Test10ZeroDimensionalArraysAreScalarsNotContainers`
(`tests/test_db_publish.py`), proven against two separate reverts: the
whole fix removed, and the review's one-liner in place of it. The second
revert is caught specifically by the asymmetry test, which is the point
of having it.

## Two pipelines could silently overwrite each other's outputs

Raised in review and confirmed directly, both kinds, before fixing. Two
*active* pipelines declaring the same output target destroyed each other's
work with no error, no warning, and a run that reported success.

**`excel_name`.** The adapter's `to_excel()` writes a whole workbook via
`toxlsx(path)`, and `PipelineSpec` has no sheet field -- so there is no
same-file-different-sheet arrangement this could have been serving.
Confirmed: two pipelines declaring `'same.xlsx'` both ran, `excel_outputs`
listed the name twice, and only the second pipeline's rows existed on
disk.

**`db_table`.** Worse, because it lands inside the committed transaction.
`publish()` does `DROP` + `CREATE`, so the second pipeline drops the table
the first has just filled. Reproduced against a real SQLAlchemy engine:

```
after pipeline 1 publish : ['a', 'b', 'c']
after pipeline 2 publish : ['z']
after commit             : ['z']
table_rows               : {'same_table': 1}
```

`row_counts` is keyed by table name, so the three lost rows leave no trace
in the `RunResult` either -- `written_tables` records both publishes, but
the count that survives is the last one.

### Where the check goes, and why

In `validate_pipeline_classes()` (`runner.py`), which is the last thing
`run_pipelines()` does before `build_context()`. A task with colliding
targets now fails before any resource is constructed, any remote file is
opened, or any connection is made -- rather than partway through a run
that has already done real work. Covered by a test that asserts
`build_context()` was never called, not merely that an exception was
raised.

Only *active* pipelines are checked: the specs dict is built from
`RUN_SEQUENCE`, so a pipeline that isn't running can declare whatever it
likes. But the check is unconditional with respect to `output_excel` /
`output_db` -- a duplicate declaration is a defect in the task definition,
not a property of one invocation, and gating it on this run's flags would
let it hide until the first run that happened to enable that output.

`excel_name` and `db_table` are separate namespaces. A workbook called
`sales` does not collide with a table called `sales`.

### Comparison is casefolded on every platform, deliberately

`os.path.normpath(value).casefold()` for `excel_name`, `.casefold()` for
`db_table`. Specifically *not* `os.path.normcase()`, which is a no-op on
POSIX and lowercases on Windows: that would make this validation give
different answers depending on where it ran, so a task could validate
clean on a developer machine and then collide on the server. A rule that
is merely strict is better than one that is inconsistent.

Casefolding is also the correct answer for both targets independently of
that: Windows filesystems treat `Report.xlsx` and `report.xlsx` as one
file, and PostgreSQL folds unquoted identifiers, so `Sales` and `sales`
are one table. The cost is rejecting two names that genuinely differ only
by case on a case-sensitive filesystem -- a combination already broken on
the platform this ships to, so refusing it everywhere loses nothing real.

`find_duplicates()` (`types.py`) is deliberately not reused here despite
doing the adjacent job for `RUN_SEQUENCE`: it returns the duplicated
*values*, and the useful half of this error is *which pipelines* collide.
The message names them and shows the raw values, so a case-only collision
is visible in the error itself:

```
pipelines in run_sequence declare the same output target, which would
silently overwrite each other: excel_name -> load_hr='Report.xlsx',
load_ops='report.xlsx'; db_table -> load_hr='Sales', load_ops='sales'
```

### Existing tasks

All three shipped tasks were audited against the new check before it went
in: `hr_task` (8 active pipelines), `ops_task` (5) and `hr_petl_task` (1)
all pass, with every `excel_name` and `db_table` distinct. This adds no
migration burden.

Covered by `Test11DuplicateOutputTargetsRejected` (`tests/test_binding.py`),
sibling to `Test4DuplicateRunSequenceRejected` -- the same class of
silent-overwrite defect, reached by declaring one target twice rather than
one pipeline twice. Proven against three separate reverts: the check
removed entirely (5 tests fail), normalization removed so only exact
strings match (2 fail), and the two namespaces merged into one (1 fails,
with the right message).

## Publishing through a staging table, and the identifier rules it needs

Two changes that only make sense together: the live table is no longer
touched until the end of the run, and every identifier involved is
validated before it reaches SQL.

### The lock window

`publish()` used to do `DROP` + `CREATE` + `INSERT` on the live table
directly, inside the pipeline loop, while the run's single `commit()` came
at the very end. That takes an `ACCESS EXCLUSIVE` lock on the published
table at its first publish and holds it for the entire remainder of the
run. Measured by instrumenting a three-pipeline run:

```
   0.00s    resource r1: opening remote file
   1.54s  PUBLISH tbl_1  <- lock taken, held until commit
   1.54s    resource r2: opening remote file      } tbl_1 locked
   3.08s  PUBLISH tbl_2                           } for all of this
   3.08s    resource r3: opening remote file      }
   4.62s  COMMIT  <- every lock released at once

  tbl_1 locked for 3.08s of a 4.62s run
```

The work filling that window is other pipelines opening remote files and
writing Excel -- nothing to do with the locked table. On a task publishing
eight tables from SMB-hosted workbooks, the first is unavailable for very
nearly the whole run.

Rows now go into a per-run staging table with the freshly inferred schema,
and a publication phase after the loop -- after `update_source_state()`,
immediately before `commit()` -- drops each live table and renames its
staging table into place. Verified against a real SQLAlchemy engine: the
live table stays readable throughout, the final schema is fully evolved
(`bigint` -> `numeric` plus a new column), and no staging table is left
behind.

### Why not TRUNCATE + INSERT

Rejected on the project owner's reasoning, and correctly: `DROP` + `CREATE`
*is* the schema-evolution mechanism here. `TRUNCATE` requires the existing
schema to stay compatible, which pushes column migration back onto every
task -- altering columns, resolving type changes, removing obsolete ones,
and distinguishing legitimate evolution from accidental drift. The staging
swap has none of that: the staging table's columns are whatever
`_infer_column_type()` decided this run.

The swap does not preserve grants (both designs create a fresh table --
`ALTER DEFAULT PRIVILEGES` is the answer there, and it is configuration,
not code) and does not survive dependent views, since `DROP TABLE` still
happens. Views and free schema evolution are mutually exclusive here; that
is a property of the design, not a defect in it.

Ordering in the publication phase is sorted by final name. Two tasks
publishing an overlapping set of tables in different orders could
otherwise deadlock on these locks; a deterministic global order removes
that for free.

PostgreSQL's DDL is transactional, so a rollback -- or a dropped
connection, or a killed backend -- should remove the staging table with
everything else, leaving no orphan residue and no cleanup job to write.

Flagged as documentation rather than measurement, because it cannot be
demonstrated in this sandbox and this project has already produced two
cases where the two needed checking separately. Confirmed directly that
SQLite cannot stand in here: pysqlite does not begin a transaction for
DDL, so `CREATE TABLE` is auto-committed and survives a rollback while DML
rolls back correctly. The test for this asserts only that a rolled-back
run leaves the live table untouched, which is true on both backends, and
deliberately does not assert the absence of residue.

Worth confirming once on a real instance, since orphan staging tables
would otherwise accumulate silently:

```sql
begin;
create table bsr.probe__stg_test (v int);
rollback;
select tablename from pg_tables where tablename like '%\_\_stg\_%';
```

### The naming rule

```
<shortened readable prefix>__stg_<target_token>_<run_token>

    employee_funnel__stg_a13f294c_7b32e910
```

Only the human-readable prefix is ever shortened. The uniqueness-bearing
suffix is fixed width and is never truncated -- truncating it would defeat
the reason it exists -- and being fixed width is also what lets preflight
calculate the full length statically.

`target_token` hashes `(schema, final table name, STAGING_NAME_KIND)` and
deliberately excludes the run. That is what makes cross-spec collisions
*statically* checkable: preflight computes exactly the tokens the real run
will use. Folding the run into the hash instead would make two targets
collide under one run id and not another, which is how an earlier draft of
this had it. Confirmed directly with a single per-run id:

```
sales_..._quarter_northern -> sales_..._quart__stg_c15ad0e6182548bd
sales_..._quarter_southern -> sales_..._quart__stg_c15ad0e6182548bd   collide
```

Position is excluded too, and for the opposite reason: including it would
give a repeated publication of the same target two different staging names
that both swap into one final table, silently -- the exact overwrite class
that duplicate-target rejection exists to prevent, one layer down. With
position excluded, a repeat produces the same name and the generated-name
registry sees it.

`STAGING_NAME_KIND = 'stg'` is a named internal namespace constant, not a
parameter. Passing a `purpose` argument would advertise supported
variation that does not exist. Generalize when a second use case arrives.

Truncation is by **bytes**, on character boundaries. PostgreSQL's limit is
63 bytes, not characters, and this project handles Russian data: confirmed
directly that a 62-character Cyrillic name is 116 UTF-8 bytes, so cutting
to 41 *characters* still leaves 77 bytes and blows the budget anyway.

### Validation: two tiers, one seam

Neither tier alone is sufficient, and the seam between them is what keeps
`runner.py` engine-neutral. `validate_pipeline_classes()` never learns
what 63 means; it knows only that there is a backend to ask, and that the
moment to ask is after structural validation and before
`build_context()`. The hook is a classmethod on `publisher_factory`, so
nothing is constructed that would need closing if `build_context()` raises
next.

**Preflight** -- always, pure, connection-free. Skipped entirely when no
spec declares `db_table`, so a genuinely DB-free task has no backend
policy applied to it. Runs regardless of `output_db`: an unpublishable
declared name is a defect in the task definition, not a property of one
invocation, and gating it would let it hide until the first run that
enabled DB output. It performs no backend I/O, so a DB-disabled run still
touches nothing -- confirmed by a test asserting no publisher is even
constructed. Validates the schema, each declared `db_table`, each derived
staging name, cross-spec staging collisions, and declared column targets
from `db_contract.values()`, `db_output` and a literal `db_updated_at`.

**Runtime** -- only when publishing. The server's own
`max_identifier_length` is read before the first DDL, and the effective
limit is `min(configured, server)`: configuration can only ever tighten,
never raise the limit past what the server will accept. Actual
`payload.columns` are validated after `db_contract` is applied, after any
`db_output` projection, and after `apply_db_updated_at()` appends its
column -- the only point at which they all exist. Generated staging names
are asserted immediately after construction, and the generated-name
registry is the final collision guard.

That placement is load-bearing, not incidental. Confirmed directly: run
the column check *before* the contract and it rejects 77 of the 79 source
names in this project -- raw Cyrillic spreadsheet headers -- and breaks
every `hr_task` pipeline. Run it after, and all 145 target names pass.

`DbPublishInvariantError` subclasses `DbPublishError` and is used only
where this module was supposed to have made failure impossible by
construction. Existing `except DbPublishError` cleanup still catches it;
`isinstance` still tells them apart; and the message says the invariant
was violated rather than implying a bad declaration.

### The portable identifier convention

`PORTABLE_IDENTIFIER_RE` lives in `types.py` and is documented there as a
*scaffold convention*, not a PostgreSQL rule -- the byte limit, staging
generation, and normalization stay in `db_publish.py`, so no PostgreSQL
fact leaks into the engine-neutral layer. `source_state.py` now imports it
rather than keeping its own copy.

Lower case only, `^[a-z_][a-z0-9_]*$`, tightened from the previous
`[A-Za-z_]` form. Uppercase is exactly what makes an identifier
case-fragile: SQLAlchemy quotes a mixed-case name to preserve it, quoting
defeats PostgreSQL's folding, and the two become different objects.
Confirmed directly against the real dialect's preparer:

```
'sales' -> sales        CREATE TABLE bsr.sales
'Sales' -> "Sales"      CREATE TABLE bsr."Sales"
```

So "portable" means something worth the name: an identifier that behaves
identically whether quoted or not, and never needs quoting downstream.
Confirmed that all 159 identifiers this project publishes -- 13 tables,
145 columns, 1 schema -- already satisfy it, as do the source-state
`bsr` / `task_scaffold_meta`, so the tightening broke nothing.

The escape hatch is `PipelineSpec.db_identifier_mode`, a mode rather than
a boolean: `allow_unsafe_identifiers` would conflate Unicode, quoting,
punctuation, case sensitivity and actual SQL safety into one word
describing none of them. `quoted` relaxes the regex and nothing else --
non-empty, no NUL, byte limit, duplicate detection, generated-name safety
and column uniqueness all still apply. It does not govern the schema,
which is task-wide and always portable: letting a per-spec flag reach a
per-run value would need an arbitrary resolution rule across specs.

### Correction to the duplicate-target check

The version shipped in 0.2.9 casefolded `db_table`, on the stated grounds
that PostgreSQL folds unquoted identifiers so `Sales` and `sales` are one
table. **That reasoning was wrong** -- nothing in this project ever emits
an unquoted mixed-case identifier, because SQLAlchemy quotes them to
preserve case, which defeats the folding. They are two different tables,
and casefolding rejected a pair that would actually have worked.
Over-strict rather than unsafe, but wrong, and corrected here rather than
left standing.

`db_table` comparison is now exact. That is correct under both identifier
modes -- under `portable` every name is lower case so exact and casefolded
coincide, and under `quoted` case is significant and only exact is right
-- so no mode-dependent comparison logic is needed. `excel_name` keeps
casefolding: Windows filesystems genuinely do treat `Report.xlsx` and
`report.xlsx` as one file, which is a filesystem property with nothing to
do with SQL.

### Teeth, and the same mistake for the third time

Every piece was reverted separately:

| reverted | caught by |
| --- | --- |
| preflight call removed from `runner.py` | 3 tests |
| fallback changed to a no-op instead of real policy | 1 test |
| portable regex loosened back to mixed case | 2 tests |
| runtime payload-column validation removed | 1 test |
| generated-name registry disabled | 1 test |
| target token no longer depends on the table name | 2 tests |
| prefix truncation replaced by truncating the whole name | 2 tests |

Recorded plainly because it is the third occurrence: the first version of
the preflight tests exercised `DbPublisher.preflight()` directly, and
deleting the call from `runner.py` left **all 264 tests passing**. Testing
a mechanism in isolation says nothing about whether the runner invokes it,
which is precisely the lesson already written down in this README for
`stabilize()` and again for `discard_pending_read()`. Learning it twice
and repeating it a third time is worth the ink.
`Test12RunnerInvokesBackendPreflightBeforeBuildingResources`
(`tests/test_binding.py`) is what actually closes it, asserting that
`build_context()` never ran.

### Corrections from external review of the staging design

Thirteen findings, every one confirmed directly before acting on it. The
consequential ones:

**Finalization moved inside `commit()`.** It was public, and the runner
was expected to call it. Two failures followed. `publish()` + `commit()`
without the runner reported `committed=True` and
`committed_tables=['None.t']` while the live table still held its old rows
and the staging table was committed permanently -- a publisher reporting
success having published nothing. And it expanded the `publisher_factory`
protocol, which is an advertised extension seam: a publisher written
against the previous contract died with `AttributeError` at the end of an
otherwise successful run. Folding the swap into `commit()` fixes both and
removes the method from every fake again.

**The source-state table is now a reserved target.** It was missing from
the model entirely, with two consequences. Its identifiers were
length-unchecked -- `SourceStateStore` applies the portable regex and
nothing else, so a 64-byte lower-case source-state table name was accepted
and would have been silently truncated, the exact failure this mechanism
exists to prevent, still reachable through the technical table. And a
pipeline could declare it as a business target: reproduced end to end, the
run updated fingerprints in the real table, then the swap dropped it and
renamed the pipeline's staging table over it. Source state destroyed, run
reports success. The staging design is what makes this succeed silently;
under direct publication the later upsert would probably have failed on
missing columns. Preflight now validates the source-state schema and table
and rejects any pipeline declaring them, and no longer skips when only the
source-state table exists.

**`SHOW max_identifier_length` failure is no longer swallowed.** The
catch-all existed for backends without the setting, but it also caught
real PostgreSQL failures, which made the "authoritative" runtime check
non-authoritative -- any error silently restored the assumed value. Worse,
the statement runs inside the explicit transaction, and a failed statement
leaves a PostgreSQL transaction aborted, so the next staging DDL would
fail with a secondary transaction-aborted error obscuring the real cause.
Now branches on dialect: non-PostgreSQL backends fall back, PostgreSQL
raises `DbPublishError` with the original exception as its cause.

**`fullmatch()`, not `match()`.** Python's `$` also matches immediately
before a trailing newline, so `'foo\n'` passed as a portable identifier --
confirmed directly. That name is interpolated unquoted into source-state
SQL, where the newline is whitespace, and quoted for output tables, where
it becomes part of the identifier. Neither is what the convention
promises.

**`DbPayload.identifier_mode` is validated, not just compared.**
`mode == 'portable'` meant any typo selected the permissive branch;
`identifier_mode='portbale'` let a Cyrillic column through. `PipelineSpec`
validates its own field, but a payload built directly does not pass
through it. `IDENTIFIER_MODES` is now one shared constant.

**Excel target comparison uses `abspath`.** `normpath` alone left
`out.xlsx` and `<cwd>/out.xlsx` as distinct keys although `to_excel()`
writes to one physical file, so the silent-overwrite class stayed
reachable through a spelling difference. (Symlink aliases would need
`realpath()`; not handled.)

**`db_table` comparison no longer strips.** The correction that made it
exact still called `.strip()`, which contradicted the semantics it
claimed: under `quoted` mode `"report"` and `"report "` are two valid,
distinct identifiers.

**Type inference no longer guesses on an all-null sample.** If the first
`sample_size` rows are all null, `Text` is a fallback rather than an
observation, and no verification ran because `Text` is not narrowable.
Confirmed: 5000 nulls then an int inferred `Text` where a full scan gives
`BigInteger`, likewise for a late float or date. Sparse columns routinely
have long leading null runs after sorting or monthly expansion. The scan
now returns the family set rather than a type, so "saw nothing" and "saw
text" are distinguishable and the rescan costs no extra pass over the
sample. The timezone question is untouched and still open: aware and naive
datetimes share one `'datetime'` family and infer `DateTime(timezone=False)`.

**Direct coverage for the swap.** `_finalize_published_tables()` was the
centre of the publication architecture and had no test that invoked it --
only fakes in runner tests, and naming and preflight logic.
`Test12StagingSwapAndReviewCorrections` drives the real publisher against
a real engine: live table untouched until commit, schema evolution
surviving, multiple tables swapping together, no residue after success,
and `commit()` publishing without any separate call.

**Still open, deliberately.** The explicit transaction still opens at the
first `publish()` inside the pipeline loop and stays open through later
SMB reads, transformations and Excel exports. The staging swap fixes the
*live-table lock*, which was the dashboard-blocking problem; it does not
shorten the transaction itself, which still holds catalog and staging
locks, delays vacuum, accumulates WAL, and makes a late rollback more
expensive. That is a deliberate trade for atomicity and is recorded here
separately so the two are not conflated.

Also open: the identifier-limit override reaches preflight only via
`run_pipelines(db_max_identifier_bytes=...)`. A factory such as
`functools.partial(DbPublisher, max_identifier_bytes=40)` carries its
limit where preflight cannot see it, so static validation would use 63 and
the real limit would surface only at publication. Passing both keeps them
aligned; a shared naming-policy object visible to factory and preflight
alike would remove the need to.

### Existing tasks

All three audited against the new validation before it went in. `hr_task`
(8 pipelines), `ops_task` (5) and `hr_petl_task` (1) all pass preflight
against schema `bsr`, and the 13 staging names they would generate are
distinct, valid UTF-8, and at most 40 bytes of the 63 available.

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

Four more, following an external review that identified real,
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

`tests/test_file_resources.py` -- file selection determinism and
fingerprinting: latest-file-by-modification-time, deterministic tie-break
when modification times are exactly equal, file-set order and fingerprint
signature both independent of whatever order the filesystem's own
enumeration happens to return, fingerprint changes on add/remove/resize/
retouch, fingerprinting reuses the already-selected files rather than
re-listing the directory, and `on_empty='raise'|'empty'`. Real temp
directories and real files throughout. Two things worth being honest
about, both found only by actually trying to break the code and checking
the test still caught it, not assumed from the test passing once:

- The first version of the latest-file/tie-break tests passed even
  against a completely broken selection (`file_infos[0]`, no mtime logic
  at all) -- this filesystem's own `glob()` happened to return the newer
  file first by coincidence, so the test wasn't actually proving what it
  claimed to. Fixed by explicitly controlling file order through a small
  wrapped `source_access`, so the result can no longer come from
  coincidence.
- An earlier version of `tests/test_source_change_runner.py`'s fake
  connection mutated a single dict immediately on every `execute()`, with
  no distinction between staged and committed state -- so
  `FakeDbPublisher.rollback()` had nothing to actually undo, and the
  existing rollback test (which never enabled source checking at all)
  couldn't have caught this either way. Found by external review, not
  here first. `FakeSourceStateConn` is now genuinely transaction-aware
  (`committed_rows`/`pending_rows`), and `Test4SourceStateGenuinelyRollsBack`
  forces a real `commit()` failure after source-state has genuinely been
  staged, then confirms the durable, committed state survives untouched
  -- the only way to actually exercise this, since a pipeline failing
  mid-loop never reaches `update_source_state()` at all (it runs after
  the loop in `runner.py`).
- Also found by external review: every failure-path test covered a
  pipeline failing during the loop, or the final `commit()` failing --
  none covered `ctx.collect_source_fingerprints()` itself raising, which
  happens earlier still, before any pipeline runs and before
  `build_source_state_store()` is ever constructed.
  `Test5FingerprintCollectionFailureCleanup` covers this directly: one
  resource fingerprints successfully, a second's `source_fingerprint()`
  raises, and the test confirms no pipeline ran, both resources (the
  already-loaded good one and the one that failed) were closed, and the
  publisher was rolled back and closed -- proven against `runner.py`'s
  real code by breaking its cleanup twice, in two different, specific
  ways, and confirming each broke a different assertion.
- Two small precision fixes, also from external review: a `time.sleep()`
  used to force one file to have a later modification time than another
  in `test_file_resources.py`, replaced with explicit `os.utime()` values
  (a sleep-based test can become flaky on filesystems with coarse
  timestamp resolution); and a bare `assertRaises(Exception)` in the
  failed-commit test, replaced with `assertRaises(RuntimeError)` so an
  unrelated exception elsewhere in the test body couldn't accidentally
  satisfy the assertion. Found the same imprecision in `test_binding.py`
  too, not originally flagged -- fixed there as well, to the exact
  exception a frozen dataclass actually raises
  (`dataclasses.FrozenInstanceError`), confirmed empirically rather than
  assumed before making the change.

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

**Python 3.11 or newer is required, and enforced at import time** --
`task_core/types.py` raises a clear `RuntimeError` on anything older,
rather than leaving 3.10 to fail only when a cleanup error actually
occurs (at which point `e.add_note(...)`'s `AttributeError` inside the
exception handler would mask the real failure -- exactly the failure
mode the cleanup redesign exists to eliminate). `task_core/cleanup.py`
and `runner.py` use the builtin `ExceptionGroup` (for raising more than
one cleanup failure together) and `BaseException.add_note()`, both added
in 3.11. This project has been built and verified against Python 3.12.3
specifically.

Changelog note (v0.2.0): `select_file_infos()` given a *file* path now
raises `ValueError` directing callers to `select_fixed_file()`, instead
of silently returning a single-file selection with every filter argument
ignored -- the facade's "every name resolves exactly as before"
guarantee is name-level everywhere and behavior-level everywhere except
this one recorded, deliberate break (also noted in
`task_core/__init__.py`'s own docstring).

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

Correction to the paragraph above, which was wrong and is left visible
rather than quietly rewritten: `petl` is genuinely a local copy of the
real package, but `sqlalchemy` is **not** -- it is a hand-written stub,
as `tests/test_source_change_runner.py`'s own docstring has said all
along ("sqlalchemy's local sandbox stub ... was extended with a minimal
`text()`/`bindparam()`"). Every test that touches the DB layer supplies
its own `FakeSqlaConnection`/`FakeSqlaEngine`, so the stub only has to
carry the import-time surface (`text`, `bindparam`, the type classes,
`MetaData`/`Column`/`Table`, `URL`, `NullPool`); it opens no connection
and models no SQL semantics.

That matters more than a documentation nit, because the stub is
`.gitignore`d and therefore exists in no artifact this project produces.
A fresh sandbox with `petl.zip` but no `sqlalchemy` cannot import
`task_core` at all -- `task_core/__init__.py` -> `resources/db.py` ->
`db_publish.py` -> `import sqlalchemy as sa` -- and **10 of the 12 test
modules error out before running a single assertion**. Confirmed directly
when exactly that happened. The standing instruction to "run the full
suite after every change" silently depends on an artifact no handoff
carries, which is the same class of problem as the packaging incident
recorded earlier: the discipline is sound, the thing it relies on isn't
captured anywhere. Ship the stub alongside `petl.zip`, or commit it under
a clearly-labelled sandbox path, rather than rediscovering this each
time.

