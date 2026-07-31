# 0008 — Bound the publication lock wait

Status: accepted (multi-target timeout invariant strengthened in 0.5.1)

## Problem

Publication drops each live table and renames its staging table into
place. Both need `ACCESS EXCLUSIVE`.

PostgreSQL queues new `ACCESS SHARE` requests *behind* a waiting
`ACCESS EXCLUSIVE`, even though they would be compatible with the reader
already holding the table. So a publisher waiting on one long-running
`SELECT` blocks every reader that arrives afterwards. One slow query plus
one publication becomes a read outage.

The previous implementation made it worse by acquiring locks
incrementally, inside the swap loop: a multi-table publication held
exclusive locks on already-swapped tables while queuing for the next, and
each held lock was itself blocking new readers.

## Decision

Lock every existing target up front, in one statement, under a bounded
wait; retry the whole publication on lock unavailability, within a
wall-clock horizon.

```sql
SET LOCAL lock_timeout = <derived>;
SET LOCAL statement_timeout = <derived>;
LOCK TABLE <all existing targets, sorted> IN ACCESS EXCLUSIVE MODE;
SET LOCAL lock_timeout = 0;
SET LOCAL statement_timeout = 0;
```

Configured through `PublicationLockPolicy`, a field of `PublisherConfig`.

### 0.5.1 clarification

The original 50 ms relationship, `A >= L + M`, orders one lock wait only. It
does not guarantee retryable `55P03` for a multi-target statement because
PostgreSQL applies `lock_timeout` to each sequential acquisition while
`statement_timeout` covers the whole statement. After resolving the actual
existing target set, the publisher therefore enforces the hard invariant:

```text
A >= n * L + M
```

where `n` is the number of relations in the single `LOCK TABLE` statement. A
policy that does not fit is rejected before any target lock is requested. On a
short final attempt, the effective per-target timeout is reduced so the actual
budgets still satisfy the same inequality. `57014` remains terminal and is
never classified by parsing message text.

This is separate from the reader-impact budget `A + P <= B`. All source-state
`DELETE`/upsert and other preparatory database work completes before the first
live-target lock. Existing targets are deduplicated and locked in deterministic
`(schema, table)` order immediately before publication. Replacement leaves only
`DROP`, `RENAME`, comments and commit after acquisition. Explicit stable refill
adds `TRUNCATE`, a row-proportional second write, index/constraint maintenance,
staging drop and commit; the acquisition policy bounds `A`, not that critical
section `P`.

Retry timing uses one absolute monotonic deadline. The existing minimum sleep
is preserved, but jitter is sampled only inside the remaining usable horizon;
a random draw or minimum sleep may not consume the budget reserved for a
useful next acquisition attempt.

Live and prepared relations are resolved through exact `pg_class` /
`pg_namespace` values, and only ordinary tables (`relkind = 'r'`) are accepted.
Views and other relation kinds are rejected explicitly.

## Why each part

**One statement, not one lock per `DROP`.** PostgreSQL still acquires them
internally in sequence; what changes is the *application-level* outcome —
failure is followed by transaction rollback, so either the swap proceeds
with every lock held or nothing is left holding any.

That **bounds** the compounding rather than removing it, and the
difference is observable. Confirmed on a real server: with readers on two
targets, PostgreSQL acquired `Alpha` and kept waiting on `Beta`, and a
reader arriving on `Alpha` in that window blocked behind the
already-acquired lock. What the single statement guarantees is that the
window is bounded by the acquisition budget and ends in rollback, not that
no reader is ever blocked by a partial acquisition. Sorting is what stops two
tasks with overlapping targets deadlocking against each other.

**The horizon gates completion, not permission to start.** A horizon that
only decided whether to *begin* another attempt would let an attempt begun
just inside it run well past — a hint, not a limit. So per-attempt budgets
are derived as `min(configured, time remaining)`: the configured values are
ceilings, and a final attempt may legitimately run with far less.

**Both timeouts, not one.** `lock_timeout` bounds each individual wait;
`statement_timeout` bounds the whole acquisition. With the hard invariant,
`statement_timeout` must leave enough aggregate room for every sequential
per-target wait plus `M`; it is therefore a terminal safety ceiling, not a
smaller competing lock-contention budget. Someone reading only
`lock_timeout_ms` will still predict the wrong total because the worst-case
acquisition budget scales with the actual target count.

**Budgets lifted once the locks are held.** The horizon bounds acquisition,
not the work after acquisition. Replacement is normally catalog-time work.
Explicit stable refill is not: its `TRUNCATE`, full-row insert, target index and
constraint maintenance, and commit scale with the target. Cancelling either
publication mechanism halfway would be worse than allowing the transaction to
finish, but refill's reader impact must be sized independently.

**Locks taken last, after the publication plan.** The plan is not
harmless catalog work: on the standard runner path it runs
create-if-not-exists, a `DELETE` and an upsert against the source-state
table. Holding the target locks across that — with both timeouts already
reset to zero — would keep every live table exclusive for the duration of
whatever that work waited on, recreating the outage. Atomicity is
unaffected: a `55P03` still rolls back the source-state write along with
everything else.

**Rolled back before sleeping.** Otherwise any lock already acquired is
held across the delay, which is the disease itself.

## SQLSTATE policy

| | |
| --- | --- |
| `55P03` `lock_not_available` | retry |
| `57014` `query_canceled` | terminal |
| `40P01` `deadlock_detected` | terminal, logged at ERROR |
| anything else | terminal |

`55P03` is unambiguous here: this code never issues `NOWAIT`, and the
connection is exclusively ours — dedicated, `NullPool`, never pooled — so
nothing else can be requesting locks on it.

`57014` is **not** uniquely `statement_timeout`. An operator's
`pg_cancel_backend()`, a client-side cancel, and a role- or
database-level `statement_timeout` set outside this code all produce it.
Retrying would mean the scaffold arguing with a human who deliberately
stopped it. Deliberately conservative: it costs only the case where
retrying might have worked anyway, and the run fails cleanly with source
state unadvanced, so the next scheduled run republishes.

`40P01` is retryable in principle, but sorted lock order already prevents
deadlock between two `task_core` publications. Seeing one means something
outside this scaffold takes exclusive locks on published tables — which a
retry does not fix and which should be loud.

## Why retry is affordable here

**Only because preparation already committed** (see 0005). A failed
publication discards a swap, not the run: the staging tables are still
there, still validated, still owned. Under the single-transaction design
this replaced, a lock timeout would have meant redoing every pipeline,
every remote read and every payload build — retry would have been
near-unusable.

**The advisory lock is session-scoped** (see 0006), so it persists across
attempts and no separate mechanism is needed to keep retries from racing
another run.

**But it also holds that lock for the whole horizon.** A scheduled run
firing meanwhile skips with `task_already_running`. On a five-minute
schedule a three-minute horizon can eat the next run — correct behaviour,
but it turns one failed publication into one failure *and* one skip, and
the default should be chosen against the tightest schedule in use.

## Consequences

- Failure after the horizon is a normal run failure: `rollback()` drops
  the staging tables, source state was never advanced, and the next run
  republishes. No new recovery path.
- Diagnostics report **actual** elapsed time and attempt count, never the
  configured policy. Because budgets are derived, a final attempt may have
  run with a fraction of `acquisition_timeout_ms`, and reporting the
  configured value would not reconcile with what happened.
- `max_attempts` is a defensive ceiling, not the policy. Unreachable under
  the defaults; it exists to stop a runaway if someone configures a
  sub-second delay.
- The rationale that once justified writing source state *before* the
  swaps — keeping it out of the exclusive window — is now false, because
  the window opens at `LOCK`. It has been removed rather than left
  standing.

## Rejected

**`NOWAIT`.** Never enters the queue, which is the strongest guarantee for
readers, but fails on any overlap however brief. Too sensitive for tables
under constant BI load.

**`lock_timeout` alone, without `statement_timeout`.** Argued for on the
grounds that `lock_timeout × n` already bounds the total. True, but only
if you accept that product as the bound; at eight tables it is four
seconds where two is wanted.

**Rename the old table aside instead of dropping it**, deferring the drop.
PostgreSQL defers the file unlink to commit, so the in-transaction saving
is small, and it would add a second artifact class to cleanup.

**View indirection or partition swap.** Both still need `ACCESS EXCLUSIVE`
on whatever object readers name, so the wait moves rather than
disappearing. `DETACH CONCURRENTLY` genuinely avoids it but constrains
table shape and is a far larger change than the problem warrants.

## Verification status

Unit-tested against a PostgreSQL-dialect fake, and **confirmed against a
real server** (PostgreSQL 16.14) with a policy of `lock_timeout_ms=800`,
`acquisition_timeout_ms=1500`, `retry_horizon_seconds=8`, fixed 1.5 s
delay.

The observation this decision exists for, with the sequence described
above playing out exactly:

```
reader A holds ACCESS SHARE
publisher B queues for ACCESS EXCLUSIVE
reader C blocks behind B                    <- expected, not a defect
B: lock unavailable (attempt 1, 1.7s elapsed, 6.3s of horizon left)
reader C resumed in 2.002s while reader A remained open
```

Both halves matter and the test asserts both: C's delay was bounded
(`0.15 <= elapsed <= 2.5`), **and** reader A's transaction was still open
when C resumed. Without the second assertion the first proves nothing —
C could simply have waited for A. With it, C waited for B's budget, which
is the whole point.

Also confirmed in the same run:

- **Exact target resolution includes every existing live table selected by
  the publication plan.** The regression used a mixed-case table to expose
  parser folding; 0.4.1 subsequently removed such names from the public
  identifier contract, while retaining the exact catalog lookup.
- **Contention arrives as retryable `55P03`, not terminal `57014`.**
  Visible as the publisher's own retry warning, which is the timeout
  ordering invariant doing its job.
- **The publication plan runs before locking.** The log order is
  `publication step: ... source-state update` then
  `locking 1 publication target(s)`, on every attempt.
- **Whole-transaction retry.** Each attempt re-ran the source-state write
  and re-locked; the eventual success committed swap and source state
  together.
- **Horizon exhaustion is transaction-clean.** With `retry_horizon_seconds=0`
  the publisher raised, left `_tx` None and the SQLAlchemy connection out
  of any transaction, dropped its staging artifact, and left the live
  table unchanged.

### Multi-table finding that motivated the 0.5.1 invariant

Two targets, readers on each:

```
lock table "s"."Alpha Table", "s"."Beta Table" in access exclusive mode
PostgreSQL acquired Alpha and continued waiting on Beta
a later reader on Alpha blocked behind the publisher's acquired lock
total statement budget produced terminal 57014 after 2.604s
both live tables and source state unchanged; staging dropped
```

One sorted statement carrying both targets, exactly as designed.
But note **which** timeout fired: the aggregate `statement_timeout`, which
raises `57014` — and `57014` is terminal here by the decision above.

This was clean but violated the intended retry classification: ordinary
lock contention could surface as terminal `57014`. In 0.5.1 the publisher
rejects `A < n*L+M` before issuing the statement, so this configuration is no
longer executable.

### Sizing the two budgets

They are different things, and confusing them produces advice that works
against this decision:

| | |
| --- | --- |
| `lock_timeout_ms` (L) | the **per-conflict** limit — how long to wait for any one target |
| `acquisition_timeout_ms` (A) | the **aggregate** budget for acquiring the complete lock set |

With `n` existing targets, `M` the execution margin, and `P` the
post-acquisition publication critical section, the hard classification
invariant and separate reader-impact budget are:

```
A >= n*L + M        subject to        A + P <= B
```

Expected contention `k <= n` is useful for operational estimates, but it cannot
provide the SQLSTATE guarantee because the publisher cannot know in advance
which targets will wait. `B` is the total reader blocking you have decided to
accept.

**`A` is not `B`.** Once the complete set is held, both timeouts are reset
and the publisher performs `DROP`, `RENAME`, comments and commit with
every lock still held. So for the earliest-acquired target:

```
reader blocking = acquisition + publication
```

`A` bounds only acquisition. Under replacement, `P` is normally short
catalog work. Under explicit stable refill, `P` is row- and index-dependent.
This policy imposes no hard limit on either, so total reader blocking exceeds
`A` by the chosen publication strategy's critical section.

`M` is not decoration. `n·L` exactly equal to `A` leaves nothing for
statement execution, lock-manager work or driver latency — which is why
the defaults cover a worst case of `(5000 − 50) // 500 = 9` sequentially
contended targets, not ten.

**The `A + P ≤ B` constraint is what makes this a rule rather than a
licence.** Without it
the inequality is satisfiable by growing `A` without limit as `n` grows,
and an earlier version of this section said exactly that: widen
`acquisition_timeout_ms` for tasks publishing several contended tables.
**That advice was wrong**, in the specific way worth recording because it
looks reasonable. `A` is precisely how long an already-acquired target
keeps blocking its own readers while the statement waits for the next —
confirmed above, where a reader arriving on `Alpha` blocked behind the
publisher's acquired lock for the remainder of the acquisition. Raising
`A` to accommodate a large `L` lengthens exactly the window this mechanism
exists to shorten, and fits fewer attempts inside the horizon besides.

**When the arithmetic does not fit, the configuration is rejected before
locking.** There are three legitimate corrections, and lowering `L` is not
automatically the right one — it has its own cost. A short per-conflict limit
can turn ordinary multi-second BI contention into a steady stream of `55P03`,
making the retry loop the normal publication mechanism rather than a safety
net:

1. **Lower `L`** when short queue residence is the primary requirement.
   For eight targets, `L = 250` fits under a much smaller aggregate budget
   than `L = 500`, while reducing the window in which a partially acquired
   lock set can block readers.
2. **Publish fewer targets together**, reducing `n` directly.
3. **Raise `A` — but only as an explicit decision to accept a larger `B`**,
   never as a hidden attempt to make the arithmetic disappear.

Scheduling the task for a quieter period may reduce observed contention, but
it does not make an invalid timeout policy executable.

### What enforcement does

`_lock_publication_targets()` knows the actual number of existing targets after
missing first-publication targets are excluded, so it enforces `A >= n*L+M` at
the only point where `n` is known. The check happens before `SET LOCAL` and
before `LOCK TABLE`; failure leaves `lock_attempts == 0`.

The error recommends lowering `L`, splitting the publication, or deliberately
raising `A`. It never parses a `57014` message to guess whether PostgreSQL meant
statement timeout or operator cancellation.

### Deadlock, confirmed

A `40P01` induced from the publication plan — with **zero lock attempts**,
which is the case that made the diagnostic phase-neutral in the first
place:

```
publication step: induced external lock-order conflict
ERROR publication encountered a deadlock (40P01) on attempt 1. Automatic
      retry is disabled: a deadlock indicates an external lock-order
      conflict that needs investigation rather than repetition.
```

Emitted once, no retry, whole transaction rolled back, staging artifact
removed. Had the message still blamed the target locks, it would have
pointed the investigation at a phase that had not yet run.
