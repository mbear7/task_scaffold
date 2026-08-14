# 0016 — Keep `lock_timeout_ms` below `deadlock_timeout`

Status: accepted

Extends [0008](0008-bound-the-publication-lock-wait.md), which decided *that*
the publication lock wait is bounded and retried. This decides how the
per-conflict bound relates to a PostgreSQL server setting that 0008 does not
mention, and records why the obvious adjustment is refused.

## Problem

`PublicationLockPolicy.lock_timeout_ms` defaults to 500. PostgreSQL's
`deadlock_timeout` defaults to 1000.

Those two numbers interact, and nothing in 0008 says so. PostgreSQL cancels a
running autovacuum worker when a regular backend blocks on a lock it holds —
but the cancellation is triggered by the deadlock detector, so it fires after
`deadlock_timeout`, not immediately. A publisher whose `lock_timeout` expires
first never reaches it. It aborts with `55P03`, sleeps, and re-arms the same
too-short timeout on every subsequent attempt.

The result is a publication that retries against a condition PostgreSQL would
have cleared for it in one second, and that reports the contention as a reader.

The asymmetry inside a single publication makes it concrete. The staging
`DROP` at the end of the critical section runs after both timeouts have been
reset to zero (`_lock_publication_targets`, and 0008's "budgets lifted once the
locks are held"). Measured on PostgreSQL 18.4, both halves in one run:

```
ОШИБКА: выполнение оператора отменено из-за тайм-аута блокировки
ОПЕРАТОР: lock table "public"."_perf_noise_1b57d2e2" in access exclusive mode
```

— the target lock, `55P03` at 500 ms, blocker confirmed by `pg_locks` as
`autovacuum: VACUUM ANALYZE` holding `ShareUpdateExclusiveLock`. And:

```
ОШИБКА: отмена задачи автоочистки
КОНТЕКСТ: при сканировании блока 19947 отношения "..._stg_..."
процесс 13252 продолжает ожидать ... AccessExclusiveLock ... в течение 1009.853 мс
процесс 13252 получил ... AccessExclusiveLock ... через 1009.929 мс
```

— the staging `DROP`, with no `lock_timeout` armed at all, waiting 1009.929 ms
for PostgreSQL to cancel the autovacuum, then proceeding.

**The path with no timeout beat the path with one**, against the same
contender, in the same run. That is the observation this record exists for,
because it invites exactly the wrong fix.

## Decision

Keep `lock_timeout_ms` below `deadlock_timeout`. Do not raise the default to
capture the auto-cancellation.

The default stays 500.

## Why

**The retry loop costs the publisher, not readers.** Every unsuccessful
attempt calls `_drop_open_transaction()` before classifying the error, so
nothing is held across the sleep — 0008's "rolled back before sleeping".
Reader exposure is therefore `L` per attempt, in separate bursts, not the
elapsed retry time. Three attempts at 500 ms expose a reader to at most 500 ms
on any one arrival. One attempt at 1500 ms exposes it to 1500 ms.

Total exposure across the two is comparable. **Worst-case single-reader delay
is not**, and that is the quantity 0008 exists to bound. Trading it away to
save the publisher fifteen seconds inverts the decision.

**Raising `L` forces one of two bad moves.** The invariant `A >= n*L + M`
(0008) must still hold. With `L = 1500` and the current `A = 5000`, the
supported target count falls from `(5000-50) // 500 = 9` to
`(5000-50) // 1500 = 3`. The alternative is raising `A` — which 0008 records,
in its own words, as advice that "was wrong", because `A` is precisely how long
an already-acquired target keeps blocking its readers while the statement waits
for the next.

**`deadlock_timeout` is not ours to assume.** It is a server GUC, settable per
role, per database and per session. A default tuned to sit just above 1000 ms
would be correctly sized only on servers left at the default, silently
pointless on one set to 5 s, and silently harmful on one set to 50 ms. The
scaffold cannot read it at policy-construction time and has no business
depending on it.

**The observed cost is seconds, and it is bounded.** In the measured runs the
retry loop succeeded — three attempts, roughly fifteen seconds, publication
completed. That is a slow publication, not a failure.

## Consequences

- Autovacuum contention on a live target resolves by retry, never by
  PostgreSQL's cancellation. This is intended, not an oversight to be
  reconciled later.
- The staging `DROP` continues to benefit from the cancellation, because it
  runs with `lock_timeout = 0`. The inconsistency between the two paths is
  deliberate: the target lock is bounded to protect readers, the staging drop
  has no readers to protect. Do not "make them consistent".
- **Two claims in the code are now known to be false and need correcting.**
  `publish.py` states that `55P03` "means precisely 'a reader still held it
  when my budget expired'", and the terminal error tells the operator "A
  long-running reader is holding one of them." Autovacuum is not a reader, it
  is the measured cause here, and the message points an investigation at the
  wrong place. 0008's narrower claim — that `55P03` is unambiguous *as to its
  source*, because this code never issues `NOWAIT` on a private connection —
  remains true and is not affected.
- Publication latency under autovacuum contention is a few seconds and shows
  up as `publication lock unavailable` warnings. Those warnings are correct and
  should not be silenced.

## Known risk, not measured

An autovacuum that outlasts `retry_horizon_seconds` (default 60) would exhaust
the horizon and fail the run. On a large live target — which is exactly what a
publication has just written a million rows into — a vacuum can run for
minutes. Raising `L` above `deadlock_timeout` would resolve that case in about
a second.

This has **not** been observed. Every measured occurrence cleared within three
attempts. The decision above deliberately does not pre-empt an unmeasured
failure mode, and a run that fails this way is not destructive: source state is
unadvanced, staging is dropped, the next scheduled run republishes.

If it is ever observed, the fix is in "Rejected" below, and the escalating
variant is the one to reach for — not a larger fixed `L`.

## Rejected

**Raise the default `L` above `deadlock_timeout`.** Doubles or triples the
worst-case reader delay on every publication, in exchange for a faster
publisher in one contention scenario. Also drops the supported target count to
three, or forces `A` up. Refused on 0008's own grounds.

**Escalate `L` across attempts** — a short first attempt to fail fast against
a genuine reader, later attempts above `deadlock_timeout` to capture the
cancellation. Genuinely better than either fixed value: readers keep the
500 ms worst case on the common first attempt, and autovacuum contention
resolves on the second instead of consuming the horizon. The machinery is
already there, since `attempt_budgets_ms()` derives per-attempt budgets.

Refused for now only because the problem it solves has not been observed —
the retry loop has always converged — and it complicates the `A >= n*L + M`
invariant, which would have to hold per attempt with a varying `L`. This is
the right change if the horizon-exhaustion case above ever appears, and it is
recorded here so that it is reconsidered rather than rediscovered.

**Disable autovacuum on staging tables** (`autovacuum_enabled = false` as a
storage parameter). Addresses the wrong relation: the retries happen on the
*live* target, which task_core has just filled and which genuinely needs its
statistics refreshed. Under replacement the staging table also *becomes* the
live table by `RENAME`, so the setting would follow it into production unless
reset in the same transaction. Buys nothing for the case that motivated the
record.

**Set `deadlock_timeout` from task_core.** A server-wide GUC changed for the
convenience of one client, affecting every backend's deadlock detection. Out
of proportion, and out of scope for a scaffold.

## Verification status

Measured against PostgreSQL 18.4 on Windows, `deadlock_timeout` at its 1000 ms
default, `log_lock_waits = on`, `log_autovacuum_min_duration = 0`, with a
`pg_locks` / `pg_stat_activity` sampler recording the blocking pid and its
query at 5 ms resolution.

**Measured:**

- the target lock aborting at `lock_timeout` with `55P03`, its holder
  identified as an autovacuum worker on the live target;
- the staging `DROP` acquiring after 1009.929 ms following
  `canceling autovacuum task`, on the same table family in the same run;
- `_drop_open_transaction()` on every unsuccessful attempt, so no target lock
  is held across a retry sleep (code, and 0008's real-server run);
- `lock_timeout_ms = 500` and `deadlock_timeout = 1000` as the operative
  values.

**Documented, not measured here:** that PostgreSQL's autovacuum cancellation is
driven by the deadlock detector and therefore keyed to `deadlock_timeout`, and
that anti-wraparound autovacuum does not auto-cancel. The second does not arise
for staging tables, which live for one run.

**Reasoned, not measured:** the horizon-exhaustion risk above, and the claim
that escalating `L` would resolve it.
