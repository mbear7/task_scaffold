# 0006 — Three rules that keep staging cleanup safe

Status: accepted

## Problem

Committed staging tables (0005) can outlive the run that created them: a
process is killed, a connection drops, a backend is reaped. Something must
remove them, and removing the wrong thing is worse than leaving it.

There is also a subtler failure the staged model *introduces*. Under one
run-long transaction, an older run could never publish over a newer one —
its work died with its transaction. With committed staging, a stalled run
could in principle wake and publish stale data over fresh.

## Decision

Three rules, which only work together:

1. **The task lock is session-scoped.** `pg_try_advisory_lock` on the
   publisher's own connection, held for the whole run across every
   preparation commit.
2. **Connection loss is fatal.** `ensure_connection()` refuses to
   reconnect after the session is gone, rather than silently continuing
   without the lock.
3. **Cleanup runs under the lock**, and drops only artifacts it can
   *positively identify* as this task's.

## Why they interlock

Trace the stale-publisher case:

- A stalled process whose session is still alive still holds the lock, so
  nothing newer can start. Nothing stale to publish over.
- If the server reaps the session instead, the lock releases **and the
  connection dies with it** — so rule 2 stops that process from executing
  anything on waking.
- A newer run then acquires the lock and, under it, drops the
  predecessor's staging artifacts. Even a resumed run finds them gone at
  its publication check.

So predecessor cleanup doubles as the generation guard, and no generation
column is needed. Remove any one rule and the trap reopens. That is
exactly the shape a future maintainer would "simplify".

## Why no age threshold

Cleanup runs while holding the lock, which means no other run of this task
is live. Any staging artifact positively identified as this task's
therefore belongs to a run that is gone. There is no timestamp to compare,
no window to tune, and no race with a running peer.

Scheduled age-based cleanup is left with a much smaller job — tasks that
are decommissioned or never run again — which can afford to be
conservative because the common case is already handled.

## Ownership metadata

Compact, versioned JSON in `COMMENT ON TABLE`, read back with
`obj_description()`. No registry table, therefore no second source of
truth to fall out of sync, and no identifier budget spent.

`ALTER TABLE ... RENAME` preserves comments, so the swap replaces the
staging comment with live-table provenance. Without that, every published
table would carry staging ownership metadata and look to cleanup like an
abandoned artifact. The replacement is also genuinely useful: "which run
produced this data" answered from the catalog is the question actually
asked when a number looks wrong.

**Every documented field is required, and its type checked.** Accepting a
comment that carried only the marker, version, task and run meant metadata
failing the documented format still authorized a drop. Extra fields are
tolerated, so a later version can add one without older code refusing to
recognise its own artifacts.

**The physical name must have the exact staging shape**
(`__stg_<8 hex>_<8 hex>`), **and the name and the comment must agree with
each other** — same run token, same schema, and a target token that
actually hashes from the comment's logical target. The catalog scan uses a
broad `LIKE` because SQL cannot express the token shapes, so without this
any table merely containing the infix could be dropped on the strength of
a syntactically valid comment.

The readable prefix is deliberately not recomputed: it may have been
truncated under a different configured identifier limit, and comparing it
would refuse to clean up artifacts this project genuinely created.

**Preparation never erases an object already at the generated name.** It
creates, and lets `relation already exists` fail loudly. Dropping first
bypassed this entire rule: an object with no comment, an invalid one, or
another owner was erased anyway.

**An unparseable or unrecognised comment means ownership is UNKNOWN, and
unknown is never dropped.** A parse failure must not fall through to a
drop; that is the failure mode that turns cleanup from hygiene into an
outage.

## Consequences

- **`pg_try_advisory_lock`, not the blocking form.** A schedule firing
  faster than the task runs would otherwise build an unbounded queue,
  which is worse than the collision.
- **Losing the race is a skip, not an error.** A cron overlap is expected
  operation and should compose with the sources-unchanged skip rather than
  paging someone. Logged at WARNING, because chronic overlap means the
  schedule is wrong even when each skip is correct.
- **The lock records an identity, not a flag.** A boolean proved only
  that *some* lock was held, so a publisher holding task A's lock would
  clean task B's artifacts on request — the same cross-run risk, reached
  through a direct caller instead of through ordering. The lock methods
  take no task argument at all now: there is nowhere to name a task other
  than the publisher's own, and `_require_task_lock()` verifies the held
  identity matches.
- **`task_name` is required and must be usable.** The lifecycle cannot
  operate without one: an empty name derives an advisory key from `''` and
  writes ownership metadata that this module's own parser rejects, so the
  run prepares successfully and declares its own artifact unowned at
  publication.
- **Every invariant is enforced where it lives, not by caller ordering.**
  `cleanup_predecessor_artifacts()` requires the lock itself, because it
  drops tables and a direct caller could otherwise delete another live
  run's artifacts. `task_name` is validated at construction, because an
  empty one writes an ownership comment its own parser rejects — the run
  stages successfully and then reports its own artifact as unowned at
  publication, after every pipeline has run.
- **The lock key is namespaced.** Advisory locks are database-wide and
  shared with anything else using them; a bare `hash(task_name)` could
  collide with an unrelated application and present as this task
  mysteriously refusing to run.
- **Cleanup never reconnects.** `rollback()` and `release_task_lock()`
  use the connection directly, so they bypassed the check in
  `ensure_connection()` — confirmed directly, `rollback()` ran `DROP TABLE`
  and `COMMIT` on an invalidated connection. Both now transition to the
  terminal lost state and do nothing: PostgreSQL releases a session-scoped
  lock when the session dies, and the next run removes the artifacts under
  its own lock. Attempting cleanup would only force a reconnect onto a
  session holding no locks, which is the failure this whole decision
  exists to prevent.
- **Invalidation is detected, not merely representable.** SQLAlchemy will
  transparently reconnect an invalidated `Connection` on the next
  statement, onto a session holding none of this run's locks. The
  publisher checks `conn.invalidated` before every reuse and transitions
  to the terminal lost state; `publish()` and `commit()` additionally
  refuse on PostgreSQL when the lock is not held, so the contract holds
  for direct callers and not only through the runner.
- **Session-scoped means `NullPool` matters.** The connection must never
  return to a pool while the lock is held. `make_engine()` uses
  `NullPool`, so this holds today — and `close()` issues an explicit
  `pg_advisory_unlock` rather than relying on session end, to keep it
  honest if that ever changes.
- **Excel-only tasks get no concurrency protection.** The lock lives on
  the publisher's connection, which only exists when a task touches the
  database. Such a task has the same overwrite problem on its output file;
  solving it would need a separate mechanism with separate failure modes.
- **Cross-task target ownership is unsupported.** A PostgreSQL publication
  target belongs to exactly one task. The provenance comment makes a
  violation *detectable* at near-zero cost, but nothing enforces it.
