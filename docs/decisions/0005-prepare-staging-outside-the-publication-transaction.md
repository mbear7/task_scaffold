# 0005 — Prepare staging tables outside the publication transaction

Status: accepted; publication-duration wording amended by 0009 and 0012

## Problem

Publication was one transaction for the whole run. It opened at the first
`publish()` and committed at the end, after every pipeline had executed.

That gave clean atomicity, and it made rollback the entire cleanup
mechanism: a killed backend, a dropped connection or an OOM kill left
nothing behind, with no code involved.

It also meant the transaction stayed open across remote file reads,
transformations and Excel exports. A transaction that long holds catalog
locks, delays vacuum, accumulates WAL, and makes a late rollback
expensive — all of it scaling with how long the run takes rather than with
how much it writes.

The staging swap (see 0001) fixed the *live-table lock*, which was the
visible symptom. It did not shorten the transaction.

## Decision

Split the run into many committed preparation transactions and one atomic
publication transaction. That final transaction is normally short for
replacement; explicit stable refill is the deliberate row-dependent exception.

```
for each pipeline:
    run it, write Excel
    if it has a DB target:
        BEGIN
          create staging table, load it, verify it, comment it
        COMMIT

BEGIN
  verify every prepared artifact
  perform queued work (source state)
  publish every staging table by replacement or explicit refill
  replace staging comments with provenance
COMMIT
```

A staging table is created, loaded, validated, commented and committed in
**one** transaction. Therefore committed + owned means publishable, and
there is no `ready` flag to track — no window exists in which a committed
staging table is incomplete.

## Why the split lands where it does

Preparation can afford O(rows) work: it is already loading the dataset. For
replacement, publication remains O(number of tables), which is what keeps the
transaction short. Explicit refill deliberately performs a second O(rows)
write after locking; ADR 0009 documents that trade and ADR 0012 makes it
opt-in rather than a consequence of declaration. Every validation check still
sits before the first live-target lock where possible.

Preparation verifies exact ordered column names, and that the number of
rows loaded equals the number in the payload. Publication verifies only
identity: that each staging artifact still exists and still carries this
run's ownership metadata. It deliberately does not recount rows — the
count recorded at preparation is carried into the `RunResult` as metadata,
not compared against anything, because comparing a recorded number to
itself proves nothing and comparing it to a live count is the recount just
excluded.

Ordered column names, not types. Ordinal position is trustworthy because
the staging table was just created; `attnum` develops gaps after
`DROP COLUMN`, so this check would need care on a reused table and needs
none on a fresh one. Type comparison would require a
SQLAlchemy-to-`information_schema` map that drifts silently when it is not
maintained.

The row count is authoritative because the payload is fully materialized
before any insert begins — `len(payload.rows)` is the exact set of dicts
handed to the driver, not an expectation carried from the source. It is
counted in the chunking loop rather than taken from the driver, because
SQLAlchemy reports `supports_sane_multi_rowcount = False` for psycopg2, so
a driver count would be measuring the rewritten statement. The check
guards our chunking and says so, rather than claiming to guard the
database.

## Consequences

- **Rollback is no longer the cleanup mechanism.** Preparation
  transactions are committed, so `rollback()` must DROP this run's staging
  tables. That makes it capable of failing for new reasons — notably a
  lost connection — so it never raises: the exception that caused the
  abort matters more than a cleanup failure.
- **Cleanup becomes correctness, not hygiene.** See 0006 for the rules
  that make it safe.
- **The source-state write must be queued, not called.** It has to land in
  the publication transaction or a failed publication could still advance
  the stored fingerprints. It is queued in a `PublicationPlan` supplied at
  construction, so `commit()`'s signature and the publisher protocol stay
  unchanged.
- **Peak storage doubles for published data** while old and new tables
  coexist.
- **The publisher protocol grew by exactly one member**, `begin_run()`.
  Lock acquisition and predecessor cleanup are folded into it because they
  are one precondition, not two.
- Business validation — non-emptiness, key uniqueness, value ranges —
  stays with tasks. Putting it here would make every task pay for one
  task's rule.

## The extension seam, and the plan for it

`publisher_factory` now takes six parameters. The 0.3.0 break that added
three was deliberate and handled — minor bump, changelog, migration note —
but the seam is visibly under growth pressure, and `IdentifierPolicy` was
itself created to stop two independently-defaulted integers drifting
apart.

**The next parameter that wants in should not be a parameter.** Fold the
lot into one frozen `PublisherConfig` and the signature stops breaking
forever. Doing it now would be a gratuitous second break for no present
benefit; doing it as part of the third addition costs nothing extra. This
is written down so that addition triggers the change rather than another
parameter.

**Executed in 0.4.0.** `PublicationLockPolicy` (see 0008) was the addition
that triggered it. `publisher_factory` and `db_max_identifier_bytes` were
removed from `run_pipelines()` and became fields of a frozen
`PublisherConfig`, in one deliberate break with no compatibility aliases.
Per-task facts — `creds`, `pg_schema` — stayed direct arguments: a task
widening a lock timeout should not have to restate its credentials.

## Rejected

**Keeping one transaction and accepting the duration.** Defensible while
runs are short. Rejected because the failure mode arrives without warning
at a data size nobody predicted, and by then the infrastructure to
diagnose it usually is not in place.

**Collecting every payload first, then staging them.** Would hold every
materialized payload for the whole task in memory simultaneously. Staging
inside the pipeline loop releases each payload before the next pipeline
starts.

**A `ready` flag on staging artifacts.** Redundant once validation happens
inside the preparation transaction: committed already implies validated,
so the flag would be a state with one possible value.
