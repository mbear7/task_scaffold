# 0007 — Excel output is a debugging aid, not a publication target

Status: accepted

## Problem

The scaffold writes two kinds of output: PostgreSQL tables and Excel
workbooks. The DB path has been given staging tables, a short publication
transaction, ownership metadata, predecessor cleanup and an advisory lock.

The Excel path has none of that. Workbooks are written immediately inside
the pipeline loop with `toxlsx()`, so a failure later in the run can leave
new files on disk while the database is rolled back or left at its
previous state.

Read as a symmetry problem, that looks like an unfinished half of the
publication design, and the question of "staged filesystem publication"
keeps returning: temporary files, atomic renames, per-run directories,
cleanup of abandoned artifacts.

## Decision

**Excel output is for local debugging. It is not a production
publication target.**

Concretely, and permanently:

- no staging files;
- no temporary files;
- no atomic renames;
- no cleanup of abandoned Excel artifacts;
- no transactional relationship of any kind with database publication.

A workbook is written where it is asked for, when the pipeline produces
it. That is the whole design.

## Why

The asymmetry is not an oversight. The two outputs are not the same kind
of thing.

A published table is consumed by dashboards, reports and other tasks
that must never see partial or stale data — which is what the entire
staged model exists to guarantee. A workbook is opened by a person who
wants to see what a pipeline produced. Nothing downstream depends on it,
because nothing downstream is *permitted* to depend on it.

Building staged filesystem publication would import the DB path's whole
cost — temporary naming, ownership metadata, orphan cleanup, a scavenger
for artifacts left by killed runs, and a second set of failure modes on a
filesystem that may be a remote SMB share — in order to give
transactional guarantees to files whose only consumer is a human
eyeballing them.

That is a large amount of machinery, and every piece of it can fail in
ways that would then need their own handling. Paying it for a debugging
aid is the wrong trade.

## Consequences

- **A failed run may leave workbooks from pipelines that succeeded before
  the failure.** This is expected. Delete them, or re-run.
- **Excel files may disagree with the database.** A run that writes
  workbooks and then fails during publication leaves fresh files and stale
  tables. Also expected — the database is the source of truth, and the
  workbook is a snapshot of what one pipeline computed.
- **Nothing downstream may read these files programmatically.** If
  something needs the data, it reads the published table. A scheduled job
  that consumes a workbook this scaffold wrote is depending on something
  with no delivery guarantees, and this decision is what it is depending
  against.
- **Two pipelines declaring the same `excel_name` are still rejected**, by
  `validate_pipeline_classes()`. That check is not about publication
  guarantees; it is about a task declaring something incoherent, and it
  stays.
- **`output_excel` defaults to `False`**, matching `output_db`. A task
  that declares no outputs produces none, and each is switched on
  deliberately. It defaulted to `True` until 0.3.6, which contradicted
  this decision in the one place a reader would actually notice.

## Rejected

**Staged filesystem publication** — write to a temporary name, rename on
success. Rejected above.

**Deferring all Excel writes to the end of a successful run.** Cheaper
than staging, and it would keep files consistent with the database. But it
holds every workbook in memory until the run completes, which is the
memory profile the staged model was specifically shaped to avoid, and it
removes the property that makes Excel output useful for debugging in the
first place: that the file exists as soon as its pipeline has run, even if
a later pipeline fails.

**Writing workbooks only when the DB publication commits.** Same objection,
plus it would make a debugging aid unavailable in exactly the case where
someone most wants to look at it — a failed run.
