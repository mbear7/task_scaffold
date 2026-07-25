# 0002 — Keep the core test suite independent of task files

Status: accepted

## Problem

`task_core` is exercised in practice by `tasks/`, which are large, import
in-house helper modules that are not part of this project, and read real
workbooks from SMB paths that do not exist outside the production
environment.

## Decision

The test suite covers `task_core` only. `tasks/` has no automated
coverage. Tests import nothing from `tasks/` and never require network
access, a database, a real remote share, or any module this project does
not ship.

## Why

A suite that needed external helper modules and real files could not run
at all in most environments — including any sandbox, any CI runner, and any new
developer's machine before they had share credentials. It would be
skipped, and a skipped suite protects nothing.

Keeping the boundary at `task_core` also keeps the tests honest about what
they cover. A test that passes because a fixture workbook happened to have
the right shape tells you little about the scaffold.

## Scope note

`examples/` is inside the boundary, `tasks/` is outside it. Tests import
`examples/local_task.py` and run it, because a documented quick start that
nobody can execute is worse than none — the previous documentation's
"minimal task" called an undefined function and pointed at an SMB path,
and nothing caught it.

That does not weaken the rule. `examples/` is held to the same
import restriction as `task_core` itself, enforced by
`tests/test_standalone.py`, and the example is separately checked by AST
to import nothing the quick start disclaims. The allowance is transitive
rather than an exemption.


## Consequences

- **Task files are verified by running them**, not by tests. That is a
  real gap, and it is largest for the biggest file in the repository.
- Bugs in scaffold behaviour that only appear with real data are found in
  production. Several have been; each became a `task_core` regression test
  once reproduced with a synthetic case.
- Test doubles must model real library behaviour carefully, because
  nothing else will catch a fake that is more permissive than the library
  it stands in for. This has failed at least once: a fake connection
  accepted a call that real SQLAlchemy rejects, leaving a production-
  breaking bug invisible to the whole suite.
- **A fix without a test that fails when the fix is reverted is not
  finished.** Because there is no integration layer to catch mistakes,
  every regression test is checked by reverting the fix and confirming the
  test fails for the right reason.
