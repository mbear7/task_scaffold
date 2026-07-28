# Decision records

Non-obvious, durable decisions, and the reasoning a future maintainer
would otherwise have to reconstruct — or would "simplify" and accidentally
break.

Each record answers: what problem existed, what was decided, why, what
follows from it, and what was rejected.

This is not a changelog and not a history. Decisions that were superseded
are marked as such rather than deleted, because the reasoning that led to
them is usually still relevant to whatever replaced them.

| | |
| --- | --- |
| [0001](0001-replace-tables-instead-of-truncating.md) | Replace published tables instead of truncating them |
| [0002](0002-keep-core-tests-independent-of-tasks.md) | Keep the core test suite independent of task files |
| [0003](0003-gc-collect-for-remote-workbook-handles.md) | Releasing a workbook requires `gc.collect()` |
| [0004](0004-lowercase-portable-identifiers.md) | Portable identifiers are lower-case ASCII |
| [0005](0005-prepare-staging-outside-the-publication-transaction.md) | Prepare staging tables outside the publication transaction |
| [0006](0006-three-rules-that-keep-cleanup-safe.md) | Three rules that keep staging cleanup safe |
| [0007](0007-excel-output-is-a-debugging-aid.md) | Excel output is a debugging aid, not a publication target |
| [0008](0008-bound-the-publication-lock-wait.md) | Bound the publication lock wait |
