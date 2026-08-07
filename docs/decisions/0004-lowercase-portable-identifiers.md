# 0004 — Portable identifiers are lower-case ASCII

Status: superseded in part by [0010](0010-require-portable-database-identifiers.md)

The lower-case portable grammar remains in force. ADR 0010 supersedes only
the per-pipeline `db_identifier_mode='quoted'` escape hatch. The original
reasoning is retained below as history.

## Problem

Table, schema and column names reach PostgreSQL as identifiers. The data
these tasks read has Cyrillic column headers, and PostgreSQL will accept
almost anything if it is quoted. The question is what the scaffold should
allow by default.

## Decision

`PORTABLE_IDENTIFIER_RE = ^[a-z_][a-z0-9_]*$`, in `types.py`, enforced on
declared identifiers at preflight and on actual column names at
publication. A pipeline may opt out per-spec with
`db_identifier_mode='quoted'`, which relaxes this pattern and nothing
else. The schema is always validated as portable regardless of any
pipeline's mode.

## Why

**Lower-case, not `[A-Za-z_]`.** Uppercase is what makes an identifier
case-fragile. SQLAlchemy quotes a mixed-case name to preserve it, quoting
defeats PostgreSQL's folding of unquoted identifiers, and `Sales` then
becomes a different object from `sales`:

```
'sales' -> sales        CREATE TABLE bsr.sales
'Sales' -> "Sales"      CREATE TABLE bsr."Sales"
```

So "portable" means something specific and useful: an identifier that
behaves identically whether quoted or not, and therefore never needs
quoting in hand-written SQL downstream.

**In `types.py`, not `db_publish.py`.** The convention is engine-neutral —
it is a scaffold rule about what names we choose to use, not a PostgreSQL
constraint. PostgreSQL's actual constraint is the 63-byte limit, which
stays in `db_publish.py` (`task_core/db/publish.py` since 0.7.4) along
with staging-name generation and normalization rules. `source_state.py` shares the pattern rather than
keeping a second copy.

**A mode, not a boolean.** `allow_unsafe_identifiers=True` would conflate
Unicode, quoting, punctuation, case sensitivity and actual SQL injection
safety into one word describing none of them.

**The schema is exempt from the escape hatch** because it is task-wide
while the mode is per-pipeline. Resolving a per-run value from per-spec
flags would need an arbitrary rule — strictest wins? any wins? — and a
non-portable schema is a deliberate run-level decision, not something one
pipeline should enable for every other.

## Consequences

- Cyrillic spreadsheet headers must be renamed before publication, in
  practice through `db_contract`. All existing tasks already did this;
  when the rule was introduced, all 145 published column names and all 13
  table names already satisfied it.
- Two names differing only in case are rejected on a case-sensitive
  filesystem where they would technically work. Accepted: the deployment
  target treats them as one anyway.
- **Enforcement covers both paths.** Preflight validates declared table,
  schema and column names for every pipeline. At runtime the same pattern
  applies to the payload's table name and columns under `portable` mode,
  and to its schema under either mode — so a directly constructed
  `DbPayload`, which never passes through preflight, is held to the same
  contract.
- The identifier-mode vocabulary lives in `types.py` beside the pattern,
  so `PipelineSpec` and payload validation share one definition rather
  than each carrying a literal tuple.
- Matching uses `fullmatch()`, not `match()`. Python's `$` also matches
  immediately before a trailing newline, so `match()` accepted `'foo\n'`
  as portable.
