# 0010 — Require portable database identifiers

Status: accepted

Supersedes the quoted-identifier escape hatch in
[0004](0004-lowercase-portable-identifiers.md).

## Problem

Quoted identifiers were added when publication wrote through a much simpler
path. At that point the incremental implementation cost was small, and the
project convention already used lower-case schema and table names.

Staged publication changed the cost. Exact names now cross staging-name
generation, prepared-artifact verification, bounded live-target locking,
ownership comments, deterministic cleanup, `DROP`/`RENAME`, and future bulk
load statements. A second identifier semantic became a permanent test and
correctness matrix despite no known task requiring it.

The lock phase also repeated a defect previously fixed in staging
verification: an assembled name passed to `to_regclass()` was parsed and
case-folded. A mixed-case table could therefore be reported missing and
excluded from the bounded lock. The recurring trap was reconstructing parser
input from identifiers already available as separate values.

## Decision

All database schema, table and published column names must satisfy:

```text
^[a-z_][a-z0-9_]*$
```

> **Narrowed by [0014](0014-allow-dots-in-published-column-names.md) in
> 0.7.5.** Published *column* names now follow
> `^[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*$`, permitting a dot between parts.
> Schemas, table names and generated relation names are unchanged, as is
> everything else in this decision. A dotted column is deliberately not
> portable in 0004's sense and must be quoted in hand-written SQL.

`db_identifier_mode` is removed from `PipelineSpec` and database payloads.
There is no deprecation shim and invalid identifiers are not normalized.
Cyrillic or otherwise non-portable source headers must be renamed before
publication, normally through `db_contract`.

Generated SQL continues to quote identifiers defensively. Defensive quoting
is an SQL-construction rule, not a user-selectable identifier semantic.

Exact relation identity is resolved by one small catalog primitive:

```sql
SELECT c.oid, c.relkind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = :schema
  AND c.relname = :table;
```

At introduction in 0.4.1, the primitive had exactly two callers:

- `_verify_prepared_artifacts()`;
- `_lock_publication_targets()`.

ADR 0009 later extends the same contained primitive to declared-target
compatibility preflight. It remains a small exact catalog lookup rather than a
resolver framework, factory or registry.

It returns `(oid, relkind)` or `None`. Each caller owns its own missing-object
policy. Both require `relkind = 'r'`: a view, materialized view, foreign table,
partitioned table, or other relation kind is rejected explicitly rather than
treated as a missing target. The resolver does not lock, quote, clean up,
cache, or introduce a relation framework.

## Why

The package owns its output naming convention. Supporting arbitrary existing
PostgreSQL names is not a demonstrated requirement, while the extra semantic
has already caused a live target to escape the bounded-lock phase. Removing it
aligns the public API with actual operating conventions and reduces the
correctness surface before COPY and declared-schema work add more SQL sites.

Exact catalog lookup remains useful even under portable-only naming: it avoids
search-path dependence and makes relation-kind validation explicit.

## Consequences

- `PipelineSpec(db_identifier_mode=...)` and direct payload
  `identifier_mode=...` calls fail immediately as unsupported arguments.
- Mixed-case, spaced, Unicode and punctuation-heavy database identifiers are
  rejected. Lower-case SQL keywords remain valid because generated SQL quotes
  identifiers defensively.
- Existing portable tasks require no source changes.
- Views and other non-ordinary relations cannot accidentally be mistaken for
  absent publication targets.
- Every future database path has one identifier contract while retaining safe
  SQL quoting.

## Rejected

**Keep quoted mode because quoting itself is easy.** Quoting is not the main
cost. The cost is preserving exact-name semantics across every catalog query,
artifact lifecycle and publication path.

**Retain an undocumented low-level escape hatch.** That would become a de
facto public API without a complete support contract.

**Silently lower-case or sanitize names.** That can publish to a different
object than the task author declared and hides configuration defects.
