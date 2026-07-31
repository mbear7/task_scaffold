# 0001 — Replace published tables instead of truncating them

Status: accepted as the default for both schema sources; explicit declared refill is the opt-in exception from [0009](0009-add-fully-declared-output-schemas.md), separated by [0012](0012-separate-publication-strategy-from-schema-source.md)

## Problem

Publishing a table needs to make the new data live. Two families of
approach: keep the table and replace its rows (`TRUNCATE` + `INSERT`, or
`DELETE` + `INSERT`), or replace the table itself (`DROP` + `CREATE`, or
staging plus swap).

## Decision

Replace the table by default. The prepared staging table already has the
resolved schema: inferred from data when `output_schema` is absent, or built
from the declaration when it is supplied. Publication drops the old relation
and renames staging into place.

A declared pipeline may explicitly choose stable refill when preserving the
ordinary table object is worth the second write and row-dependent lock window.
That exception does not change replacement as the default.

## Why

Keeping the table requires its existing schema to remain compatible with
the new data. That pushes migration onto every task author: altering
column types, adding new columns, removing obsolete ones, and — hardest —
telling deliberate schema evolution apart from accidental drift. For a
scaffold whose purpose is to remove repeated work from reporting tasks,
that is the wrong burden in the wrong place.

For inferred outputs, replacement makes schema evolution automatic and
invisible. A column that gains its first decimal value becomes `numeric`
without anyone doing anything. For declared outputs, replacement makes the
current declaration authoritative without requiring an in-place migration.
In both cases it writes the dataset once and keeps the locked publication work
to catalog operations.

## Consequences

- **Grants are not preserved.** A fresh table gets default privileges.
  `ALTER DEFAULT PRIVILEGES` on the schema is the answer, and it is
  configuration rather than code.
- **Dependent views break the publish.** `DROP TABLE` fails when a view
  depends on the table, and `CASCADE` would destroy the view. Views and replacement publication are mutually exclusive unless an
  indirection or another explicit publication strategy is used; this is a
  property of the decision, not a defect in it.
- **Inferred column types are data-dependent.** An inferred table's schema can
  differ between runs. For downstream consumers, pin selected types with
  `db_type_overrides` or declare the complete user schema with `output_schema`.
- **Type inference must be right**, because nothing downstream will catch
  it being wrong. Sampling the first 5000 rows was not sufficient: a
  column whose sample is all integers and whose later rows contain a
  decimal inferred `bigint`, and PostgreSQL's assignment cast silently
  rounded the value on insert. Sampled inference is now verified against
  the remaining rows for the two types PostgreSQL can silently widen.

## Rejected

**`TRUNCATE` + `INSERT` as the default** — requires schema compatibility and
writes every row a second time while the live table is locked. Explicit
declared refill retains it only for targets whose stable ordinary-table
identity and attached objects justify that price.

**Direct `DROP` + `CREATE` inside the pipeline loop** — how this worked
originally. Correct, but it took an `ACCESS EXCLUSIVE` lock on the live
table at its first publish and held it until the run's single commit,
which on a task publishing several tables from remote workbooks made the
first table unavailable for nearly the whole run. Replaced by staging plus
swap, which is the same decision with a shorter lock window.
