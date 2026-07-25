# 0001 — Replace published tables instead of truncating them

Status: accepted

## Problem

Publishing a table needs to make the new data live. Two families of
approach: keep the table and replace its rows (`TRUNCATE` + `INSERT`, or
`DELETE` + `INSERT`), or replace the table itself (`DROP` + `CREATE`, or
staging plus swap).

## Decision

Replace the table. Column types are inferred from the data on every run,
and the new table is created with whatever schema that inference produced.

## Why

Keeping the table requires its existing schema to remain compatible with
the new data. That pushes migration onto every task author: altering
column types, adding new columns, removing obsolete ones, and — hardest —
telling deliberate schema evolution apart from accidental drift. For a
scaffold whose purpose is to remove repeated work from reporting tasks,
that is the wrong burden in the wrong place.

Replacement makes schema evolution automatic and invisible. A column that
gains its first decimal value becomes `numeric` without anyone doing
anything.

## Consequences

- **Grants are not preserved.** A fresh table gets default privileges.
  `ALTER DEFAULT PRIVILEGES` on the schema is the answer, and it is
  configuration rather than code.
- **Dependent views break the publish.** `DROP TABLE` fails when a view
  depends on the table, and `CASCADE` would destroy the view. Views and
  freely-evolving schemas are mutually exclusive; this is a property of
  the decision, not a defect in it.
- **Column types are data-dependent.** A table's schema can differ between
  runs. For any table with downstream consumers, pin types with
  `db_type_overrides`. Inference is a convenience for exploratory tables.
- **Type inference must be right**, because nothing downstream will catch
  it being wrong. Sampling the first 5000 rows was not sufficient: a
  column whose sample is all integers and whose later rows contain a
  decimal inferred `bigint`, and PostgreSQL's assignment cast silently
  rounded the value on insert. Sampled inference is now verified against
  the remaining rows for the two types PostgreSQL can silently widen.

## Rejected

**`TRUNCATE` + `INSERT`** — requires schema compatibility, which is the
burden this decision exists to avoid. It would preserve grants and views,
which is genuinely valuable, but not at that price.

**Direct `DROP` + `CREATE` inside the pipeline loop** — how this worked
originally. Correct, but it took an `ACCESS EXCLUSIVE` lock on the live
table at its first publish and held it until the run's single commit,
which on a task publishing several tables from remote workbooks made the
first table unavailable for nearly the whole run. Replaced by staging plus
swap, which is the same decision with a shorter lock window.
