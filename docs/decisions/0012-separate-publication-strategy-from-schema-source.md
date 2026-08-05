# 0012 — Separate publication strategy from schema source

Status: accepted
Amends: 0009

## Problem

0009 introduced declared output schemas and, with them, a stable-target
publication mechanism: `TRUNCATE` the live table and insert from staging,
so the target's OID never changes and views, grants, indexes, ownership
and triggers survive.

It made that mechanism **mandatory** for declared schemas. The presence of
`output_schema` selected both the schema source and the publication
strategy at once, which 0009 defended as making contradictory
configuration unrepresentable.

That conflated two orthogonal questions:

- **where does the shape come from** — inferred from data, or declared;
- **how does new data replace old** — replace the relation, or refill it.

Three consequences followed.

**The fastest combination was unreachable.** Declared + replace does one
database write instead of two, and holds a catalog-time lock instead of
one proportional to row count. Nothing about declaring a schema requires
paying refill's cost.

**Refill's cost landed on choices unrelated to it.** Schema mode is mostly
a working preference between team members, and only sometimes a
requirement from a consumer. So a reader-blocking window proportional to
row count was being imposed by a preference that has nothing to do with
reader impact.

**A declaration could not be edited.** Changing `output_schema` on an
existing target makes the compatibility check refuse it and demand manual
migration. Under replace, the declaration is authoritative and a change
simply takes effect.

## Decision

`PipelineSpec.db_publication_strategy`, defaulting to `None`, which
resolves to `replace` for **both** schema sources.

| schema source | replace | refill |
| --- | --- | --- |
| inferred | **default** | rejected |
| declared | **default** | optional |

`refill` requires `output_schema` and is rejected at both spec construction
and the direct payload/publisher boundary without it — with the reason, not
just the fact. Unknown strategy values are rejected rather than silently
falling through to replacement.

`partition` is **absent from the vocabulary**, not reserved and rejected.
An accepted value that raises `NotImplementedError` is its own small lie.
It is added when it is built.

## Why refill is excluded for inferred schemas

Refill truncates the live table and inserts into it, so the target's
physical schema must be stable across runs. Only a declaration can promise
that; an inferred schema changes whenever the data does.

Permitting the combination and documenting the hazard would produce a job
that works until a column widens and then fails on every run. Rejecting it
at construction keeps 0009's principle intact — incoherent configurations
unrepresentable rather than validated — while separating the axes it
originally conflated. Three legal combinations out of the four possible schema/strategy pairs.

## Why the default is replace for both

Replace has been the publication mechanism since 0001 and remains the one
whose costs are best understood: one write, a catalog-time lock, and the
staged preparation that ADR 0005 exists to protect.

Refill buys OID stability and pays for it with a locked window
proportional to row count. That is a good trade when something is attached
to the table — and a pure cost when nothing is. It should be requested,
not inherited.

## Consequences

- **The same spec now behaves differently.** A declared output that
  published by refill in 0.5.0 publishes by replace in 0.5.1 unless it
  asks for refill. Views fail loudly, because `DROP` errors on dependents;
  **grants, ownership, indexes and triggers are lost silently**. The bundled
  representative tasks use inferred schemas and need no source change. Private
  running scripts must be searched for `output_schema=` and reviewed using
  the migration checklist.
- Switching an existing target from refill to replace has the same effect,
  permanently and by deliberate act. The published-table provenance
  comment could carry the strategy and let replace refuse a target its own
  predecessor published as stable. Not built; recorded because the hazard
  outlives this release.
- Implementation was a reversal, not a mechanism. `_build_table()` already
  builds staging from the same `ResolvedSchema` in both modes, so a
  declared staging table is already exactly the declared shape and
  `RENAME` produces a correct target with no further work. What was
  coupled was only the *selection*: a set populated from
  `resolved_schema.source == 'declared'`, now populated from
  `payload.publication_strategy == 'refill'`.
- In the 0.5 implementation, `db_publication_strategy` was appended after all
  existing fields to preserve positional construction. ADR 0013 supersedes
  that constructor-compatibility rule in 0.7.0: `PipelineSpec` is keyword-only.
- Direct `DbPayload`, `from_petl()` and `from_pandas()` callers receive the
  same closed-vocabulary and inferred/refill validation as `PipelineSpec`; the
  mutable payload is checked again at `publish()` as defense in depth.
- ADR 0011's spool design assumed the coupling. Reversing it before COPY
  lands avoids reworking both.

## Rejected

**Keeping the coupling and documenting the cost.** The choice is made on
preference grounds unrelated to reader impact, so documentation would not
reach the person making it.

**A `schema_mode` field alongside a strategy field.** 0009 was right that
`output_schema`'s presence should select the schema source; making it
explicit adds a way to contradict yourself for no gain. Only the strategy
became explicit.

**Reserving `partition` as a rejected value.** Documents intent at the
cost of a vocabulary entry that does not work. Absence is honest, and
adding a value later breaks nothing.
