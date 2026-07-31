# 0009 — Add fully declared output schemas

Status: accepted (publication-strategy coupling reversed by 0012)

Implemented in 0.5.0.

## Problem

`task_core` originally had one PostgreSQL schema model: infer the complete
output schema from the produced data, optionally overriding selected physical
types.

That remains useful for exploratory and adaptive outputs, but it is a poor
contract for long-lived BI and integration tables:

- an empty output cannot prove types from values;
- all-`NULL` columns are ambiguous;
- schema changes may be caused by data changes rather than code changes;
- column nullability is implicit;
- replacing the table on every run discards object identity, grants and
  indexes, and prevents dependent views.

A complete manual schema must therefore be a separate contract rather than a
larger collection of inference overrides.

## Decision

Support two schema sources:

```text
output_schema is None
→ infer the complete schema, with optional type and NOT NULL overrides

output_schema is supplied
→ use the fully declared user schema, append enabled framework columns,
  and disable inference
```

There is no `schema_mode` field. The presence of `output_schema` is the mode
selection and avoids contradictory configurations.

Both paths produce the same internal `ResolvedSchema` / `ResolvedColumn`
representation. Staging DDL, ordered row loading and publication consume that
single representation and do not branch on the public configuration shape.

## Public declaration

```python
OutputColumn(
    name='customer_id',
    type=sa.BigInteger(),
    nullable=False,
)
```

`nullable` defaults to `True`. Nullable columns are the common ETL case;
`nullable=False` is the meaningful constraint and remains explicit.

`output_schema` must:

- contain at least one column;
- contain unique portable lower-case names;
- define the complete user-output column set and order;
- use supported SQLAlchemy type instances, SQLAlchemy type classes or the
  existing string type aliases.

It is mutually exclusive with:

- `db_output`;
- `db_type_overrides`;
- `db_not_null_columns`.

`db_output` remains an inferred-mode-only, declarative convenience. A static
`db_contract` may be used before declared validation because its target names
are the final PostgreSQL names. `get_dynamic_db_contract()` is rejected with
`output_schema`: a runtime-changing projection is incompatible with a static
complete schema. That conflict is rejected during structural pipeline
validation, before resources are built.

## Inferred-mode nullability

Inferred columns remain nullable by default. A task may mark selected inferred
columns `NOT NULL`:

```python
PipelineSpec(
    db_table='customers',
    db_not_null_columns=('customer_id',),
)
```

This does not create a hybrid declared mode. The framework still infers the
column set and types; only the listed columns gain a nullability constraint.

Framework-generated technical columns are framework-owned and sit outside the
user declaration. `db_updated_at=False` adds no timestamp column.
`db_updated_at=True` appends the default `etl_updated_at` column, while a string
supplies a custom portable lower-case column name. In either schema mode the
column is appended after user columns as `TIMESTAMPTZ NOT NULL` and must not be
repeated in `output_schema`, `db_type_overrides`, `db_not_null_columns`, or the
produced user columns. Therefore the final physical schema is the resolved user
schema plus enabled framework-owned columns.

## Strict row validation

The existing `_normalize_value()` semantics run first. Scalar missing markers
such as `None`, NaN, `pd.NA`, `pd.NaT` and NumPy `NaT` normalize to SQL `NULL`.
A normalized `NULL` violates a non-nullable column during staging preparation;
the live target is never touched.

Declared validation is intentionally strict and performs no implicit
cross-family parsing or lossy conversion. The initial supported families are:

- Boolean;
- `SMALLINT`, `INTEGER`, `BIGINT`;
- floating point;
- `NUMERIC` / `DECIMAL`;
- text and bounded variable-length strings;
- binary;
- date;
- timestamps with and without timezone.

Examples:

- Python `int` is valid for integer types and `NUMERIC` when it fits;
- Python `Decimal` is valid for `NUMERIC` when precision and scale fit without
  rounding;
- Python `float` is not implicitly converted to `NUMERIC`;
- `datetime` is not implicitly converted to `DATE`;
- strings are not parsed into numeric, Boolean, date or timestamp values;
- aware datetimes are required for `TIMESTAMP WITH TIME ZONE`;
- naive datetimes are required for `TIMESTAMP WITHOUT TIME ZONE`.

PostgreSQL remains the final authority for database constraints and
backend-specific adaptation. A rejection by constraints present on staging
rolls back the complete preparation transaction; target-only constraints and
triggers on explicit refill are handled later as described below.

### 0.5.2 type-shape clarification

Declared type parameters are validated before DDL so SQLAlchemy cannot silently
render a different PostgreSQL type. Supported parameterized shapes are
`Float(p)` with integer `1..53`, `String(n)` with positive integer length, and
`Numeric(p[, s])` with integer precision `1..1000` and the deliberately narrow
subset `0 <= s <= p`. Scale without precision, bounded `LargeBinary`, text
collation and non-integer parameters are rejected. NUL text is rejected during
row validation with framework context rather than leaking a driver error.

## Column matching and ordering

A declared output may produce its columns in a different order. The framework:

1. compares the complete produced and declared column sets;
2. rejects missing or unexpected columns;
3. reorders values into declaration order before staging.

Empty outputs are valid because the schema comes from the declaration. An
empty declaration is invalid.

## Publication-strategy amendment

ADR 0012 changed the default in 0.5.1. `output_schema` now selects only the
schema source; both schema modes use replacement by default. The stable-target
mechanism below remains available only through explicit
`db_publication_strategy='refill'`.

Stable refill deliberately trades the inferred/replacement path's short,
row-independent swap window for the identity of the same ordinary table and
its attached database objects. Its reader-blocking window scales with rows,
indexes and database-side enforcement.

## Explicit stable refill for declared targets

Both schema sources use the existing staged replacement publication from ADR
0001 by default. When a declared output explicitly selects `refill`, it uses a
permanent ordinary logged target table as described in this section.

### First publication

When the target is absent, the publication transaction:

1. verifies the prepared staging artifact;
2. creates the permanent target from the resolved declared schema;
3. fills it from staging;
4. applies the framework-owned publication comment;
5. drops staging;
6. commits.

The target is never committed empty or partially filled. In a mixed publication, all absent explicit-refill targets are created and filled before the first existing live-target lock.

### Existing target

Before locking, the target must be an ordinary table and must exactly match the
prepared staging table's PostgreSQL catalog metadata:

- ordered column names;
- type OIDs;
- type modifiers such as numeric precision/scale and varchar length;
- nullability;
- relevant collation;
- identity and generated-column metadata;
- presence of a column default (`atthasdef`). Defaults are not declared in
  this release, so a target default is incompatible rather than silently
  ignored.

No widening compatibility and no automatic migration are performed. Views,
materialized views, foreign tables and partitioned tables are rejected
explicitly.

External incoming foreign keys are rejected before locking. The framework
never uses `TRUNCATE ... CASCADE` and does not coordinate dependent-table
refreshes in this release.

Stable refill deliberately writes every row twice: first into the committed
logged staging table and then into the stable target. This consumes additional
storage, WAL and I/O, but moves source processing, normalization, declared
validation and the first database load before the live lock, preserves the
target object, and lets publication retries reuse the prepared artifact. Peak
storage is at least the existing target plus staging; indexes, WAL and
transactional state can make the transient peak higher.

Target-owned indexes do not create semantic validation errors, but they extend
refill time. Primary/unique/check/exclusion constraints and triggers that exist
only on the live target are not reimplemented during staging validation; they
may reject the second insert inside the publication transaction. Such a failure
is later and less specific, but remains atomic and restores the old live
contents.

The publication transaction then:

```text
complete source-state and preparatory work
→ acquire every existing live-target lock in deterministic sorted order
→ TRUNCATE declared target
→ INSERT FROM staging
→ update framework provenance comment
→ drop staging
→ commit
```

The target OID, views, indexes, grants, ownership, triggers and row-level
security remain attached to the same object. The table comment remains
framework-owned and is updated on publication.

## Locking consequence

`TRUNCATE` requires `ACCESS EXCLUSIVE`, and the lock remains held through the
full refill, index maintenance, constraint checks and commit. There is no
generic duration estimate: representative row width, indexes, storage,
replication and server load must be measured.

The formal locking model is:

```text
A >= n * L + M
A + P <= B
```

The first inequality is enforced for the actual existing lock set so ordinary
multi-target contention reaches retryable `55P03` rather than aggregate
`57014`. The second is the independent reader-impact budget.

For explicit declared refill, `P` includes the locked `TRUNCATE`, refill, staging drop,
comment and commit work. Partition swap may be considered later only if live
measurements show that this critical section is unacceptable.

## Validation

The implementation passed the complete automated suite: 450 tests and 204
subtests, with no failures, errors or skips. The suite covers configuration,
normalization, strict compatibility, control-flow ordering and rollback
invariants.

Two separate PostgreSQL 16.11 acceptance campaigns also passed:

- the existing live server verified atomic first publication, stable target
  OIDs, preservation of views, indexes, grants, ownership and triggers,
  strict staging-time rejection, physical-schema compatibility, incoming
  foreign-key rejection, empty refreshes, multi-target rollback and cleanup;
- a resource-constrained Ubuntu/Docker VPS verified `55P03` retry, observable
  reader blocking during refill, backend termination, rollback of the live
  refill, successor cleanup of abandoned staging artifacts and final cleanup.

On the constrained VPS, a declared refresh of 50,000 rows measured 4.233
seconds for the locked refill and commit and 2.429 seconds of concurrent reader
blocking. The harness deliberately prolonged the refill to make the lock state
observable, so these values are acceptance evidence rather than production
performance estimates.

Both campaigns used positively scoped temporary objects and verified cleanup.
The prior 0.4.0 live acceptance results remain the empirical baseline for the
unchanged inferred swap path.

## Deferred

- bounded-memory `COPY FROM STDIN`;
- partition swap;
- automatic schema migration;
- incoming-foreign-key coordination;
- configurable ownership of the target table comment;
- defaults, generated expressions, identity declarations and broader
  PostgreSQL-specific type families.
