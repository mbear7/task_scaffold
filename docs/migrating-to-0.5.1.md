# Migrating to 0.5.1

Only scripts using `output_schema` need a publication-strategy review.

In 0.5.0, supplying `output_schema` implied stable refill. In 0.5.1, both
inferred and declared schemas use replacement by default:

```python
PipelineSpec(
    db_table='customer_summary',
    output_schema=OUTPUT_SCHEMA,
)
```

Use explicit refill only when the existing ordinary table object and objects
attached to it must survive:

```python
PipelineSpec(
    db_table='customer_summary',
    output_schema=OUTPUT_SCHEMA,
    db_publication_strategy='refill',
)
```

## Checklist

1. Search every running script for `output_schema=`.
2. For each result, ask what would break if the table were replaced by another
   table with the same name and declared schema.
3. Add `db_publication_strategy='refill'` only when the answer includes the
   target OID, dependent views, grants, ownership, indexes, constraints,
   triggers or row-level security.
4. Leave the strategy unset when replacement is acceptable. This performs one
   database write and keeps the publication lock normally row-independent.
5. Search for direct `DbPayload`, `from_petl()` or `from_pandas()` construction.
   Only `replace` is valid with an inferred schema; `refill` requires
   `output_schema`.

An incompatible existing target is not migrated automatically. Migrate it
manually or drop it and run the modified task again.
