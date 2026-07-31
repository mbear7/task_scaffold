# Migrating to 0.5.2

0.5.2 tightens the declared `output_schema` contract. Ordinary inferred
pipelines and valid declared pipelines require no changes.

Review external scripts containing `output_schema=` for these declarations:

- `sa.Float(p)`: `p` must be an integer from 1 through 53;
- `sa.String(n)`: `n` must be a positive integer;
- `sa.Numeric(p, s)`: `p` must be 1 through 1000 and the supported subset is
  `0 <= s <= p`;
- `sa.Numeric(scale=s)` is no longer accepted; specify precision as well or use
  unconstrained `sa.Numeric()`;
- `sa.LargeBinary(n)` is rejected because PostgreSQL `BYTEA` does not preserve
  the length; use `sa.LargeBinary()`;
- text collation in `String`/`Text` is outside the declared contract.

Declared text containing `\x00` now fails before staging DDL with a contextual
`DbPublishError`. No changes are required in the bundled `hr_task`,
`hr_petl_task`, or `ops_task`; none uses `output_schema`.
