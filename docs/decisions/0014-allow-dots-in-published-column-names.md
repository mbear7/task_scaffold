# 0014 — Allow dots in published column names

Status: accepted.

Amends [0010](0010-require-portable-database-identifiers.md), whose Decision
reads "All database schema, table **and published column** names must
satisfy `^[a-z_][a-z0-9_]*$`". That sentence is what this ADR narrows: the
rule still governs schemas, table names and every generated relation name,
and only published column names stop sharing it.

The rationale in [0004](0004-lowercase-portable-identifiers.md) — why lower
case, and what 'portable' buys — is unchanged and still describes the
relation contract exactly. Nothing in it is edited; see Consequence below for
why that matters. 0010's removal of `db_identifier_mode`, and its rule that
defensive quoting is an SQL-construction concern rather than a user-selectable
semantic, both stand.

## Problem

A source column named `lev.1` could not be published. Preflight and runtime
payload validation both applied `PORTABLE_IDENTIFIER_RE`
(`^[a-z_][a-z0-9_]*$`) to schemas, table names and every column alike, so the
run failed before any database work.

That is the right rule for relations and the wrong one for columns, because
the two are not chosen the same way. A table name is ours: the task author
picks it, and picking a portable one costs nothing. A column name usually is
not — it arrives from the source, out of analytical vocabulary where `lev.1`,
`lev.2` and `metric.plan_2026` are ordinary rather than accidental. Applying
the relation rule there forced a rename of data the scaffold does not own,
purely to satisfy a convention that exists for a different reason.

## Decision

Split the contract. Add a second pattern for columns and leave
`PORTABLE_IDENTIFIER_RE` untouched:

```python
PORTABLE_IDENTIFIER_RE = re.compile(r'^[a-z_][a-z0-9_]*$')
PUBLISHED_COLUMN_RE    = re.compile(r'^[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*$')
```

`validate_published_column_name()` sits beside `validate_portable_identifier()`
in `db/identifiers.py`, and the two column call sites in `db/publish.py` —
preflight and runtime payload validation — use it. Everything else keeps the
relation rule: schema, table name, `db_table_id_pix` and friends, generated
staging names, and the source-state relation in `source_state.py`.

The 63-byte limit is unaffected: `validate_identifier()` still runs on every
column, so widening the character set did not widen the length rule.

### Why a repeated dotted segment, not `[a-z0-9_.]*`

The simpler `^[a-z_][a-z0-9_.]*$` would also accept `lev.`, `a..b` and
`lev.1.`. A dot separates parts; it is not just another permitted character,
and a name with a trailing or doubled dot is malformed rather than merely
unusual. The chosen pattern rejects `.lev`, `lev.`, `lev..1`, `Lev.1`,
`lev-1`, `lev 1` and `1lev`.

## Consequence, stated rather than hidden

**A dotted column is not portable in the sense ADR 0004 defined.** That
decision chose lower case specifically so an identifier behaves identically
whether quoted or not, and therefore never needs quoting in hand-written SQL.
A dot breaks exactly that property:

```sql
select lev.1 from hr_ssch;     -- parses as a qualified reference, not the column
select "lev.1" from hr_ssch;   -- what you have to write
```

This is why the dot was added as a second pattern rather than folded into
`PORTABLE_IDENTIFIER_RE`. Widening the original would have left its comment —
which explains at length what 'portable' buys — describing something no longer
true. The resulting architecture is two contracts, not one weaker one:

```text
relation identifiers        portable: identical quoted or unquoted,
                            never needs quoting downstream

published column names      controlled lower-case vocabulary,
                            dots permitted, quoting required in SQL
```

Task authors who query published tables by hand need to know this. It is
stated in `docs/task-authoring.md` for that reason.

## Why this is safe

Not a case of permitting something the publication paths cannot render. Every
path where a column name reaches SQL already quoted it before this change:

| path | mechanism |
|---|---|
| staging and target DDL | `sa.Column(...)` — SQLAlchemy quotes |
| INSERT | `staging_table.insert()` — SQLAlchemy owns rendering |
| COPY | `dialect.identifier_preparer.quote(column.name)` |
| refill `INSERT … SELECT` | `_quote_identifier(column.name)` |

Verified against the real PostgreSQL dialect:

```text
preparer.quote('lev.1')     -> "lev.1"
_quote_identifier('lev.1')  -> "lev.1"
CREATE TABLE bsr.hr_ssch ( "lev.1" TEXT, ok TEXT )
INSERT INTO x ("lev.1", ok) VALUES (%(lev_1)s, %(ok)s)
```

The INSERT rendering shows the one thing that looked like it might break and
does not. SQLAlchemy sanitises the *bind parameter* to `lev_1` while the
*identifier* stays `"lev.1"`, and maps a dict keyed by the original name
automatically — `construct_params({'lev.1': 'v'})` returns `{'lev_1': 'v'}`.
`load_rows_into_staging()` passes `list[dict]` keyed by column name straight
through, so it needed no change.

Nothing in `task_core/` or `tools/` parses a name on `.` — no `split('.')`,
`rsplit` or `partition` — so a dotted column cannot be mistaken for a
qualified reference anywhere internally.

## Verification

Unit coverage asserts the split rather than the widening: `lev.1` accepted as
a column and rejected as a table and as a schema, the malformed forms
rejected, and a dotted name over 63 bytes still rejected by the length check.
Reverting either call site fails those tests.

Live acceptance on the target localhost PostgreSQL 18.4, one table carrying
`lev.1`, `lev.2` and `metric.plan_2026` alongside plain columns, through
**every** path and **both** loaders — 7/7:

- values round-trip unchanged under `replace`, for INSERT and for COPY;
- `information_schema.columns` stores the dotted names verbatim rather than
  mangled, for both loaders;
- refill preserves the target OID and refills correctly, which exercises the
  `_quote_identifier()` column list — the only path that builds the list as a
  string itself instead of letting SQLAlchemy render it;
- a dotted *table* name is still rejected against the live server.

## Also updated

`tools/generate_output_schema.py` carries its own copy of the pattern,
because it is standalone by design and must not import `task_core`. It gained
the same split — columns take the wider rule, table and schema keep the
strict one — and a test asserts the two copies stay in step, since nothing
else can.

## Rejected

**Widening `PORTABLE_IDENTIFIER_RE` itself.** Simplest change, and it would
have made the word 'portable' false for every user of that constant while
leaving ADR 0004's reasoning in place as documentation of a property the code
no longer had. The scaffold's most common defect is a correct mechanism with
an overstated sentence beside it; this would have been one deliberately.

**Allowing arbitrary quoted identifiers for columns.** Would accept upper
case, spaces and punctuation, which reintroduces exactly the case-folding
fragility ADR 0004 exists to prevent — `"Sales"` and `sales` becoming
different columns. The dot is a specific, bounded concession for a specific,
observed need, not a general retreat from a controlled vocabulary.
