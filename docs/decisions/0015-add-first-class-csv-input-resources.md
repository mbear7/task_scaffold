# 0015 — Add first-class CSV input resources

Status: accepted.

Builds on [0011](0011-add-bounded-memory-copy-loader.md), [0013](0013-name-configuration-choices.md), and [0014](0014-allow-dots-in-published-column-names.md).

## Problem

task_core has first-class declarative file resources for XLSX but not CSV.

The current declarative file-source surface is:

```python
latest_xlsx(path='.', pattern='*.xlsx', tracker=True)
xlsx_file_set(path='.', pattern='*.xlsx', tracker=True)
resource(loader, tracker=False)
```

CSV can currently be consumed only through task-specific code or a custom resource loader.

CSV is a normal production input format and needs the same framework-owned behavior as XLSX for:

- local and SMB/DFS access;
- exact-file selection;
- latest-file selection;
- deterministic file sets;
- source-change tracking;
- resource injection;
- cleanup;
- PETL/pandas pipeline compatibility;
- one-shot PostgreSQL COPY publication.

The implementation must not create a second file-resource architecture merely because the parser is different.

Selection, tracking and resource wiring are format-independent. Parsing is not.

The public CSV and XLSX APIs should therefore match wherever their semantics are shared, while CSV-specific behavior belongs in one explicit parser configuration object.

CSV output and compressed CSV input are deferred from the first implementation.

## Decision

### 1. Complete the declarative file-source matrix

The public author-facing factory matrix becomes:

```python
# Exact file
xlsx_file(path, tracker=True)

csv_file(
    path,
    tracker=True,
    *,
    options=None,
)

# Latest matching file
latest_xlsx(
    path='.',
    pattern='*.xlsx',
    tracker=True,
)

latest_csv(
    path='.',
    pattern='*.csv',
    tracker=True,
    *,
    options=None,
)

# Matching file set
xlsx_file_set(
    path='.',
    pattern='*.xlsx',
    tracker=True,
)

csv_file_set(
    path='.',
    pattern='*.csv',
    tracker=True,
    *,
    options=None,
)
```

The shared arguments have the same names, ordering and semantics.

CSV adds only `options`, because parser configuration is format-specific.

`xlsx_file()` is added at the same time. Exact-file selection is a generic file-resource concept and should not exist only for CSV.

This ADR therefore adds the following new public factories:

```python
xlsx_file(...)
csv_file(...)
latest_csv(...)
csv_file_set(...)
```

File factories remain tracked by default. `tracker=False` remains the explicit opt-out.

The generic `resource()` factory remains untracked by default because task_core cannot infer whether an arbitrary loader is fingerprintable.

### 2. Keep high-level selection APIs narrow

The high-level CSV factories intentionally match the current XLSX convenience surface.

They do not add CSV-only factory arguments for:

- `recursive`;
- `include_hidden`;
- `include_system`;
- `include_temp`;
- `min_age_seconds`;
- `on_empty`.

The generic file-selection and file-set layers continue to own those advanced controls.

If one of those controls later becomes useful enough for declarative convenience factories, it should normally be promoted for both CSV and XLSX at the same time.

Do not make CSV's public selection API richer merely because the generic machinery already supports more switches.

For recursive generic selection, use the existing explicit `recursive=True` mechanism with a normal pattern such as:

```python
pattern='*.csv'
```

Do not establish `**/*.csv` as a portable local/SMB public convention.

### 3. Add one immutable CSV parser configuration

CSV parsing configuration is represented by:

```python
@dataclass(frozen=True, kw_only=True)
class CsvReadOptions:
    encoding: str = 'utf-8-sig'
    errors: str = 'strict'

    delimiter: str = ';'
    quotechar: str | None = '"'
    escapechar: str | None = None
    doublequote: bool = True
    skipinitialspace: bool = False
    quoting: int = csv.QUOTE_MINIMAL
    strict: bool = True

    header: bool = True
    columns: tuple[str, ...] | None = None
    row_width: str = 'strict'
```

The default delimiter is intentionally:

```python
delimiter=';'
```

This is a task_core project convention, not automatic dialect detection.

Comma-separated sources must opt in explicitly:

```python
CsvReadOptions(delimiter=',')
```

This follows ADR 0013: configuration choices are named.

Factories take only:

```python
options=...
```

They do not expose duplicate parser arguments beside it.

There is no merge or precedence rule between:

```python
options=...
delimiter=...
encoding=...
```

because the second form does not exist.

`options=None` means the default `CsvReadOptions()`.

A non-`CsvReadOptions` value passed as `options` raises `TypeError`.

`columns` may be supplied as a list or tuple but is normalized once to an immutable tuple during `CsvReadOptions` construction.

Reusable parser configurations are expected:

```python
VENDOR_CSV = CsvReadOptions(
    encoding='cp1251',
    delimiter=',',
    header=False,
    columns=('id', 'name', 'amount'),
    row_width='pad_or_truncate',
)

CURRENT = csv_file('current.csv', options=VENDOR_CSV)
ARCHIVE = csv_file_set('archive', options=VENDOR_CSV)
```

There is no task_core-specific CSV dialect abstraction.

### 4. Validate configuration at construction

Invalid parser configuration is an author/configuration error, not a source-content error.

It fails when `CsvReadOptions` is constructed, before any source row is consumed.

Validation includes:

- encoding through `codecs.lookup()`;
- decoding error handler through `codecs.lookup_error()`;
- standard-library CSV format parameters;
- `header`;
- `columns`;
- `row_width`.

Standard-library CSV option compatibility should be validated by constructing a `csv.reader` over an empty in-memory iterable where practical rather than duplicating CPython's complete parser validation.

Configuration validation must never consume the real source or call `next()` on a reader backed by the real source.

task_core adds no artificial compatibility restrictions beyond what the running Python CSV implementation supports.

Invalid configuration raises `TypeError` or `ValueError`.

It does not raise `CsvReadError`.

A parser-originated `ValueError` encountered later while consuming actual source content is a source-content failure and is wrapped as `CsvReadError`.

This construction-time versus iteration-time boundary is deliberate and must be tested.

### 5. Do not infer dialect or encoding

The parser is deterministic.

task_core does not use:

- `csv.Sniffer`;
- automatic encoding detection;
- automatic delimiter detection;
- automatic quote-character detection;
- registered dialect names;
- comment-line conventions;
- preamble conventions.

The parser configuration that produced a table must be visible in task code.

Each selected binary file is wrapped as:

```python
io.TextIOWrapper(
    binary_stream,
    encoding=options.encoding,
    errors=options.errors,
    newline='',
)
```

`newline=''` is framework infrastructure, not a user option.

This delegates CSV record-boundary handling to the standard-library parser.

`\n`, `\r\n` and `\r` are accepted as record endings according to that parser's semantics.

Embedded newlines inside quoted fields remain part of the field.

With the default `utf-8-sig`, a UTF-8 BOM is removed independently at the beginning of every selected file.

`lineterminator` is not exposed because it is a writer concern.

### 6. Preserve CSV values instead of guessing their meaning

Under the default:

```python
quoting=csv.QUOTE_MINIMAL
```

CSV fields remain strings.

For example:

```text
001
125.50
true
2026-08-07
```

remain:

```python
'001'
'125.50'
'true'
'2026-08-07'
```

An empty CSV field remains:

```python
''
```

task_core does not infer:

- integers;
- decimal values;
- booleans;
- dates;
- timestamps;
- null markers.

There is no `null_values=` option in the first implementation.

These remain ordinary strings:

```text
NULL
null
\N
NA
N/A
None
-
```

Padding also inserts `''`.

A direct default CSV -> inferred PostgreSQL pipeline therefore normally produces TEXT user columns because the source supplied strings.

Typing is an ETL responsibility:

```python
return (
    source.get_table()
    .convert('id', int)
    .convert(
        'amount',
        lambda value: None if value == '' else Decimal(value),
    )
)
```

A declared `output_schema` is validation, not semantic coercion.

For example, a CSV value:

```python
'125'
```

does not silently become:

```python
125
```

because the declared database type is integer. The pipeline must perform that conversion first.

This preserves the current publication rule that declared schemas validate actual Python value families instead of interpreting text.

### 7. Preserve standard-library quoting semantics

The default is:

```python
csv.QUOTE_MINIMAL
```

Every `csv.QUOTE_*` mode supported by the running Python version may be used.

task_core does not define its own quoting enum.

Parser-produced values are preserved.

For example, if a supported standard-library quoting mode produces:

- `float`;
- `None`;

task_core does not stringify those values afterward.

Therefore the "CSV values are strings" guarantee applies to the default text-preserving configuration, not to every possible standard-library quoting mode.

Header records are parsed through the same `csv.reader` configuration as data records.

There is no second header parser.

When `csv.QUOTE_NONE` is used, quoting is disabled.

If the source uses an escape convention for delimiter, quote or escape characters, the corresponding `escapechar` must be configured explicitly.

task_core does not infer an escape character.

Parser-originated `ValueError`, including value conversion errors caused by a quoting mode, is a source-content failure during iteration and is wrapped as `CsvReadError`.

Configuration-time `ValueError` remains a configuration error.

### 8. Use the process-wide CSV field-size limit

CSV resources use the currently active:

```python
csv.field_size_limit()
```

They do not modify it.

There is no per-resource field-size option.

Python exposes the limit as process-wide state. A resource-local option would imply isolation that does not exist if multiple readers execute in the same process.

A task requiring a different limit changes it explicitly at process level.

A field-size failure includes the active limit in its diagnostic.

Tests that modify `csv.field_size_limit()` must restore the previous value.

A shared test helper or context manager may be used if more than one test requires temporary modification, but this is test infrastructure rather than public API.

### 9. Table column names and database identifiers are separate contracts

CSV table column names are not PostgreSQL identifiers.

Explicit `columns=` must contain:

- at least one name;
- strings only;
- no empty strings;
- no whitespace-only strings;
- no exact duplicates.

Names are otherwise preserved exactly.

CSV resources do not:

- trim names;
- lower-case names;
- replace spaces;
- transliterate names;
- replace punctuation;
- normalize names into database identifiers.

CSV resources preserve source column names rather than assigning domain meaning to them. Any semantic mapping from source vocabulary to the pipeline's desired domain or output vocabulary belongs to the pipeline. If the source already uses the desired names, no mapping is required.

Therefore these are valid CSV/table column names:

```python
'Employee ID'
'Department Name'
'lev.1'
'lev.1.col'
'Metric/Plan'
```

When `header=True` and `columns=None`, the inferred physical header must satisfy the same table-column rules: non-empty, non-blank, string and unique.

Invalid inferred headers raise `CsvReadError` lazily when reached.

This is deliberately separate from ADR 0014.

If a pipeline later publishes the table to PostgreSQL, database publication applies its own column-name contract.

For example:

```text
lev.1
lev.1.col
metric.plan_2026
```

are valid published columns under ADR 0014.

A source column such as:

```text
Employee ID
```

is valid inside the CSV/PETL table but must be renamed before database publication, normally through the existing `db_contract` or a pipeline transformation.

CSV parsing must not reject a valid source table merely because one possible later consumer has a narrower naming vocabulary.

### 10. Define header behavior explicitly

#### Inferred header

With:

```python
header=True
columns=None
```

the first nonblank CSV record is the output header.

It must satisfy the table-column rules above.

That record is not emitted as data.

#### Explicit columns with a physical header

With:

```python
header=True
columns=('a', 'b', 'c')
```

the first nonblank record is consumed as the physical source header but does not define the output schema.

Its names:

- are not compared with explicit columns;
- are not used to reorder data;
- are not validated for uniqueness;
- are not required to have the same width as the explicit schema.

The record must still decode and parse successfully.

Explicit columns are positional.

This intentionally permits:

```text
file A header: ID;Name;Value
file B header: identifier;description;amount

output columns: id;name;value
```

when the task author explicitly declared that positional contract.

#### Headerless source

With:

```python
header=False
columns=None
```

the first nonblank data record defines the width.

Generated column names are:

```python
Column1
Column2
...
ColumnN
```

That first record remains a data row.

With:

```python
header=False
columns=(...)
```

the explicit schema is used immediately and no record is consumed as a header.

### 11. Define row-width behavior

`row_width` accepts:

```text
strict
pad
truncate
pad_or_truncate
```

Without explicit `columns`, expected width comes from:

- the inferred header when `header=True`; or
- the first nonblank data row when `header=False`.

The modes mean:

```text
strict
    short -> error
    long  -> error

pad
    short -> pad with ''
    long  -> error

truncate
    short -> error
    long  -> truncate

pad_or_truncate
    short -> pad with ''
    long  -> truncate
```

`pad` and `truncate` are intentionally one-sided repair policies.

`pad` repairs only missing trailing fields.

`truncate` repairs only surplus trailing fields.

Use `pad_or_truncate` when both short and long rows should be repaired.

This asymmetry is deliberate. Changing `truncate` to also pad short rows would make it equivalent to `pad_or_truncate`.

Explicit `columns=` has one additional rule:

> Surplus source fields are always projected away beyond the explicit output width.

Explicit columns describe the positional output schema, so fields to the right of that schema are not output columns.

For explicit columns, short rows still obey `row_width`:

```text
strict            -> error
pad               -> pad with ''
truncate          -> error
pad_or_truncate   -> pad with ''
```

### 12. Ignore only genuinely empty CSV records

Only:

```python
[]
```

returned by `csv.reader` is ignored.

A record such as:

```csv
;;;
```

is data. It contains four empty fields.

A physical line containing spaces is also data if the parser returns those spaces as a field.

task_core performs no implicit whitespace stripping.

`skipinitialspace=True` affects parsing only according to the standard-library parser's own semantics.

### 13. Define empty-source behavior

An empty or blank-only source cannot infer a schema.

Therefore, for one file:

```text
explicit columns
    -> valid zero-row table

no explicit columns
    -> CsvReadError: schema cannot be inferred
```

A header-only file is valid.

With an inferred header it establishes the output schema and yields zero data rows.

With explicit columns it consumes the physical header and yields zero data rows.

For a file set:

- empty members contribute no rows;
- blank-only members contribute no rows;
- they do not define or alter an inferred schema;
- a header-only member may establish or validate an inferred header schema.

If the complete selected set contains no usable record:

```text
explicit columns
    -> zero-row table with the explicit schema

no explicit columns
    -> CsvReadError
```

Selection emptiness and content emptiness remain different concepts.

`csv_file_set()` follows `xlsx_file_set()` and uses the generic file-set default that zero matching files raises `NoMatchingFilesError`.

The lower-level generic file-set builder retains its existing `on_empty='empty'` option.

### 14. A CSV file set is one logical table

`csv_file_set(...).get_table()` returns one logical table in the deterministic order of the already selected files.

The CSV layer does not rescan the directory.

Each member file is parsed independently.

Parser and decoder state never cross a file boundary.

Only one member file is open at a time.

The combined table emits:

1. one logical output header;
2. the data records of the first file;
3. the data records of each later file in deterministic selected-file order.

No implicit filename, source-path or provenance column is added.

#### Explicit columns

With `columns=...`, those columns are the shared output schema for every member.

When `header=True`, every nonempty member consumes its own physical header.

Physical header names are not compared with the explicit schema.

Rows remain positional.

#### Inferred headers

With:

```python
header=True
columns=None
```

the first usable file header establishes the shared output schema.

Every later usable file must have an exactly equal header:

- same width;
- same text;
- same order;
- same case;
- same whitespace.

There is no:

- union by name;
- reordering by name;
- case-insensitive comparison;
- whitespace normalization;
- automatic reconciliation.

A mismatch raises `CsvReadError` when iteration reaches that member.

For recurring feeds whose physical header spelling is unstable but whose column positions are contractual, use explicit `columns=` with `header=True`.

That is the supported v1 mechanism for positional compatibility across differently named headers.

No `header_matching` mode is introduced.

#### Headerless inferred width

With:

```python
header=False
columns=None
```

the first nonblank record across the selected set defines the width and generated `ColumnN` names.

That record remains data.

Every later data row in every member is checked against the same width.

### 15. Support combined and per-file tables

A CSV file-set resource provides the shared file-set operations plus:

```python
source.get_table()
source.get_file_table(selected_file)
```

The composed resource exposes:

```python
source.files
source.open_file(selected_file)
source.source_fingerprint(source_key)
source.get_table()
source.get_file_table(selected_file)
source.close()
```

`get_file_table(selected_file)` accepts only a `SelectedFile` belonging to that resource.

Membership uses the same value-based `SelectedFile` semantics as the underlying generic file-set resource.

It does not introduce object-identity semantics.

It parses only that member.

With explicit columns, it uses the explicit schema.

Without explicit columns, its schema is inferred locally from that file.

Cross-file header/schema agreement is a property of combined `get_table()` only.

Therefore a later member whose header conflicts with the first file may still be inspected independently with `get_file_table()`.

### 16. Compose the generic file-set resource

CSV file-set behavior is implemented by composition, not by adding CSV parser methods to `file_set_resource`.

The current generic file-set layer continues to own:

- selection;
- `SelectedFile` membership;
- deterministic ordering;
- local/SMB binary opening;
- file-set source fingerprints.

The CSV wrapper owns:

- decoding;
- CSV parser configuration;
- header handling;
- row width;
- cross-file schema agreement;
- lazy CSV PETL views.

Conceptually:

```text
file_set_resource
    selection
    files
    open_file()
    source_fingerprint()
          |
          v
csv_file_set_resource
    options
    get_table()
    get_file_table()
    close()
```

`csv_file_set_resource` delegates:

```text
files
open_file()
source_fingerprint()
```

to the composed generic `file_set_resource`.

It does not duplicate those implementations.

Do not add `get_table()` CSV semantics to the generic file-set type.

That generic resource may represent XLSX files or arbitrary binary files.

### 17. Consolidate exact/latest selection metadata generically

The current file layer already has related path-returning selection helpers and `SelectedFile` metadata used for resource fingerprinting.

CSV must not introduce another selection implementation.

Add one internal metadata-producing primitive for exact selection and one for latest selection, for example:

```python
source_access.select_fixed_file_info(path) -> SelectedFile

source_access.select_latest_file_info(
    folder_path,
    pattern='*',
    ...
) -> SelectedFile
```

Latest-file selection must preserve the existing deterministic key exactly:

```python
max(
    file_infos,
    key=lambda item: (
        item.stat_result.st_mtime,
        item.path,
    ),
)
```

The path tie-breaker is part of existing behavior.

If two selected candidates have the same modification time, lexicographically greater `path` wins exactly as it does today.

This must remain deterministic for both local and SMB sources.

The existing path-returning methods use the same metadata-producing primitives:

```python
select_fixed_file(path)
    -> select_fixed_file_info(path).path

select_latest_file(...)
    -> select_latest_file_info(...).path
```

`build_latest_xlsx_resource()` is changed to use the generic latest-info primitive rather than maintaining an independent latest-selection implementation.

CSV latest selection uses the same primitive.

Exact CSV and XLSX resources use the same fixed-file metadata primitive.

Selection behavior therefore exists in one generic place.

### 18. A captured SelectedFile is the sole selection source of truth

Once a resource builder has captured a `SelectedFile`, it must not perform another selection or stat merely to construct the format-specific resource.

The captured object supplies:

- the path that will later be opened;
- the source metadata used for fingerprinting.

Conceptually:

```text
select_*_file_info(...)
        |
        v
SelectedFile
   |            |
   |            +-> fingerprint metadata
   |
   +-> selected_file.path
           |
           v
      format resource
```

This is especially important for the current XLSX path, where generic exact/latest selection is being separated from workbook parsing.

Do not implement:

```text
select_latest_file_info()
    -> SelectedFile

then

build_excel_resource(selected_file.path)
    -> select_fixed_file() again
```

That would perform another filesystem selection/check after the resource selection had already been captured.

The captured `SelectedFile` is the sole source of truth for both read-path choice and tracked source metadata.

This is a selection snapshot, not a byte snapshot.

### 19. Add tracked exact XLSX without creating another XLSX parser

`xlsx_file()` uses the existing Excel parser resource.

It does not create a separate parser implementation.

The exact-file builder captures selection metadata with:

```text
source_kind = 'fixed_file'
selected_file = captured SelectedFile
```

The latest builder uses:

```text
source_kind = 'latest_file'
```

The Excel parser itself remains independent of how the workbook was selected.

The exact selected path is passed directly from the captured `SelectedFile`.

It is not re-selected or re-statted for fingerprinting.

### 20. Exact and latest CSV share one parser resource

`csv_file()` and `latest_csv()` use the same single-file CSV parser resource.

They differ only in generic source-selection metadata.

Conceptually:

```text
csv_file
    -> fixed-file selection
    -> csv_resource

latest_csv
    -> latest-file selection
    -> csv_resource
```

The parser does not contain separate "latest CSV" behavior.

`csv_file_set()` wraps the generic file-set resource because it has genuinely different multi-file composition semantics.

### 21. Capture file selection once

Source membership and source fingerprint metadata are a construction-time snapshot.

For an exact or latest resource:

```text
captured SelectedFile.path used for opening
==
path represented by captured fingerprint metadata
```

For a file set:

```text
resource.files
==
ordered selected files used by get_table()
==
ordered selected files used by source_fingerprint()
```

Fingerprinting must not:

- rescan the folder;
- select "latest" again;
- independently reconstruct a different member set;
- re-stat selected files merely to rebuild selection metadata.

This preserves the task_core rule that source-change tracking describes the resource instance actually injected into the task context.

### 22. Selection snapshot is not a byte snapshot

Capturing `SelectedFile` metadata does not make source bytes immutable.

task_core does not copy or lock source files.

After resource construction:

- a newly added file does not join an existing file set;
- a newly created newer file does not replace an existing `latest_csv` selection;
- deleting a selected file may raise its native filesystem error when iteration opens it;
- replacing or modifying bytes at the same selected path may be observed by a later traversal.

The selected membership and fingerprint metadata remain those captured at construction.

The bytes are read when iteration occurs.

task_core does not add:

- content hashing;
- before/after stat comparison;
- file locking;
- source copying;
- mutation detection;
- snapshot isolation.

Source tracking remains run-to-run change detection, not transactional source isolation.

Source producers should publish completed files atomically where practical: write to a temporary name, close it, then rename or move it into the watched location.

### 23. CSV resources expose re-iterable lazy PETL tables

All task_core resources continue to return PETL tables regardless of the pipeline's configured adapter.

CSV does not create a pandas-specific resource API.

Calling:

```python
table = source.get_table()
```

does not parse CSV rows.

The resource may cache the lightweight table-view object:

```python
source.get_table() is source.get_table()
```

but it must never cache one captured generator as that table's backing iterable.

The table view itself must be re-iterable.

Every:

```python
iter(table)
```

must create a fresh CSV traversal.

That traversal:

1. opens the selected source;
2. creates a fresh `TextIOWrapper`;
3. creates a fresh `csv.reader`;
4. parses and validates rows;
5. closes the stream.

Therefore:

```python
table = source.get_table()

rows1 = list(table)
rows2 = list(table)
```

must perform two source traversals and, if the source bytes did not change, produce equivalent rows.

A simple one-shot generator object captured during `get_table()` construction is not a valid implementation.

An implementation may use an appropriate PETL container or another PETL-compatible re-iterable object whose `__iter__()` creates a fresh underlying row generator.

The behavioral contract matters more than a specific PETL internal class.

For file sets, each member file is opened and closed before the next member is opened.

Rows emitted by the CSV view are immutable tuples.

The mutable lists produced internally by `csv.reader` are not exposed as output rows.

### 24. Laziness does not imply that every downstream operation streams

The correct guarantee is:

> The CSV resource exposes rows lazily. The CSV resource itself does not materialize the complete input.

Do not document:

> PETL is lazy.

That statement is too broad.

A downstream operation may:

- stream;
- build an index;
- sort;
- group;
- cache;
- spill;
- materialize the complete table;
- retain aggregation state;

according to its own semantics.

CSV guarantees only its own source boundary.

### 25. pandas consumes the same parser and is eager

A pandas pipeline still receives task_core resources in the normal resource form.

It converts the PETL table itself, for example:

```python
df = source.get_table().todf()
```

That conversion is eager.

It consumes and materializes the complete table as a pandas `DataFrame`.

There is no:

```python
source.get_dataframe()
```

CSV-specific API.

There is no second:

```python
pandas.read_csv(...)
```

implementation.

Using two parsers would create two different answers for:

- quoting;
- row width;
- blank records;
- decoding;
- errors;
- headers;
- multi-file schemas.

PETL and pandas consumers must see the same task_core CSV semantics.

For large CSV or CSV-file-set inputs, `.todf()` may consume substantially more memory than the source files themselves and may exhaust the worker's available memory.

This memory behavior must be made prominent in task-authoring documentation.

A sole `db_loader='copy'` consumer with a non-materializing PETL transformation chain does not require pandas materialization and can preserve bounded-memory source processing into the COPY spool.

That statement does not apply to arbitrary PETL chains that themselves materialize data.

### 26. Repeated traversal is observable and must be documented

Because CSV rows are not cached by the resource, this pattern:

```python
table = source.get_table()

etl.nrows(table)  # first source traversal
return table      # later consumer traverses it again
```

traverses the source once for `nrows()` and again when the returned table is later consumed.

That is not a free inspection.

If the file changes between those traversals, the two consumers may observe different bytes at the same selected path.

The task-authoring documentation must show this anti-pattern explicitly.

The runner's stabilization logic protects configured multi-consumer output after a pipeline result is returned where stabilization is required.

It cannot infer or erase manual traversals performed inside pipeline code before that result is returned.

### 27. Integrate with the current COPY one-shot path

The CSV design does not add a special database publication route.

It feeds the existing table adapter and `db.payload` / `db.copy` path.

For an active:

```python
db_loader='copy'
```

publication with no other configured consumer requiring stabilization, the runner already avoids both:

```python
adapter.nrows()
adapter.stabilize()
```

before publication.

Therefore a non-materializing PETL chain sourced from CSV can be consumed once by the COPY row source.

#### Declared schema

With `output_schema`, the current COPY preparer already knows the target types.

It traverses the row source once and writes directly to the final COPY-text spool.

For a suitable CSV/PETL chain:

```text
CSV source
    -> PETL transformations
    -> COPY row source
    -> final COPY-text spool
```

requires one CSV traversal.

#### Inferred schema

Streaming COPY cannot use the materialized INSERT inference strategy.

The current COPY preparer instead:

1. traverses the source once;
2. normalizes each value;
3. feeds bounded per-column inference state that observes all rows;
4. writes the same normalized values to a neutral spool;
5. resolves the schema at EOF;
6. replays the neutral spool into the final COPY-text spool.

The CSV source is not traversed again.

The second pass is over task_core's local neutral spool.

This is deliberately different from ordinary materialized INSERT inference, which starts from the first 5000 rows and performs targeted remainder verification or full fallback where required.

Do not describe COPY inference as using the 5000-row sample.

### 28. Other configured consumers may require stabilization

The one-traversal CSV -> COPY statement applies only when COPY is the sole consumer that requires the pipeline result.

Configured consumers such as:

- Excel output;
- `debug_display`;
- `publish_result`;

may cause the existing runner to stabilize the pipeline result before multiple consumers traverse it.

That may materialize data according to the table adapter's current semantics.

The CSV resource itself remains lazy; the broader pipeline no longer necessarily is.

Documentation must keep that distinction explicit.

### 29. Add one public source-content exception

Add:

```python
CsvReadError
```

for invalid CSV source content or a CSV source whose table schema cannot be constructed.

It covers failures such as:

- decoding failure;
- malformed CSV syntax;
- parser-originated value conversion failure;
- row-width violation;
- invalid inferred header;
- cross-file inferred-header mismatch;
- field-size-limit failure;
- inability to infer a schema from the selected content.

It does not represent:

- invalid `CsvReadOptions`;
- nonexistent paths;
- permission failures;
- SMB transport failures;
- other generic filesystem failures.

Configuration errors remain `TypeError` or `ValueError`.

Native filesystem errors retain their native types.

Wrapped source-content exceptions are chained as `__cause__`.

Failures remain lazy when they depend on source content.

Constructing the resource or calling `get_table()` must not read source rows merely to discover those failures.

### 30. Every CsvReadError identifies its source

Every `CsvReadError` must identify the source or logical source whose content failed.

For a single-file resource, this is the selected file path.

For a member-specific failure inside a file set, the diagnostic identifies the failing member path.

For example:

```text
decode failure
    -> failing member path

row-width failure
    -> failing member path

header mismatch
    -> later conflicting member path
```

For a failure that applies to the selected set as a whole, such as inability to infer any schema from all selected members, the diagnostic identifies the logical file-set source.

Diagnostics additionally include useful structural information when available:

- encoding for decode errors;
- `csv.reader.line_num`;
- one-based logical data-row number;
- expected field count;
- actual field count;
- active `csv.field_size_limit()`.

`csv.reader.line_num` is a physical source-line counter, not a logical record number. A quoted CSV record may span more than one physical line.

When a logical data-row number is reported, it is counted within the current member file after the consumed header and ignored empty records.

Diagnostics must not include:

- complete raw rows;
- complete field values;
- arbitrary byte prefixes;
- oversized field content.

A duplicate inferred header may identify the conflicting name and positions without dumping the entire source record.

### 31. Resource lifecycle belongs to the CSV wrapper

CSV resources do not retain open streams between traversals.

Streams belong to individual iterators and close on:

- exhaustion;
- parser/error exit;
- explicit iterator closure.

The resource does not keep an active-iterator registry merely so `resource.close()` can force-close generators retained by arbitrary caller code.

`csv_resource.close()` and `csv_file_set_resource.close()` are idempotent and clear resource-owned cached table views.

A direct caller may reuse the resource after `close()` and obtain fresh views.

The current generic `file_set_resource` does not need a new `close()` method for this feature merely because the CSV wrapper has one. It owns no persistent file handle required by CSV traversal.

If the generic resource later acquires a genuine persistent lifecycle, the composed CSV resource should delegate to that lifecycle rather than duplicate it.

### 32. Keep implementation ownership aligned with the current package architecture

CSV is a resource/input concern.

It does not belong under `task_core.db`.

The expected ownership is:

```text
task_core/resources/csv.py
    CsvReadOptions
    CsvReadError
    csv_resource
    csv_file_set_resource
    CSV parsing/building internals

task_core/resources/factories.py
    xlsx_file
    csv_file
    latest_csv
    csv_file_set
```

Generic file selection remains in:

```text
task_core/file_access.py
```

Generic file-set selection/fingerprinting remains in:

```text
task_core/resources/file_set.py
```

`task_core/__init__.py` remains a pure re-export facade.

`task_core/resources/__init__.py` remains deliberately empty.

CSV implementation code must not import database publication modules merely to support COPY.

COPY integration happens through the existing table/payload boundaries.

### 33. Public surface added by this decision

The author-facing public additions are:

```python
CsvReadOptions
CsvReadError

xlsx_file
csv_file
latest_csv
csv_file_set
```

Existing:

```python
latest_xlsx
xlsx_file_set
resource
```

remain unchanged.

Internal implementation classes and helper functions do not become public merely because analogous historical XLSX helpers happen to be re-exported today.

A name is added to the public facade only if task authors need to depend on it.

## Rejected alternatives

### Use `pandas.read_csv()` for pandas pipelines

Rejected.

It would create two parser contracts depending on the pipeline adapter.

### Use `petl.fromcsv()` as the task_core parser contract

Rejected.

PETL remains the table abstraction, but task_core owns CSV parsing through Python's standard-library `csv` module.

This is necessary so task_core can define one consistent contract for:

- strict header validation;
- explicit positional schemas;
- row-width policies;
- multi-file header agreement;
- per-file parser reset;
- BOM behavior;
- parser-value preservation;
- diagnostics;
- `CsvReadError`;
- PETL and pandas consumers.

The resulting re-iterable row source is exposed as a PETL table.

### Automatically infer data types

Rejected.

Values such as:

```text
00123
01
1.0
20260101
FALSE
NULL
```

do not carry enough semantics for task_core to decide their business type safely.

### Automatically infer delimiter or encoding

Rejected.

It turns parser configuration into heuristic runtime state instead of explicit task configuration.

The default delimiter remains deliberately `;`.

### Add every parser option directly to each factory

Rejected.

Selection and parsing are different concerns. Parser configuration belongs in `CsvReadOptions`.

### Add fuzzy multi-file header matching

Rejected for v1.

No:

```text
casefold matching
whitespace normalization
automatic column reordering
header union
```

is performed.

If physical header spelling is unstable but positions are stable, use explicit `columns=`.

### Add CSV methods to `file_set_resource`

Rejected.

The generic file set is intentionally format-neutral.

### Give CSV richer selection switches than XLSX

Rejected.

Shared file-selection behavior should have shared author-facing APIs.

### Re-scan during source fingerprinting

Rejected.

A fingerprint must describe the resource selection that the pipeline actually receives.

### Hash or copy every input file

Rejected for this implementation.

That would create a new snapshot-isolation subsystem, additional I/O and additional cleanup semantics.

### Add a resource-local field-size option

Rejected.

The underlying Python setting is process-wide.

### Add implicit null tokens

Rejected.

Null meaning belongs to the ETL transformation.

### Automatically sanitize CSV headers for PostgreSQL

Rejected.

CSV/table column names and published database identifiers are separate contracts.

### Parse CSV separately for PETL and pandas

Rejected.

One source should have one parsing contract.

## Deferred

### Compressed CSV input

Deferred from the first implementation.

No:

- gzip;
- bzip2;
- ZIP;
- compression suffix detection;
- compression magic-byte detection.

Plain uncompressed CSV/text files only.

This is not a permanent architectural rejection. Compression can be considered separately if a real requirement appears.

### CSV output

Deferred from this decision.

Writing CSV introduces separate policy choices around:

- output encoding;
- BOM emission;
- line endings;
- null serialization;
- quoting;
- replacement;
- atomic publication.

CSV output may be added through a separate decision if required.

## Consequences

- CSV becomes a first-class tracked input resource.
- Exact-file selection becomes first-class for XLSX as well.
- CSV and XLSX share selection vocabulary where the underlying behavior is format-independent.
- CSV-specific parser policy is centralized in one immutable configuration object.
- The default delimiter is explicitly `;`.
- Default ingestion is text-preserving.
- Table column names are preserved independently of PostgreSQL identifier policy.
- Dotted names such as `lev.1` and `lev.1.col` can flow unchanged through CSV and, under ADR 0014, through PostgreSQL publication.
- Multi-file CSV ingestion has one deterministic schema contract.
- CSV resources expose re-iterable lazy PETL rows.
- Repeated traversal intentionally reopens and reparses the source.
- Arbitrary downstream PETL operations are not promised to remain streaming.
- pandas pipelines materialize the same parsed CSV table rather than invoking a second parser.
- DB-only COPY can consume a suitable non-materializing CSV/PETL chain without re-reading the CSV source.
- Source tracking captures selection metadata, not an immutable byte snapshot.
- Compression and CSV output remain deferred.
- Automatic type inference, automatic dialect detection, null-token inference and fuzzy header reconciliation remain intentionally absent.

## Documentation changes

`docs/task-authoring.md` must document the complete declarative matrix:

```python
xlsx_file(...)
latest_xlsx(...)
xlsx_file_set(...)

csv_file(...)
latest_csv(...)
csv_file_set(...)
```

The CSV documentation must state prominently:

```text
Default delimiter: ;
```

and immediately show a comma-separated override:

```python
csv_file(
    'employees.csv',
    options=CsvReadOptions(delimiter=','),
)
```

It must document `CsvReadOptions` and make these boundaries explicit:

1. Default CSV values are strings.
2. Type conversion belongs to the pipeline.
3. CSV/table column names are not PostgreSQL identifier rules.
4. Dotted names accepted by ADR 0014 may publish unchanged.
5. Other source headers may require `db_contract` or explicit transformation before publication.
6. CSV resource tables are lazy and re-iterable.
7. Arbitrary PETL transformations are not guaranteed to remain lazy.
8. Repeated traversal reopens and reparses the source.
9. Runner stabilization does not erase manual traversals performed inside `run()`.
10. `.todf()` materializes the complete table.
11. Large `.todf()` conversions may exhaust worker memory.
12. A DB-only non-materializing COPY path can consume the source once.
13. Extra output consumers may trigger stabilization/materialization.
14. Explicit `columns=` is the supported v1 solution for positionally stable file sets whose physical headers vary.
15. `csv.reader`, rather than `petl.fromcsv()`, owns the parser contract so PETL and pandas consumers receive identical task_core semantics.
16. CSV resources preserve source column names; semantic renaming belongs to the pipeline and is unnecessary when the source already uses the desired names.

The repeated-traversal anti-pattern must be shown explicitly:

```python
table = source.get_table()

etl.nrows(table)  # first source traversal
return table      # later consumer traverses it again
```

The documentation must not describe `.todf()` as memory-safe merely because the CSV source itself is lazy.

## Verification

Implementation is not complete until tests prove at least:

1. `xlsx_file`, `csv_file`, `latest_csv` and `csv_file_set` expose the agreed factory signatures.
2. Shared CSV/XLSX factory arguments remain aligned.
3. Exact and latest resource fingerprints use the same captured `SelectedFile` that determined the opened path.
4. Resource construction does not re-select or re-stat an already captured `SelectedFile`.
5. XLSX and CSV latest selection use the same generic latest-selection primitive.
6. Latest selection preserves the exact `(st_mtime, path)` maximum key.
7. Equal-`st_mtime` candidates are deterministically resolved by path.
8. File-set traversal uses exactly the immutable ordered selection represented by the resource fingerprint.
9. Default CSV parsing uses `delimiter=';'`.
10. Comma-separated input works when explicitly configured with `delimiter=','`.
11. Default CSV parsing preserves strings and `''`.
12. Every `row_width` mode behaves as specified.
13. `pad` repairs only short rows.
14. `truncate` repairs only long rows.
15. `pad_or_truncate` repairs both.
16. Explicit columns always project surplus fields and handle short rows according to `row_width`.
17. Inferred headers reject empty, blank, non-string and duplicate names.
18. Valid source-table names containing spaces or dots are preserved by CSV parsing.
19. Dotted names such as `lev.1` and `lev.1.col` remain valid CSV columns and can pass the existing ADR 0014 publication contract.
20. Database-invalid source names are not rejected by CSV parsing itself.
21. Multi-file inferred headers require exact equality.
22. Explicit columns permit different physical headers across member files.
23. Empty, blank-only and header-only sources follow the documented schema rules.
24. Every file in a file set gets independent decoder and CSV parser state.
25. UTF-8 BOM handling restarts independently for each file.
26. `\n`, `\r\n`, `\r` and embedded quoted newlines follow standard-library parser behavior.
27. Standard-library quoting modes preserve the values produced by that parser.
28. `QUOTE_NONE` behavior follows the configured standard-library parser without task_core inventing an escape character.
29. The resource never changes `csv.field_size_limit()`.
30. Tests that temporarily change `csv.field_size_limit()` restore its previous value.
31. Configuration parser failures remain construction-time `TypeError` or `ValueError`.
32. Configuration validation consumes no real source rows.
33. Parser/content failures during real iteration become `CsvReadError`.
34. Every `CsvReadError` identifies its source or logical source.
35. Member-specific file-set failures identify the failing member path.
36. Wrapped source-content failures retain their chained causes.
37. CSV diagnostics do not expose business field values.
38. Native local/SMB filesystem errors retain their native types.
39. `get_table()` performs no source-row read.
40. The same table view can be traversed twice and returns equivalent rows when the source is unchanged.
41. Those two traversals perform two actual source opens.
42. The cached table view is not backed by a one-shot captured generator.
43. Repeated traversal reopens and reparses the source.
44. Combined file-set traversal keeps at most one member file open at a time.
45. `get_file_table()` uses the same `SelectedFile` membership semantics as the composed generic file-set resource.
46. `get_file_table()` rejects a `SelectedFile` not belonging to that resource.
47. `csv_file_set_resource` delegates `.files`, `.open_file()` and `.source_fingerprint()` to the composed generic file-set resource.
48. pandas conversion consumes the same CSV parser output rather than invoking another parser.
49. `.todf()` consumes the complete table.
50. A sole DB COPY consumer does not pre-count or stabilize the CSV-derived table.
51. Declared COPY traverses the CSV-derived row source once.
52. Inferred COPY traverses the CSV-derived row source once and replays only its neutral spool.
53. Additional configured consumers follow the existing runner stabilization behavior.
54. Compressed files are not accidentally presented as supported input.
55. CSV output is not accidentally presented as supported functionality.

Every defect discovered while implementing this decision gets a regression test with teeth under the standing repository policy: remove or revert the corresponding fix, prove the intended test fails for the intended reason, then restore it.

The complete repository-mandated normal and `-O` verification remains required in addition to these targeted tests.

## Verification status

As of 0.7.9, implemented and verified.

Measured offline, in the suite: the parser contract, the header/width state
machine, the construction-versus-iteration exception boundary, laziness and
re-iterability, stream lifecycle, and the file-set semantics including
cross-file header agreement and one-member-at-a-time traversal.

Two gaps in the 0.7.7 implementation, found by review afterwards and fixed
in 0.7.8. Both are recorded here rather than only in the changelog, because
both were cases where the code satisfied every test that existed and still
did not satisfy this decision:

- Item 33 was only half met. `_iter_records()` wrapped `csv.Error` and
  `UnicodeDecodeError` but not a plain `ValueError`, which is what a
  value-converting quoting mode raises — `QUOTE_NONNUMERIC` converts every
  unquoted field to float, so an unquoted `abc` escaped raw. It also
  carried the offending field value in its message, so the leak defeated
  item 37 at the same time. Now wrapped, without interpolating the cause.
- Item 34 did not hold for the one failure that belongs to no member: an
  all-unusable file set reported that a schema could not be inferred
  without naming the set. It now reports the selection root and pattern,
  read from the selection already made rather than from a rescan.

Items 27 and 28 had no test at all in 0.7.7 despite being listed above.
`QUOTE_NONNUMERIC` and `QUOTE_NONE` are both covered now, including the
part that reads like a contradiction: under `QUOTE_NONNUMERIC` values are
*not* strings, because the standard library converts them, and section 7
means task_core neither adds inference nor suppresses the library's.

Measured against a live PostgreSQL 18.4 instance — items 51 and 52, which
no offline fake can settle, since the claim is about what the COPY path
does to a real server:

- **declared schema + `db_loader='copy'`** — 5 000 rows published,
  the CSV source opened **exactly once**;
- **inferred schema + `db_loader='copy'`** — same, 5 000 rows, one open;
- **a three-member CSV file set + COPY** — 3 000 rows, **exactly three
  opens**, one per member.

In all three, `000042` arrived in the database as `000042` rather than
`42`, and the BOM did not survive into the first column name.

The open counter was itself checked before those numbers were trusted: two
traversals of the same view register two opens, and three consumers of a
stabilized view register one. A count that could not move would have made
`opens == 1` a constant rather than an assertion.

Measured against a real DFS share in 0.7.9, read-only, from a host where
the referral resolves only intermittently. The suite still contains no SMB
harness — it must run offline — so this was a scratchpad run, and the
results are recorded here because they are not otherwise reproducible.

Passing: the SMB branch of `select_file_infos`, `select_latest_file_info`
and `select_fixed_file_info`; fingerprints for the exact, latest and
file-set resources; opening and reading a workbook; and the CSV reader's
transport seam end to end.

That run found a defect no offline test could: `select_fixed_file_info` and
the SMB folder scan both wrapped their remote stat in
`except FileNotFoundError`, and `smbclient` signals a missing file with
`SMBOSError`, which subclasses `OSError` directly. The clause could never
fire. A missing file therefore raised `FileNotFoundError` locally and
`SMBOSError` remotely from the same function, and had done since long
before this decision. Both sites now key on `errno.ENOENT` and re-raise
anything else untouched.

Still not measured over SMB: CSV parsing of a real CSV file. The share
holds only workbooks and access is read-only, so the seam was exercised by
reading a workbook *as* CSV — which proves transport, decoding and the
failure path, but not the parser against genuine CSV bytes over the wire.
