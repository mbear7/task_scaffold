"""
Level 2: CSV resource. Depends on file_access.py (level 1) for selection
and types.py (level 0) for the row-width vocabulary.

This module is named csv.py and the standard library module it wraps is
also named csv. That is safe here, and deliberately so: the package uses
absolute imports throughout, so `import csv` below binds the standard
library, exactly as `task_core.types` shadowing stdlib `types` has worked
since 0.1. The rule that makes it safe is the same one stated in
CLAUDE.md and enforced by tests -- no relative imports inside task_core.
The facade exports csv_file/latest_csv/csv_file_set, never a name `csv`,
so `task_core.csv` does not become ambiguous either.
"""

import codecs
import csv
import io
from dataclasses import dataclass

import petl as etl

from task_core.file_access import _resolve_source_access, _ResourceSelection
from task_core.resources.file_set import build_file_set_resource
from task_core.source_tracking import single_file_fingerprint
from task_core.types import SourceCheckError, validate_row_width


class CsvReadError(Exception):
    """Invalid CSV source content, or content no schema can be built from.

    A direct Exception subclass on purpose, and specifically *not* a
    ValueError and *not* a PipelineError. Both would be caught by handlers
    that mean something else:

    - `except ValueError` is how a caller checks its own CsvReadOptions,
      per decisions/0015 section 4. If bad source content also arrived as
      a ValueError, a configuration guard would silently swallow a data
      problem and report it as a configuration one.
    - PipelineError means the runner's pipeline contract was broken. Bad
      bytes in a vendor file are not that.

    The runner still wraps whatever escapes a pipeline's run() into a
    PipelineError with __cause__ set (runner.py), which is correct and
    does not undo the distinction -- the type survives on the chain. But
    note that a CSV table is lazy, so a content failure surfaces wherever
    the traversal happens, which is often *not* inside run().
    """


def _validate_columns(columns):
    """Table column names, which are not database identifiers.

    decisions/0015 section 9 keeps these two contracts apart deliberately.
    'Employee ID' and 'Metric/Plan' are valid here and would be rejected
    by the publication identifier rule; that is the point. A pipeline that
    publishes renames first. Rejecting them at read time would make
    task_core refuse a perfectly good source because one possible later
    consumer has a narrower vocabulary.
    """
    if isinstance(columns, str):
        # Caught explicitly: a bare string is iterable, so without this it
        # would be accepted as one column per character.
        raise TypeError(
            f'columns must be a sequence of names, got a str: {columns!r}'
        )

    names = tuple(columns)
    if not names:
        raise ValueError('columns must name at least one column')

    for index, name in enumerate(names):
        if not isinstance(name, str):
            raise TypeError(
                f'columns[{index}] must be a str, got '
                f'{type(name).__name__}'
            )
        if not name.strip():
            raise ValueError(
                f'columns[{index}] is empty or whitespace-only: {name!r}'
            )

    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f'columns contains duplicate names: {duplicates}')

    return names


@dataclass(frozen=True, kw_only=True)
class CsvReadOptions:
    """One immutable CSV parser configuration.

    Keyword-only per ADR 0013: this is author-facing configuration, not a
    natural value object.

    The delimiter default is ';' -- a project convention, not detection.
    task_core never sniffs a dialect (decisions/0015 section 5), so a
    comma-separated source says so: CsvReadOptions(delimiter=',').

    encoding defaults to 'utf-8-sig' rather than 'utf-8' because the
    expected producer of a ';'-separated file is Excel, which writes a BOM.
    Under plain 'utf-8' that BOM survives as part of the first header name
    -- confirmed directly, the header parses as '\\ufeffname' rather than
    'name' -- so every lookup of the first column fails with a name that
    looks correct in a traceback. 'utf-8-sig' reads BOM-less UTF-8 equally
    well, so the default costs nothing.
    """

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

    def __post_init__(self):
        # Everything here fails before a single source byte is read. An
        # author error must not wait for the first traversal, which may be
        # minutes into a run and inside an unrelated pipeline.
        # codecs.lookup raises LookupError, which is neither TypeError nor
        # ValueError and so escapes the `except ValueError` that
        # decisions/0015 section 4 tells authors to guard option
        # construction with. Re-raised as ValueError, cause chained.
        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise ValueError(f'unknown encoding: {self.encoding!r}') from exc

        if not isinstance(self.errors, str):
            raise TypeError(
                f'errors must be a str, got {type(self.errors).__name__}'
            )
        try:
            codecs.lookup_error(self.errors)
        except LookupError as exc:
            raise ValueError(
                f'unknown decoding error handler: {self.errors!r}'
            ) from exc

        if not isinstance(self.header, bool):
            raise TypeError(
                f'header must be a bool, got {type(self.header).__name__}'
            )
        validate_row_width(self.row_width)

        if self.columns is not None:
            object.__setattr__(self, 'columns', _validate_columns(self.columns))

        # The standard library owns CSV format validation, so ask it rather
        # than reimplementing it. Measured: a bad delimiter, a bad quoting
        # constant, a multi-character escapechar and quotechar=None with
        # quoting enabled all raise TypeError at reader construction, which
        # is exactly the boundary decisions/0015 section 4 asks for. An
        # empty iterable means this can never consume the real source.
        csv.reader(iter(()), **self.dialect_kwargs())

    def dialect_kwargs(self):
        return {
            'delimiter': self.delimiter,
            'quotechar': self.quotechar,
            'escapechar': self.escapechar,
            'doublequote': self.doublequote,
            'skipinitialspace': self.skipinitialspace,
            'quoting': self.quoting,
            'strict': self.strict,
        }


def _coerce_options(options):
    if options is None:
        return CsvReadOptions()
    if not isinstance(options, CsvReadOptions):
        raise TypeError(
            f'options must be a CsvReadOptions, got {type(options).__name__}'
        )
    return options


def _describe(source_label, *, reader=None, data_row=None, **extra):
    """Structural context for a diagnostic, never source content.

    decisions/0015 section 30 requires the failing source to be named and
    forbids dumping rows, whole field values or byte prefixes -- a CSV that
    fails to parse is exactly the kind of file whose contents should not
    land in a log.
    """
    parts = [f'source={source_label}']
    if reader is not None:
        parts.append(f'line={reader.line_num}')
    if data_row is not None:
        parts.append(f'data_row={data_row}')
    parts.extend(f'{key}={value}' for key, value in extra.items() if value is not None)
    return ', '.join(parts)


def _fit_row(record, expected, *, mode, explicit_columns, source_label,
             reader, data_row):
    """Bring one parsed record to the expected width, or refuse to."""
    actual = len(record)
    if actual == expected:
        return tuple(record)

    if actual > expected:
        # Explicit columns describe the positional output schema, so
        # anything to the right of it is not an output column at all --
        # projected away regardless of row_width (decisions/0015 s.11).
        # Without explicit columns the surplus is a genuine surprise and
        # row_width decides.
        if explicit_columns or mode in ('truncate', 'pad_or_truncate'):
            return tuple(record[:expected])
        raise CsvReadError(
            f'CSV row has more fields than the table has columns '
            f'({_describe(source_label, reader=reader, data_row=data_row, expected=expected, actual=actual)}). '
            f"row_width={mode!r} does not drop surplus fields; use "
            f"'truncate' or 'pad_or_truncate' if that is intended."
        )

    if mode in ('pad', 'pad_or_truncate'):
        return tuple(record) + ('',) * (expected - actual)
    raise CsvReadError(
        f'CSV row has fewer fields than the table has columns '
        f'({_describe(source_label, reader=reader, data_row=data_row, expected=expected, actual=actual)}). '
        f"row_width={mode!r} does not pad short rows; use 'pad' or "
        f"'pad_or_truncate' if that is intended."
    )


def _validate_inferred_header(record, source_label, reader):
    """An inferred header must satisfy the same rules explicit columns do."""
    try:
        return _validate_columns(record)
    except (TypeError, ValueError) as exc:
        raise CsvReadError(
            f'CSV header row cannot be used as table columns: {exc} '
            f'({_describe(source_label, reader=reader)})'
        ) from exc


def _iter_records(stream, options, source_label):
    """Parse records, ignoring only genuinely empty ones.

    csv.reader returns [] for a physically blank line, and that is the only
    thing skipped. A record like ';;;' is four empty fields and is data
    (decisions/0015 section 12); so is a line holding one space.

    `record == []` and `not record` are the same test here -- a list is
    falsy exactly when it is empty -- so the spelling is not what protects
    this. The mistake it has to survive is the tempting one a maintainer
    would actually write: skipping records that merely *look* blank, via
    `not any(record)` or by joining and stripping. Those discard ';;;' and
    silently drop real rows, which is why the test asserts that ';;;'
    arrives as four empty fields.
    """
    reader = csv.reader(stream, **options.dialect_kwargs())
    try:
        for record in reader:
            if record == []:
                continue
            yield reader, record
    except csv.Error as exc:
        # csv.Error is not a ValueError -- measured. It covers malformed
        # quoting under strict=True and the field-size limit, whose message
        # already carries the active limit, so it is not repeated here.
        raise CsvReadError(
            f'CSV source is malformed: {exc} '
            f'({_describe(source_label, reader=reader, field_size_limit=csv.field_size_limit())})'
        ) from exc
    except UnicodeDecodeError as exc:
        raise CsvReadError(
            f'CSV source could not be decoded as {options.encoding!r}: '
            f'{exc.reason} '
            f'({_describe(source_label, reader=reader, encoding=options.encoding)})'
        ) from exc


def iter_csv_rows(open_stream, options, source_label):
    """Yield the header tuple, then data tuples, for one CSV source.

    open_stream() returns a fresh binary stream. Called once per traversal,
    never held between them -- see decisions/0015 section 31.

    Always emits its header, including for a file-set member: the file-set
    loop consumes that header to compare it against the first member's,
    and drops it rather than re-emitting. Passing a "suppress the header"
    flag down here instead would move the comparison to the one place that
    cannot see both.
    """
    with open_stream() as binary:
        # newline='' is framework infrastructure, not an option: it hands
        # record-boundary handling to the CSV parser, which is what makes a
        # newline inside a quoted field stay part of the field.
        text = io.TextIOWrapper(
            binary,
            encoding=options.encoding,
            errors=options.errors,
            newline='',
        )
        try:
            yield from _iter_parsed(text, options, source_label)
        finally:
            # detach() rather than close(): the caller's `with` owns the
            # binary stream, and TextIOWrapper.close() would close it early
            # on the exception path.
            text.detach()


def _iter_parsed(text, options, source_label):
    records = _iter_records(text, options, source_label)
    explicit = options.columns is not None
    columns = options.columns

    reader = None
    first = None
    for reader, record in records:
        first = record
        break

    if columns is not None:
        if options.header and first is not None:
            # Consumed as the physical header and discarded. Not compared
            # with the declared columns, not used to reorder: explicit
            # columns are positional by decision (0015 section 10), which
            # is what lets two files with different header spellings feed
            # one table.
            first = None
        yield tuple(columns)
        expected = len(columns)
    else:
        if first is None:
            raise CsvReadError(
                f'CSV source has no usable record, so its columns cannot be '
                f'inferred ({_describe(source_label)}). Supply columns= to '
                f'read an empty source as a zero-row table.'
            )
        if options.header:
            columns = _validate_inferred_header(first, source_label, reader)
            first = None
        else:
            columns = tuple(f'Column{i + 1}' for i in range(len(first)))
        yield tuple(columns)
        expected = len(columns)

    data_row = 0
    if first is not None:
        # Only reachable headerless: the first record defined the width and
        # is still a data row.
        data_row += 1
        yield _fit_row(
            first, expected, mode=options.row_width,
            explicit_columns=explicit, source_label=source_label,
            reader=reader, data_row=data_row,
        )

    for reader, record in records:
        data_row += 1
        yield _fit_row(
            record, expected, mode=options.row_width,
            explicit_columns=explicit, source_label=source_label,
            reader=reader, data_row=data_row,
        )


class _CsvTable(etl.Table):
    """A petl table whose every iteration is a fresh source traversal.

    Subclassing petl's Table rather than returning a generator is what
    makes the view re-iterable. decisions/0015 section 23 rules out
    caching a captured generator as the backing iterable: a one-shot
    generator would yield rows to the first consumer and nothing to the
    second, which is the shape of bug that looks like an empty result
    rather than an error.

    One inherited sharp edge, measured here rather than assumed, because
    it costs a whole extra read of the file and nothing about the calling
    code looks wrong:

        list(table)   -> two traversals
        tuple(table)  -> two traversals
        table.list()  -> one
        for row in table  -> one

    petl's IterContainer defines __len__ as `sum(1 for _ in self)`, and
    list()/tuple() call it to pre-size the result before iterating. This
    is generic petl behaviour on every Table including petl.fromcsv's --
    verified against a bare probe subclass, not specific to this class --
    and petl works around it internally, which is why IterContainer.list()
    carries the comment "avoid iterating twice". It only becomes expensive
    here because for a CSV table a traversal is a file read rather than a
    walk over an already-materialized list.

    The runner is not exposed to this: table_adapters stabilizes with
    tbl.cache(), and nrows()/to_excel()/to_db_payload() all iterate
    normally. It bites inside pipeline code that calls list() on a
    resource table itself.
    """

    def __init__(self, make_iterator):
        self._make_iterator = make_iterator

    def __iter__(self):
        return self._make_iterator()


class csv_resource:
    """One selected CSV file, parsed lazily on every traversal.

    Holds the SelectedFile that chose the file, not a path re-resolved
    later -- the fingerprint has to describe the same selection the data
    load used.
    """

    def __init__(self, selected_file, options=None, *, source_access=None,
                 selection=None):
        self._selected = selected_file
        self._options = _coerce_options(options)
        self._source_access = _resolve_source_access(source_access)
        self._selection = selection
        self._table = None

    @property
    def path(self):
        return self._selected.path

    @property
    def options(self):
        return self._options

    def _open(self):
        return self._source_access.open_binary(self._selected.path)

    def get_table(self):
        if self._table is None:
            self._table = _CsvTable(lambda: iter_csv_rows(
                self._open, self._options, self._selected.path,
            ))
        return self._table

    def source_fingerprint(self, source_key):
        if self._selection is None:
            raise SourceCheckError(
                f'csv_resource for {self._selected.path!r} was not built with '
                'source-selection metadata; source_fingerprint() is only '
                'supported for resources built via build_csv_file_resource() '
                'or build_latest_csv_resource()'
            )
        sel = self._selection
        return single_file_fingerprint(
            source_key,
            source_kind=sel.source_kind,
            root_path=sel.root_path,
            include_mask=sel.include_mask,
            recursive=sel.recursive,
            selected_file=sel.selected_file,
        )

    def close(self):
        # Idempotent, and only drops the cached view -- there is no stream
        # to release, because a traversal owns its own and closes it. A
        # caller may keep using the resource afterwards and get fresh views.
        self._table = None


class csv_file_set_resource:
    """One logical CSV table over a selected set of files.

    Composition, not inheritance, and not new methods on
    file_set_resource: that generic resource may hold workbooks or
    arbitrary binary files, and CSV parsing is not part of what it means
    (decisions/0015 section 16). Selection, membership, ordering, binary
    opening and the file-set fingerprint stay where they already work;
    this owns decoding, headers, widths and cross-file agreement.

    The CSV layer never rescans the folder. It reads the file set that was
    already selected, in the order it was selected, so the table and the
    fingerprint describe the same files.
    """

    def __init__(self, file_set, options=None):
        self._file_set = file_set
        self._options = _coerce_options(options)
        self._table = None

    @property
    def files(self):
        return self._file_set.files

    @property
    def options(self):
        return self._options

    def open_file(self, selected_file):
        return self._file_set.open_file(selected_file)

    def source_fingerprint(self, source_key):
        # Delegated, but the refusal is re-worded. The composed resource
        # says 'built via build_file_set_resource()', which is the right
        # advice for a workbook set and the wrong builder to reach for
        # here. Confirmed reachable: build_csv_file_set_resource() always
        # supplies selection metadata, but this class can be constructed
        # directly around a file_set_resource that has none.
        #
        # Re-worded here rather than widened in file_set.py -- adding a CSV
        # builder to that message would make it overstate for every plain
        # workbook file set that raises it.
        try:
            return self._file_set.source_fingerprint(source_key)
        except SourceCheckError as exc:
            raise SourceCheckError(
                'csv_file_set_resource was not built with source-selection '
                'metadata; source_fingerprint() is only supported for '
                'resources built via build_csv_file_set_resource()'
            ) from exc

    def _iter_combined(self):
        """Header once, then every member's data rows, one file at a time.

        Only one member is open at any moment: each member's traversal is
        fully consumed and closed before the next is opened, because this
        is a generator delegating to one _iter_member() at a time rather
        than zipping them.
        """
        # Each member infers its own header and is then compared with the
        # first usable one. Deliberately not "tell member N the schema
        # member 1 established": that was the first shape of this loop and
        # it silently disabled the check, because a member handed a schema
        # emits *that* schema as its header, so the comparison compared
        # member 1's header with itself and no mismatch could ever fail.
        #
        # With explicit columns every member emits the declared tuple, so
        # the comparison is trivially satisfied -- which is correct, since
        # explicit columns are positional and physical header spellings are
        # not compared at all.
        columns = None
        emitted_header = False

        for selected in self.files:
            rows = iter_csv_rows(
                lambda selected=selected: self._file_set.open_file(selected),
                self._options,
                selected.path,
            )
            try:
                header = next(rows)
            except StopIteration:
                continue
            except CsvReadError as exc:
                if 'cannot be inferred' in str(exc):
                    # An empty or blank-only member contributes no rows and
                    # does not get to define or veto the shared schema
                    # (section 13). Only the whole set being unusable is an
                    # error, and that is decided after the loop.
                    continue
                raise

            if not emitted_header:
                columns = header
                emitted_header = True
                yield header
            elif header != columns:
                raise CsvReadError(
                    f'CSV member header does not match the header '
                    f'established by the first usable member of this file '
                    f'set (source={selected.path}, expected_width='
                    f'{len(columns)}, actual_width={len(header)}). Headers '
                    f'are compared exactly -- same text, order, case and '
                    f'whitespace. For a feed whose header spelling moves '
                    f'but whose column positions are contractual, declare '
                    f'columns= and keep header=True.'
                )

            yield from rows

        if not emitted_header:
            raise CsvReadError(
                'no selected CSV file in this file set has a usable record, '
                'so the columns cannot be inferred '
                f'(files={len(self.files)}). Supply columns= to read an '
                'empty set as a zero-row table.'
            )

    def get_table(self):
        if self._table is None:
            self._table = _CsvTable(self._iter_combined)
        return self._table

    def get_file_table(self, selected_file):
        """One member, parsed on its own.

        Deliberately not subject to cross-file header agreement: that is a
        property of the combined table. A member whose header conflicts
        with the first file is exactly the one an author needs to be able
        to look at, so refusing to show it would defeat the diagnosis.
        """
        if selected_file not in self.files:
            raise ValueError(
                f'{selected_file!r} is not one of this '
                f'csv_file_set_resource\'s own selected files -- did it '
                f'come from a different resource?'
            )
        return _CsvTable(lambda: iter_csv_rows(
            lambda: self._file_set.open_file(selected_file),
            self._options,
            selected_file.path,
        ))

    def close(self):
        self._table = None


def build_csv_file_set_resource(
    folder_path,
    pattern='*.csv',
    options=None,
    *,
    include_hidden=False,
    include_system=False,
    include_temp=False,
    min_age_seconds=None,
    recursive=False,
    source_access=None,
    on_empty='raise',
):
    """A folder of CSV files as one logical table.

    on_empty stays the generic builder's, which is why it is passed
    through rather than reinterpreted here: zero matching files is a
    selection question and belongs to the file-set layer. Content
    emptiness -- files that exist but hold no usable record -- is a
    separate concept decided during traversal.
    """
    return csv_file_set_resource(
        build_file_set_resource(
            folder_path,
            pattern,
            include_hidden=include_hidden,
            include_system=include_system,
            include_temp=include_temp,
            min_age_seconds=min_age_seconds,
            recursive=recursive,
            source_access=source_access,
            on_empty=on_empty,
        ),
        options,
    )


def build_csv_file_resource(file_path, options=None, *, source_access=None):
    """One named CSV file, tracked."""
    source_access = _resolve_source_access(source_access)
    selected = source_access.select_fixed_file_info(file_path)
    return csv_resource(
        selected,
        options,
        source_access=source_access,
        selection=_ResourceSelection(
            source_kind='fixed_file',
            # Nothing was scanned, so there is no root or mask to report.
            # See build_xlsx_file_resource for why the resulting signature
            # is deliberately insensitive to the containing directory.
            root_path=None,
            include_mask=None,
            recursive=False,
            selected_file=selected,
        ),
    )


def build_latest_csv_resource(
    folder_path,
    pattern='*.csv',
    options=None,
    *,
    include_hidden=False,
    include_system=False,
    include_temp=False,
    min_age_seconds=None,
    recursive=False,
    source_access=None,
):
    """The newest matching CSV in a folder, tracked.

    The same selection primitive the latest-workbook builder uses, so the
    two cannot disagree about which file 'latest' means.
    """
    source_access = _resolve_source_access(source_access)
    selected = source_access.select_latest_file_info(
        folder_path,
        pattern,
        include_hidden=include_hidden,
        include_system=include_system,
        include_temp=include_temp,
        min_age_seconds=min_age_seconds,
        recursive=recursive,
    )
    return csv_resource(
        selected,
        options,
        source_access=source_access,
        selection=_ResourceSelection(
            source_kind='latest_file',
            root_path=str(folder_path),
            include_mask=pattern,
            recursive=recursive,
            selected_file=selected,
        ),
    )
