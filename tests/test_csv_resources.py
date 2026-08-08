"""CSV input resources: parser contract, laziness, and the exception boundary.

Protects the behaviour decisions/0015 specifies. The parts most worth
holding still are not the happy paths but three seams:

- the construction-time versus iteration-time exception boundary, which is
  what lets a task tell a configuration mistake from a bad vendor file;
- re-iterability, because a one-shot generator would not raise, it would
  quietly return nothing on the second traversal;
- stream lifecycle, because a resource that leaks handles fails only under
  load, on the network share, in production.
"""

import csv
import gc
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

import petl as etl

import task_core as tc
from task_core.file_access import source_access as FileAccessImpl
from task_core.resources.csv import (
    CsvReadError,
    CsvReadOptions,
    build_csv_file_resource,
    build_csv_file_set_resource,
    build_latest_csv_resource,
    csv_file_set_resource,
    csv_resource,
)
from task_core.resources.file_set import file_set_resource
from task_core.table_adapters import get_table_adapter


class TempDir:
    def __enter__(self):
        self._dir = tempfile.mkdtemp()
        return Path(self._dir)

    def __exit__(self, *exc):
        import shutil
        shutil.rmtree(self._dir, ignore_errors=True)


def _write(path, text, encoding='utf-8'):
    path.write_bytes(text.encode(encoding) if isinstance(text, str) else text)
    return path


@contextmanager
def field_size_limit(value):
    """Set the process-wide CSV field-size limit and put it back.

    decisions/0015 section 8 requires every test that moves this to restore
    it. It is process-global state, so a test that leaks it changes the
    behaviour of every later test in the same run -- including tests in
    other files, which would make the failure look unrelated to its cause.
    """
    previous = csv.field_size_limit()
    csv.field_size_limit(value)
    try:
        yield
    finally:
        csv.field_size_limit(previous)


class _CountingAccess(FileAccessImpl):
    """Records every source open, and whether each one was closed."""

    def __init__(self):
        super().__init__()
        self.opened = []
        self.closed = []

    @contextmanager
    def open_binary(self, path, *, buffered=False):
        with super().open_binary(path, buffered=buffered) as handle:
            self.opened.append(path)
            try:
                yield handle
            finally:
                self.closed.append(path)


def _rows(path, options=None, access=None):
    resource = build_csv_file_resource(
        str(path), options, source_access=access or tc.LOCAL_FILE_ACCESS,
    )
    try:
        return list(iter(resource.get_table()))
    finally:
        resource.close()


class Test1CsvReadOptionsValidateAtConstruction(unittest.TestCase):
    """Configuration errors must not wait for the first source row.

    decisions/0015 section 4 puts this boundary in place so that a task
    with a typo in its options fails at import, not thirty minutes into a
    run inside an unrelated pipeline. The suite has to hold both halves:
    that construction rejects, and that it rejects *without reading*.
    """

    def test_defaults_are_the_documented_ones(self):
        options = CsvReadOptions()
        self.assertEqual(options.delimiter, ';')
        self.assertEqual(
            options.encoding, 'utf-8-sig',
            'the default encoding must strip a BOM; see the BOM test below '
            'for what plain utf-8 does to the first column name',
        )
        self.assertEqual(options.row_width, 'strict')
        self.assertTrue(options.header)
        self.assertIsNone(options.columns)

    def test_configuration_errors_are_typeerror_or_valueerror(self):
        """Never LookupError, and never CsvReadError.

        codecs.lookup() raises LookupError natively, which is neither of
        the two types section 4 promises -- so an author guarding option
        construction with `except ValueError` would miss a bad encoding
        entirely. Re-raised deliberately; this is the test that says so.
        """
        cases = {
            'row_width': dict(row_width='nope'),
            'columns duplicate': dict(columns=['a', 'a']),
            'columns empty': dict(columns=[]),
            'columns blank name': dict(columns=['a', '  ']),
            'encoding': dict(encoding='no-such-codec'),
            'errors': dict(errors='no-such-handler'),
        }
        for label, kwargs in cases.items():
            with self.subTest(label):
                with self.assertRaises(ValueError) as raised:
                    CsvReadOptions(**kwargs)
                self.assertNotIsInstance(
                    raised.exception, CsvReadError,
                    f'{label} is a configuration error, not source content',
                )

        type_cases = {
            'delimiter width': dict(delimiter=';;'),
            'quoting value': dict(quoting=987),
            'escapechar width': dict(escapechar='ab'),
            'quotechar None while quoting': dict(quotechar=None),
            'columns as str': dict(columns='ab'),
            'columns member': dict(columns=['a', 7]),
            'header not bool': dict(header='yes'),
        }
        for label, kwargs in type_cases.items():
            with self.subTest(label):
                with self.assertRaises(TypeError):
                    CsvReadOptions(**kwargs)

    def test_validation_never_consumes_a_reader_over_real_content(self):
        """Section 4 forbids calling next() on a reader backed by the source.

        Checked structurally: the reader built during validation is handed
        an empty iterable, so even a bug that consumed from it could not
        reach a source row.
        """
        seen = {}
        real_reader = csv.reader

        def spy(iterable, **kwargs):
            seen['iterable'] = iterable
            return real_reader(iterable, **kwargs)

        csv.reader = spy
        try:
            CsvReadOptions()
        finally:
            csv.reader = real_reader

        self.assertEqual(list(seen['iterable']), [])

    def test_options_are_frozen_and_columns_normalized_to_a_tuple(self):
        options = CsvReadOptions(columns=['a', 'b'])
        self.assertEqual(options.columns, ('a', 'b'))
        self.assertIsInstance(options.columns, tuple)
        with self.assertRaises(Exception):
            options.delimiter = ','

    def test_a_non_options_value_is_refused(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            with self.assertRaises(TypeError):
                build_csv_file_resource(
                    str(path), {'delimiter': ','},
                    source_access=tc.LOCAL_FILE_ACCESS,
                )


class Test2HeaderAndWidthBehaviour(unittest.TestCase):
    """The header/width state machine of decisions/0015 sections 10-13."""

    def test_inferred_header_is_the_first_nonblank_record(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'name;value\r\nx;1\r\ny;2\r\n')
            self.assertEqual(
                _rows(path),
                [('name', 'value'), ('x', '1'), ('y', '2')],
            )

    def test_a_bom_does_not_become_part_of_the_first_column_name(self):
        """The failure this prevents is silent, which is why it is pinned.

        Under encoding='utf-8' the BOM survives into the header and the
        first column is named '\\ufeffname'. Nothing raises -- every later
        lookup of 'name' just fails to match, and the traceback shows a
        name that looks correct. Excel writes that BOM, and Excel is the
        expected producer of the ';'-delimited files this defaults to.
        """
        with TempDir() as d:
            path = _write(d / 'x.csv', '﻿name;value\r\nx;1\r\n')

            self.assertEqual(_rows(path)[0], ('name', 'value'))
            self.assertEqual(
                _rows(path, CsvReadOptions(encoding='utf-8'))[0],
                ('﻿name', 'value'),
                'plain utf-8 is expected to keep the BOM -- if this ever '
                'stops being true the default no longer earns its keep',
            )

    def test_explicit_columns_consume_but_ignore_a_physical_header(self):
        """Positional by decision: two files spelled differently, one table."""
        with TempDir() as d:
            first = _write(d / 'a.csv', 'ID;Name\n1;bob\n')
            second = _write(d / 'b.csv', 'identifier;description\n2;ann\n')
            options = CsvReadOptions(columns=['id', 'name'])

            self.assertEqual(
                _rows(first, options), [('id', 'name'), ('1', 'bob')],
            )
            self.assertEqual(
                _rows(second, options), [('id', 'name'), ('2', 'ann')],
            )

    def test_headerless_generates_column_names_and_keeps_the_first_row(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b;c\nd;e;f\n')
            self.assertEqual(
                _rows(path, CsvReadOptions(header=False)),
                [('Column1', 'Column2', 'Column3'),
                 ('a', 'b', 'c'), ('d', 'e', 'f')],
            )

    def test_headerless_with_explicit_columns_consumes_no_record(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', '1;2\n3;4\n')
            self.assertEqual(
                _rows(path, CsvReadOptions(header=False, columns=['a', 'b'])),
                [('a', 'b'), ('1', '2'), ('3', '4')],
            )

    def test_the_row_width_matrix_is_exactly_as_documented(self):
        """All four modes, both directions, without explicit columns.

        Written as a matrix rather than eight separate tests because the
        asymmetry *is* the contract: 'pad' repairing a long row, or
        'truncate' repairing a short one, would make one of the four values
        redundant, and that is precisely the change this must catch.
        """
        expected = {
            ('strict', 'short'): CsvReadError,
            ('strict', 'long'): CsvReadError,
            ('pad', 'short'): ('1', '2', ''),
            ('pad', 'long'): CsvReadError,
            ('truncate', 'short'): CsvReadError,
            ('truncate', 'long'): ('1', '2', '3'),
            ('pad_or_truncate', 'short'): ('1', '2', ''),
            ('pad_or_truncate', 'long'): ('1', '2', '3'),
        }
        with TempDir() as d:
            sources = {
                'short': _write(d / 'short.csv', 'a;b;c\n1;2\n'),
                'long': _write(d / 'long.csv', 'a;b;c\n1;2;3;4\n'),
            }
            for (mode, shape), want in expected.items():
                with self.subTest(mode=mode, shape=shape):
                    options = CsvReadOptions(row_width=mode)
                    if want is CsvReadError:
                        with self.assertRaises(CsvReadError):
                            _rows(sources[shape], options)
                    else:
                        self.assertEqual(_rows(sources[shape], options)[1], want)

    def test_explicit_columns_always_project_surplus_fields_away(self):
        """Even under row_width='strict'.

        Explicit columns declare the positional output width, so a field to
        the right of it is not an output column at all -- there is nothing
        for strictness to be strict about. Short rows still obey the mode.
        """
        with TempDir() as d:
            long_row = _write(d / 'long.csv', 'a;b\n1;2;3;4\n')
            short_row = _write(d / 'short.csv', 'a;b\n1\n')
            columns = ['x', 'y', 'z']

            for mode in ('strict', 'pad', 'truncate', 'pad_or_truncate'):
                with self.subTest(mode=mode, shape='long'):
                    self.assertEqual(
                        _rows(long_row, CsvReadOptions(
                            row_width=mode, columns=columns))[1],
                        ('1', '2', '3'),
                    )

            with self.subTest(shape='short', mode='strict'):
                with self.assertRaises(CsvReadError):
                    _rows(short_row, CsvReadOptions(
                        row_width='strict', columns=columns))
            with self.subTest(shape='short', mode='pad'):
                self.assertEqual(
                    _rows(short_row, CsvReadOptions(
                        row_width='pad', columns=columns))[1],
                    ('1', '', ''),
                )

    def test_only_a_genuinely_empty_record_is_ignored(self):
        """';;;' is four empty fields, not a blank line.

        Only a physically blank line -- csv.reader's [] -- is skipped.

        The revert this must survive is not `record == []` versus
        `not record`; those are the same test for a list, and swapping them
        changes nothing (checked). It is the plausible-looking
        `not any(record)`, or joining the fields and stripping, either of
        which discards ';;;' and drops real rows from the middle of a file
        with nothing to show for it. task_core strips no whitespace of its
        own.
        """
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b;c;d\n\n;;;\n')
            self.assertEqual(
                _rows(path),
                [('a', 'b', 'c', 'd'), ('', '', '', '')],
                'a blank line was kept as data, or ";;;" was discarded as '
                'blank',
            )

            # A spaces-only line is data too. Asserted through strict width
            # rather than by reading it back: it parses as one field where
            # four are expected, so it must raise. Were it being skipped as
            # blank, a read-back assertion would pass quietly on a
            # one-data-row table and prove nothing.
            spaces = _write(d / 'spaces.csv', 'a;b;c;d\n \n')
            with self.assertRaises(CsvReadError):
                _rows(spaces)
            self.assertEqual(
                _rows(spaces, CsvReadOptions(row_width='pad'))[1],
                (' ', '', '', ''),
            )

    def test_empty_and_header_only_sources(self):
        with TempDir() as d:
            empty = _write(d / 'empty.csv', '')
            blank = _write(d / 'blank.csv', '\n\n\n')
            header_only = _write(d / 'ho.csv', 'a;b\n')

            for label, path in (('empty', empty), ('blank-only', blank)):
                with self.subTest(label, columns=False):
                    with self.assertRaises(CsvReadError) as raised:
                        _rows(path)
                    self.assertIn('cannot be inferred', str(raised.exception))
                with self.subTest(label, columns=True):
                    self.assertEqual(
                        _rows(path, CsvReadOptions(columns=['a', 'b'])),
                        [('a', 'b')],
                    )

            self.assertEqual(_rows(header_only), [('a', 'b')])
            self.assertEqual(
                _rows(header_only, CsvReadOptions(columns=['x', 'y'])),
                [('x', 'y')],
            )

    def test_quoting_and_embedded_newlines_follow_the_standard_library(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n"line1\nline2";2\n')
            self.assertEqual(
                _rows(path), [('a', 'b'), ('line1\nline2', '2')],
            )

    def test_values_stay_strings(self):
        """No type inference. '001' must not become 1."""
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b;c;d\n001;125.50;true;2026-08-07\n')
            self.assertEqual(
                _rows(path)[1], ('001', '125.50', 'true', '2026-08-07'),
            )


class Test3TableColumnNamesAreNotDatabaseIdentifiers(unittest.TestCase):
    """decisions/0015 section 9 keeps the two contracts apart on purpose.

    A CSV whose header says 'Employee ID' is a perfectly good source. It
    is not a publishable PostgreSQL identifier, and the pipeline renames
    before publication -- but rejecting it at read time would make
    task_core refuse a valid file because one possible later consumer has
    a narrower vocabulary.
    """

    def test_source_names_are_preserved_exactly(self):
        with TempDir() as d:
            path = _write(
                d / 'x.csv',
                'Employee ID;Department Name;lev.1;Metric/Plan\n1;ops;a;b\n',
            )
            self.assertEqual(
                _rows(path)[0],
                ('Employee ID', 'Department Name', 'lev.1', 'Metric/Plan'),
            )

    def test_an_inferred_header_still_obeys_the_table_column_rules(self):
        with TempDir() as d:
            duplicate = _write(d / 'dup.csv', 'a;a\n1;2\n')
            blank = _write(d / 'blank.csv', 'a;  \n1;2\n')

            with self.assertRaises(CsvReadError) as raised:
                _rows(duplicate)
            self.assertIn('duplicate', str(raised.exception))

            with self.assertRaises(CsvReadError):
                _rows(blank)


class Test4TheExceptionBoundaryHolds(unittest.TestCase):
    """Source content fails as CsvReadError, and as nothing else.

    The user's decision was that CsvReadError is a direct Exception
    subclass rather than a ValueError or a PipelineError, so that
    `except ValueError` around configuration cannot swallow a data problem
    and a bad vendor file cannot masquerade as a broken pipeline contract.
    Asserted from both sides, because a type that merely *has* the right
    name proves nothing.
    """

    def test_csv_read_error_is_neither_valueerror_nor_pipelineerror(self):
        self.assertTrue(issubclass(CsvReadError, Exception))
        self.assertNotIsInstance(CsvReadError('x'), ValueError)
        self.assertNotIsInstance(CsvReadError('x'), tc.PipelineError)

    def test_content_failures_are_csvreaderror_with_the_cause_chained(self):
        with TempDir() as d:
            undecodable = _write(d / 'bad.csv', b'a;b\n\xff\xfe;2\n')
            malformed = _write(d / 'mal.csv', b'a;b\n"x"y;2\n')

            with self.assertRaises(CsvReadError) as decode:
                _rows(undecodable)
            self.assertIsInstance(decode.exception.__cause__, UnicodeDecodeError)
            self.assertIn('utf-8-sig', str(decode.exception))

            with self.assertRaises(CsvReadError) as parse:
                _rows(malformed)
            self.assertIsInstance(parse.exception.__cause__, csv.Error)

    def test_a_field_size_failure_reports_the_active_process_limit(self):
        with TempDir() as d:
            path = _write(d / 'big.csv', 'a;b\n' + 'x' * 200 + ';2\n')
            with field_size_limit(16):
                with self.assertRaises(CsvReadError) as raised:
                    _rows(path)
                self.assertIn('16', str(raised.exception))

    def test_the_field_size_limit_helper_restores_the_previous_value(self):
        """The helper is test infrastructure, so it gets its own test.

        A leak here would change parsing for every later test in the run
        and the resulting failure would point anywhere but at this file.
        """
        before = csv.field_size_limit()
        with field_size_limit(32):
            self.assertEqual(csv.field_size_limit(), 32)
        self.assertEqual(csv.field_size_limit(), before)

        with self.assertRaises(RuntimeError):
            with field_size_limit(32):
                raise RuntimeError('boom')
        self.assertEqual(
            csv.field_size_limit(), before,
            'the limit was not restored when the body raised',
        )

    def test_a_missing_file_keeps_its_native_type(self):
        """Filesystem failures are not source-content failures."""
        with TempDir() as d:
            with self.assertRaises(FileNotFoundError) as raised:
                build_csv_file_resource(
                    str(d / 'nope.csv'), source_access=tc.LOCAL_FILE_ACCESS,
                )
            self.assertNotIsInstance(raised.exception, CsvReadError)

    def test_diagnostics_name_the_source_without_dumping_its_content(self):
        """Section 30: identify the source, never the row.

        A CSV that fails to parse is exactly the file whose contents should
        not reach a log, so the diagnostic carries structure -- path, line,
        logical row, expected and actual widths -- and no field values.
        """
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b;c\n1;2;3\nSECRET;VALUE\n')
            with self.assertRaises(CsvReadError) as raised:
                _rows(path)

            message = str(raised.exception)
            self.assertIn('x.csv', message)
            self.assertIn('expected=3', message)
            self.assertIn('actual=2', message)
            self.assertIn('data_row=2', message)
            self.assertNotIn('SECRET', message)
            self.assertNotIn('VALUE', message)


class Test5TablesAreLazyAndReIterable(unittest.TestCase):
    """decisions/0015 sections 23 and 26.

    The failure a one-shot generator produces is not an exception. The
    first consumer gets rows, the second gets an empty table, and the run
    publishes zero rows over a healthy-looking source. That is why
    re-iterability is asserted directly rather than inferred from the
    class it inherits.
    """

    def test_get_table_does_not_read_and_returns_a_stable_view(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)

            table = resource.get_table()
            self.assertEqual(
                access.opened, [],
                'get_table() read the source; it must only build a view',
            )
            self.assertIs(resource.get_table(), table)

    def test_every_traversal_is_a_fresh_read_of_the_source(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)
            table = resource.get_table()

            first = list(iter(table))
            second = list(iter(table))
            self.assertEqual(first, second)
            self.assertEqual(
                first, [('a', 'b'), ('1', '2')],
                'the second traversal returned nothing -- a captured '
                'one-shot generator is being reused as the backing iterable',
            )
            self.assertEqual(len(access.opened), 2)

    def test_a_changed_file_is_visible_to_the_next_traversal(self):
        """The other face of laziness, and the reason section 26 exists."""
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            resource = build_csv_file_resource(
                str(path), source_access=tc.LOCAL_FILE_ACCESS,
            )
            self.addCleanup(resource.close)
            table = resource.get_table()

            self.assertEqual(len(list(iter(table))), 2)
            _write(path, 'a;b\n1;2\n3;4\n')
            self.assertEqual(len(list(iter(table))), 3)

    def test_a_petl_chain_stays_lazy_until_it_is_traversed(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)

            chain = etl.cut(etl.convert(resource.get_table(), 'b', int), 'a', 'b')
            self.assertEqual(access.opened, [])
            self.assertEqual(list(iter(chain)), [('a', 'b'), ('1', 2)])
            self.assertEqual(len(access.opened), 1)

    def test_stabilization_collapses_repeated_traversals_to_one_read(self):
        """What protects the runner's multi-consumer path.

        table_adapters stabilizes with tbl.cache(). Asserted on cost rather
        than on rows, because the rows are identical either way -- an
        assertion on values here could not fail.
        """
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n3;4\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)
            table = resource.get_table()

            etl.nrows(table)
            list(iter(table))
            list(iter(table))
            self.assertEqual(len(access.opened), 3)

            access.opened.clear()
            cached = get_table_adapter('petl').stabilize(table, True)
            etl.nrows(cached)
            list(iter(cached))
            list(iter(cached))
            self.assertEqual(
                len(access.opened), 1,
                'stabilize() did not collapse three traversals into one read',
            )

    def test_list_of_a_petl_table_costs_two_traversals(self):
        """Inherited petl behaviour, pinned because it is invisible.

        petl's IterContainer defines __len__ as `sum(1 for _ in self)` and
        list()/tuple() call it to pre-size before iterating. On an ordinary
        materialized petl table the second pass is free; on a CSV table it
        is a second read of the file. Measured on a bare petl.Table probe
        too, so this is petl's semantics and not something this class
        introduced -- petl works around it internally, which is why
        IterContainer.list() carries the comment 'avoid iterating twice'.

        Pinned rather than fixed: overriding __len__ here would make this
        one table disagree with every other petl table in the process.
        """
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)
            table = resource.get_table()

            access.opened.clear()
            list(table)
            self.assertEqual(len(access.opened), 2)

            access.opened.clear()
            table.list()
            self.assertEqual(len(access.opened), 1)

            access.opened.clear()
            [row for row in table]
            self.assertEqual(len(access.opened), 1)

    def test_rows_are_tuples_not_the_readers_mutable_lists(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            for row in _rows(path):
                self.assertIsInstance(row, tuple)


class Test6StreamLifecycleBelongsToTheTraversal(unittest.TestCase):
    """decisions/0015 section 31: no stream outlives its own traversal.

    A leaked handle does not fail the suite. It fails later, on a network
    share, under a scheduler, as a resource exhaustion nobody traces back.
    """

    def test_a_completed_traversal_closes_its_stream(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)

            list(iter(resource.get_table()))
            self.assertEqual(len(access.opened), len(access.closed))

    def test_an_abandoned_partial_traversal_closes_its_stream(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n3;4\n5;6\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)

            iterator = iter(resource.get_table())
            next(iterator)
            del iterator
            gc.collect()

            self.assertEqual(
                len(access.closed), len(access.opened),
                'a traversal abandoned halfway left its source handle open',
            )

    def test_a_failed_traversal_closes_its_stream(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b;c\n1;2\n')
            access = _CountingAccess()
            resource = build_csv_file_resource(str(path), source_access=access)
            self.addCleanup(resource.close)

            with self.assertRaises(CsvReadError):
                list(iter(resource.get_table()))
            gc.collect()
            self.assertEqual(len(access.closed), len(access.opened))

    def test_close_is_idempotent_and_the_resource_survives_it(self):
        """Section 31 allows reuse after close(); it clears views, not state."""
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            resource = build_csv_file_resource(
                str(path), source_access=tc.LOCAL_FILE_ACCESS,
            )
            before = resource.get_table()
            resource.close()
            resource.close()

            after = resource.get_table()
            self.assertIsNot(before, after)
            self.assertEqual(list(iter(after)), [('a', 'b'), ('1', '2')])


class Test7CsvResourcesAreTracked(unittest.TestCase):
    """Selection metadata, shared with the workbook resources.

    Both builders capture a SelectedFile and hand it to the same
    single_file_fingerprint(), so exact and latest CSV cannot describe a
    selection differently from exact and latest XLSX.
    """

    def _fingerprint(self, resource):
        self.addCleanup(resource.close)
        return resource.source_fingerprint('csv')

    def test_an_exact_file_reports_kind_and_count(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            fingerprint = self._fingerprint(build_csv_file_resource(
                str(path), source_access=tc.LOCAL_FILE_ACCESS,
            ))
            self.assertEqual(fingerprint.source_kind, 'fixed_file')
            self.assertEqual(fingerprint.file_count, 1)

    def test_latest_reports_its_root_and_mask(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'a;b\n1;2\n')
            _write(d / 'b.csv', 'a;b\n3;4\n')
            os.utime(d / 'b.csv', (1_800_000_000,) * 2)

            fingerprint = self._fingerprint(build_latest_csv_resource(
                str(d), '*.csv', source_access=tc.LOCAL_FILE_ACCESS,
            ))
            self.assertEqual(fingerprint.source_kind, 'latest_file')
            self.assertEqual(fingerprint.include_mask, '*.csv')
            self.assertEqual(fingerprint.root_path, str(d))

    def test_a_changed_file_moves_the_signature(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')

            def signature():
                resource = build_csv_file_resource(
                    str(path), source_access=tc.LOCAL_FILE_ACCESS,
                )
                try:
                    return resource.source_fingerprint('csv').source_signature
                finally:
                    resource.close()

            before = signature()
            self.assertEqual(before, signature())
            _write(path, 'a;b\n1;2\n3;4\n')
            self.assertNotEqual(before, signature())

    def test_latest_picks_the_newest_and_notices_a_newer_arrival(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'a;b\n1;2\n')

            def signature():
                resource = build_latest_csv_resource(
                    str(d), '*.csv', source_access=tc.LOCAL_FILE_ACCESS,
                )
                try:
                    return resource.source_fingerprint('csv').source_signature
                finally:
                    resource.close()

            before = signature()
            _write(d / 'b.csv', 'a;b\n3;4\n5;6\n')
            os.utime(d / 'b.csv', (1_900_000_000,) * 2)
            self.assertNotEqual(
                before, signature(),
                'a newer file became the selection without moving the '
                'fingerprint',
            )

    def test_an_untracked_resource_refuses_to_fingerprint(self):
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            selected = tc.select_fixed_file_info(str(path))
            resource = csv_resource(selected, source_access=tc.LOCAL_FILE_ACCESS)
            self.addCleanup(resource.close)

            with self.assertRaises(tc.SourceCheckError) as raised:
                resource.source_fingerprint('csv')
            self.assertIn('build_csv_file_resource()', str(raised.exception))

    def test_construction_selects_the_file_exactly_once(self):
        """A captured SelectedFile is never re-resolved by path.

        Same invariant the workbook builders hold: a second filesystem
        observation would mean the fingerprint describes a look at the
        source other than the one the data load used.
        """
        with TempDir() as d:
            path = _write(d / 'x.csv', 'a;b\n1;2\n')
            calls = {'fixed': 0, 'fixed_info': 0, 'latest_info': 0}

            class CountingSelection(FileAccessImpl):
                def select_fixed_file(self, *a, **kw):
                    calls['fixed'] += 1
                    return super().select_fixed_file(*a, **kw)

                def select_fixed_file_info(self, *a, **kw):
                    calls['fixed_info'] += 1
                    return super().select_fixed_file_info(*a, **kw)

                def select_latest_file_info(self, *a, **kw):
                    calls['latest_info'] += 1
                    return super().select_latest_file_info(*a, **kw)

            exact = build_csv_file_resource(
                str(path), source_access=CountingSelection(),
            )
            self.addCleanup(exact.close)
            self.assertEqual(
                calls['fixed'], 0,
                'the captured SelectedFile was re-selected by path',
            )
            self.assertEqual(calls['fixed_info'], 1)

            calls.update(fixed=0, fixed_info=0, latest_info=0)
            latest = build_latest_csv_resource(
                str(d), '*.csv', source_access=CountingSelection(),
            )
            self.addCleanup(latest.close)
            self.assertEqual(calls['fixed'], 0)
            self.assertEqual(calls['latest_info'], 1)


class Test8TheModuleNameDoesNotShadowTheStandardLibrary(unittest.TestCase):
    """task_core/resources/csv.py sits next to modules that import stdlib csv.

    Safe because the package uses absolute imports throughout -- the same
    rule that lets task_core/types.py coexist with stdlib types. Asserted
    rather than assumed, since the failure would be a wrong module silently
    bound rather than an ImportError.
    """

    def test_the_module_binds_the_standard_library_csv(self):
        import task_core.resources.csv as module
        self.assertIs(module.csv, csv)
        self.assertTrue(hasattr(module.csv, 'field_size_limit'))

    def test_a_sibling_module_still_reaches_the_standard_library(self):
        import task_core.resources.excel as excel
        self.assertTrue(excel.__name__.startswith('task_core.'))
        self.assertIs(csv, __import__('csv'))

    def test_the_facade_exports_no_name_called_csv(self):
        self.assertFalse(
            hasattr(tc, 'csv'),
            'a facade export named csv would make task_core.csv ambiguous '
            'between the submodule and the export',
        )


class Test9TheFacadeExposesTheDocumentedSurface(unittest.TestCase):
    """decisions/0015 section 33 names exactly what becomes public."""

    def test_the_public_additions_are_reachable_from_the_facade(self):
        for name in ('CsvReadOptions', 'CsvReadError', 'xlsx_file',
                     'csv_file', 'latest_csv', 'csv_file_set'):
            with self.subTest(name):
                self.assertTrue(hasattr(tc, name), f'{name} is not exported')

    def test_internals_do_not_become_public_by_association(self):
        for name in ('iter_csv_rows', '_CsvTable', 'csv_resource'):
            with self.subTest(name):
                self.assertFalse(
                    hasattr(tc, name),
                    f'{name} is an implementation detail and section 33 '
                    f'keeps it off the facade',
                )

    def test_the_factories_produce_tracked_resource_specs(self):
        for factory, kwargs in ((tc.csv_file, {'path': 'x.csv'}),
                                (tc.latest_csv, {}),
                                (tc.csv_file_set, {})):
            with self.subTest(factory.__name__):
                self.assertTrue(factory(**kwargs).tracker)
                self.assertFalse(factory(tracker=False, **kwargs).tracker)

    def test_the_factories_reject_a_non_options_value_at_load_time(self):
        with TempDir() as d:
            _write(d / 'x.csv', 'a;b\n1;2\n')
            spec = tc.csv_file('x.csv', options={'delimiter': ','})
            env = tc.ResourceEnvironment(
                base_path=str(d), file_access=tc.LOCAL_FILE_ACCESS,
            )
            with self.assertRaises(TypeError):
                spec.loader(env)


class Test10ACsvFileSetIsOneLogicalTable(unittest.TestCase):
    """decisions/0015 sections 14-16.

    The wrapper composes the generic file-set resource rather than adding
    CSV methods to it, because that resource also holds workbooks and
    arbitrary binaries. Selection, ordering, membership and the file-set
    fingerprint stay where they already work; only parsing is new here.
    """

    def _set(self, folder, options=None, access=None):
        resource = build_csv_file_set_resource(
            str(folder), '*.csv', options,
            source_access=access or tc.LOCAL_FILE_ACCESS,
        )
        self.addCleanup(resource.close)
        return resource

    def test_members_combine_into_one_table_in_selection_order(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'name;value\ny;2\nz;3\n')

            resource = self._set(d)
            self.assertEqual(
                list(iter(resource.get_table())),
                [('name', 'value'), ('x', '1'), ('y', '2'), ('z', '3')],
            )
            self.assertEqual(
                [f.relative_path for f in resource.files], ['a.csv', 'b.csv'],
            )

    def test_no_provenance_column_is_added(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'name;value\ny;2\n')
            self.assertEqual(
                list(iter(self._set(d).get_table()))[0], ('name', 'value'),
            )

    def test_delegated_operations_come_from_the_composed_file_set(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'name;value\ny;2\n')
            resource = self._set(d)

            fingerprint = resource.source_fingerprint('csvset')
            self.assertEqual(fingerprint.source_kind, 'file_set')
            self.assertEqual(fingerprint.file_count, 2)
            self.assertEqual(fingerprint.include_mask, '*.csv')

            with resource.open_file(resource.files[0]) as handle:
                self.assertTrue(handle.read().startswith(b'name'))

    def test_an_untracked_file_set_names_the_csv_builder_when_it_refuses(self):
        """The refusal must point at a builder that would actually help.

        The composed file_set_resource says 'built via
        build_file_set_resource()', which is correct advice for a workbook
        set and the wrong builder here. Reachable: this class can be
        constructed directly around a file set carrying no selection
        metadata, even though its own builder always supplies some.
        """
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            selected = tc.select_fixed_file_info(str(d / 'a.csv'))
            bare = file_set_resource(
                [selected], source_access=tc.LOCAL_FILE_ACCESS,
            )

            resource = csv_file_set_resource(bare)
            with self.assertRaises(tc.SourceCheckError) as raised:
                resource.source_fingerprint('csvset')

            self.assertIn(
                'build_csv_file_set_resource()', str(raised.exception),
                'the refusal names a builder that does not produce this '
                'resource',
            )
            self.assertIsInstance(
                raised.exception.__cause__, tc.SourceCheckError,
                'the underlying file-set refusal was discarded rather than '
                'chained',
            )

    def test_an_inferred_header_must_match_exactly_across_members(self):
        """Case, whitespace, order and width -- no reconciliation.

        Asserted on a case difference specifically, because that is the
        mismatch a union-by-name or case-insensitive implementation would
        silently absorb, producing a table that looks right and quietly
        misfiles a column.
        """
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'name;VALUE\ny;2\n')

            with self.assertRaises(CsvReadError) as raised:
                list(iter(self._set(d).get_table()))
            self.assertIn('b.csv', str(raised.exception))

    def test_the_mismatch_check_compares_against_the_first_member(self):
        """A guard against the shape this loop had first.

        Handing member N the schema member 1 established makes member N
        emit *that* schema as its header, so the comparison compares the
        first header with itself and no mismatch can ever fail. The bug
        passed every combined-table test; only a deliberately conflicting
        second member exposes it.
        """
        with TempDir() as d:
            _write(d / 'a.csv', 'one;two\n1;2\n')
            _write(d / 'b.csv', 'three;four\n3;4\n')

            with self.assertRaises(CsvReadError):
                list(iter(self._set(d).get_table()))

    def test_explicit_columns_absorb_differing_header_spellings(self):
        """The supported mechanism for a feed whose header names move."""
        with TempDir() as d:
            _write(d / 'a.csv', 'ID;Name\n1;bob\n')
            _write(d / 'b.csv', 'identifier;description\n2;ann\n')

            resource = self._set(d, CsvReadOptions(columns=['id', 'name']))
            self.assertEqual(
                list(iter(resource.get_table())),
                [('id', 'name'), ('1', 'bob'), ('2', 'ann')],
            )

    def test_headerless_members_share_the_first_members_width(self):
        with TempDir() as d:
            _write(d / 'a.csv', '1;2\n3;4\n')
            _write(d / 'b.csv', '5;6\n')
            options = CsvReadOptions(header=False)

            self.assertEqual(
                list(iter(self._set(d, options).get_table())),
                [('Column1', 'Column2'), ('1', '2'), ('3', '4'), ('5', '6')],
            )

            _write(d / 'c.csv', '7;8;9\n')
            with self.assertRaises(CsvReadError):
                list(iter(self._set(d, options).get_table()))

    def test_empty_members_contribute_nothing_and_veto_nothing(self):
        with TempDir() as d:
            _write(d / 'a.csv', '')
            _write(d / 'b.csv', '\n\n')
            _write(d / 'c.csv', 'name;value\nx;1\n')

            self.assertEqual(
                list(iter(self._set(d).get_table())),
                [('name', 'value'), ('x', '1')],
                'an empty member displaced the schema of a usable one',
            )

    def test_a_set_with_no_usable_record_at_all(self):
        with TempDir() as d:
            _write(d / 'a.csv', '')
            _write(d / 'b.csv', '\n\n')

            with self.assertRaises(CsvReadError) as raised:
                list(iter(self._set(d).get_table()))
            self.assertIn('cannot be inferred', str(raised.exception))

            self.assertEqual(
                list(iter(self._set(
                    d, CsvReadOptions(columns=['a', 'b'])).get_table())),
                [('a', 'b')],
                'with a declared schema an unusable set is a zero-row table, '
                'not an error',
            )

    def test_only_one_member_is_open_at_a_time(self):
        """Section 14, asserted on peak concurrency rather than on rows.

        A implementation that chained per-member generators eagerly would
        produce identical rows while holding every handle open at once --
        invisible locally, a real limit on a share with hundreds of files.
        """
        with TempDir() as d:
            for index in range(5):
                _write(d / f'f{index}.csv', f'a;b\n{index};x\n')

            live = {'now': 0, 'max': 0}

            class TrackingAccess(FileAccessImpl):
                @contextmanager
                def open_binary(self, path, *, buffered=False):
                    with super().open_binary(path, buffered=buffered) as handle:
                        live['now'] += 1
                        live['max'] = max(live['max'], live['now'])
                        try:
                            yield handle
                        finally:
                            live['now'] -= 1

            resource = self._set(d, access=TrackingAccess())
            rows = list(iter(resource.get_table()))

            self.assertEqual(len(rows), 6)
            self.assertEqual(
                live['max'], 1,
                'more than one member file was open at the same time',
            )
            self.assertEqual(live['now'], 0)

    def test_the_combined_table_is_re_iterable(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'name;value\ny;2\n')
            table = self._set(d).get_table()

            self.assertEqual(list(iter(table)), list(iter(table)))
            self.assertEqual(len(list(iter(table))), 3)


class Test11PerMemberTablesAreIndependent(unittest.TestCase):
    """get_file_table() exists so a conflicting member can be looked at.

    Cross-file header agreement is a property of the combined table only.
    Refusing to parse the very member whose header broke the set would
    remove the one view an author needs in order to fix it.
    """

    def _set(self, folder, options=None):
        resource = build_csv_file_set_resource(
            str(folder), '*.csv', options, source_access=tc.LOCAL_FILE_ACCESS,
        )
        self.addCleanup(resource.close)
        return resource

    def test_a_member_that_breaks_the_set_can_still_be_inspected(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'name;VALUE\ny;2\n')
            resource = self._set(d)

            with self.assertRaises(CsvReadError):
                list(iter(resource.get_table()))

            conflicting = [f for f in resource.files
                           if f.relative_path == 'b.csv'][0]
            self.assertEqual(
                list(iter(resource.get_file_table(conflicting))),
                [('name', 'VALUE'), ('y', '2')],
            )

    def test_a_member_schema_is_inferred_locally(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(d / 'b.csv', 'other;cols;here\n1;2;3\n')
            resource = self._set(d)

            tables = {
                f.relative_path: list(iter(resource.get_file_table(f)))
                for f in resource.files
            }
            self.assertEqual(tables['a.csv'][0], ('name', 'value'))
            self.assertEqual(tables['b.csv'][0], ('other', 'cols', 'here'))

    def test_membership_is_value_based_and_a_foreigner_is_refused(self):
        """Inherited `in self.files` semantics, not identity.

        SelectedFile is a frozen dataclass whose equality includes the
        whole os.stat_result. That is inherited from the generic file-set
        resource deliberately -- tightening the composed resource to
        identity would make it disagree with the one it composes.

        One caveat, measured rather than reasoned: stat_result equality
        covers st_atime, so on a filesystem that updates access times a
        separately-selected twin of a member that has been read could stop
        comparing equal. It did not on this machine (NTFS with last-access
        updates disabled, the current Windows default), so the twin case is
        asserted only for what it demonstrably is -- equal here -- while
        the two guarantees that do not depend on the filesystem get
        unconditional assertions. An `if twin == member:` guard was the
        first shape of this test and would simply vanish where the caveat
        bites.
        """
        with TempDir() as d, TempDir() as other:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            _write(other / 'z.csv', 'name;value\nq;9\n')
            resource = self._set(d)
            member = resource.files[0]

            self.assertEqual(
                list(iter(resource.get_file_table(member))),
                [('name', 'value'), ('x', '1')],
            )

            foreign = tc.select_fixed_file_info(str(other / 'z.csv'))
            with self.assertRaises(ValueError):
                resource.get_file_table(foreign)

            twin = tc.select_fixed_file_info(str(d / 'a.csv'))
            self.assertEqual(
                twin, member,
                'a separately-selected SelectedFile for the same untouched '
                'file no longer compares equal -- if this starts failing, '
                'check whether the filesystem began recording access times, '
                'because membership would then depend on whether a member '
                'had been read',
            )
            self.assertEqual(
                list(iter(resource.get_file_table(twin))),
                [('name', 'value'), ('x', '1')],
            )

    def test_a_member_table_is_re_iterable(self):
        with TempDir() as d:
            _write(d / 'a.csv', 'name;value\nx;1\n')
            resource = self._set(d)
            table = resource.get_file_table(resource.files[0])
            self.assertEqual(list(iter(table)), list(iter(table)))


if __name__ == '__main__':
    unittest.main()
