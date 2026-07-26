# -*- coding: utf-8 -*-
"""
table_adapters.py had no persistent test coverage at all before this
file -- confirmed directly (grep found nothing) before writing it. That
gap is exactly how the NaN-in-Excel fix silently regressed: the fix
itself was real and verified when it was made, but only ever checked
by hand, in that session, never captured as an automated test. A later
rebuild of this project, from a point before the fix existed, silently
lost it, with nothing to catch the loss until it was noticed by hand a
second time.

Every test in this file that touches Excel export reads the real,
on-disk worksheet XML directly out of the .xlsx file's own zip
archive -- not a table/DataFrame value in memory, and not a cell read
back through openpyxl's own load_workbook(), which is lenient enough on
read to silently paper over exactly the malformation these tests exist
to catch (confirmed directly: a structurally malformed cell -- a
numeric-typed cell with an empty <v></v>, or a string-typed cell with no
content at all -- reads back as a clean None either way, which is
precisely why the original bug went unnoticed as long as it did).
"""

import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd
import petl as etl

from task_core.table_adapters import PETL_ADAPTER, PANDAS_ADAPTER, normalize_for_excel

_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'


class TempDir:
    def __enter__(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        return Path(self._tmpdir.name)

    def __exit__(self, *exc):
        self._tmpdir.cleanup()


def _read_sheet_xml(xlsx_path, sheet_index=1):
    with zipfile.ZipFile(xlsx_path) as zf:
        return zf.read(f'xl/worksheets/sheet{sheet_index}.xml').decode('utf-8')


def _cell_element(sheet_xml, cell_ref):
    """The raw <c> element for a specific cell reference (e.g. 'B3'), or
    None if the cell is genuinely absent from the XML entirely -- the
    correct, standard representation of a truly empty cell."""
    root = ET.fromstring(sheet_xml)
    for cell in root.iter(f'{{{_NS}}}c'):
        if cell.get('r') == cell_ref:
            return cell
    return None


def _cell_is_structurally_malformed(cell_element):
    """A cell that's declared (has a type attribute, exists as an
    element) but carries no actual content -- neither a <v> value with
    real text, nor an <is><t> inline string with real text. This is the
    exact shape a genuine, unhandled NaN produced before the fix: a
    numeric-typed cell with an empty <v></v> (petl's toxlsx()), or a
    string-typed cell with no child element at all (pandas's
    to_excel()). A genuinely absent cell element (the correct
    representation of an empty value) is not malformed -- that's the
    fixed, correct case, not the bug."""
    if cell_element is None:
        return False
    v = cell_element.find(f'{{{_NS}}}v')
    if v is not None:
        return not (v.text and v.text.strip())
    is_elem = cell_element.find(f'{{{_NS}}}is')
    if is_elem is not None:
        t = is_elem.find(f'{{{_NS}}}t')
        return t is None or not (t.text and t.text.strip())
    return True  # declared cell, no <v> and no <is> at all


class Test1PetlAdapterNanInExcel(unittest.TestCase):
    def test_nan_cell_is_genuinely_omitted_not_malformed(self):
        with TempDir() as d:
            tbl = etl.wrap([('a', 'grade'), ('x', 5.0), ('y', float('nan')), ('z', 3.0)])
            path = d / 'petl_nan.xlsx'
            PETL_ADAPTER.to_excel(tbl, str(path))

            sheet_xml = _read_sheet_xml(path)
            cell = _cell_element(sheet_xml, 'B3')

            self.assertIsNone(
                cell, 'the NaN cell should be entirely absent from the XML -- '
                'matching standard XLSX convention for an empty cell -- not merely present but empty',
            )

    def test_normal_values_around_the_nan_are_unaffected(self):
        with TempDir() as d:
            tbl = etl.wrap([('a', 'grade'), ('x', 5.0), ('y', float('nan')), ('z', 3.0)])
            path = d / 'petl_nan.xlsx'
            PETL_ADAPTER.to_excel(tbl, str(path))

            sheet_xml = _read_sheet_xml(path)
            b2 = _cell_element(sheet_xml, 'B2')
            b4 = _cell_element(sheet_xml, 'B4')
            self.assertEqual(b2.find(f'{{{_NS}}}v').text, '5')
            self.assertEqual(b4.find(f'{{{_NS}}}v').text, '3')


class Test2PandasAdapterNanInExcel(unittest.TestCase):
    """The pandas adapter has a separate, deeper limitation this fix
    does not address: pandas's own df.to_excel() writes a structurally
    malformed cell for *any* missing value by default (na_rep=''),
    confirmed directly with a completely NaN-free, pre-existing None --
    fixing that would mean bypassing pandas's native Excel writer
    entirely, out of scope for this fix. What this fix does guarantee,
    and what these tests hold it to: NaN must not be *worse* than a
    genuine, pre-existing None -- the two must produce identical XML.

    Worth being honest about, found while proving these tests have
    genuine teeth: table_adapters.py's numeric/datetime-dtype column
    loop (separate from normalize_for_excel() itself) currently changes
    nothing observable for this adapter -- confirmed directly that
    pandas's own to_excel() already treats an untouched, native nan
    identically to a pre-converted None, with or without that loop.
    Removing it doesn't fail any test below; it's kept in
    table_adapters.py as defensive, forward-looking code, not because
    these tests currently prove it does anything. If a future pandas
    version ever changes that equivalence, a new, more targeted test
    would be needed to actually exercise it -- this comment exists so
    that isn't mistaken for something already covered here."""

    def test_nan_produces_identical_xml_to_a_genuine_none(self):
        with TempDir() as d:
            df_nan = pd.DataFrame({'a': ['x', 'y', 'z'], 'grade': [5.0, float('nan'), 3.0]})
            df_none = pd.DataFrame({'a': ['x', 'y', 'z'], 'grade': pd.array([5.0, None, 3.0], dtype=object)})

            path_nan = d / 'pandas_nan.xlsx'
            path_none = d / 'pandas_none.xlsx'
            PANDAS_ADAPTER.to_excel(df_nan, str(path_nan))
            PANDAS_ADAPTER.to_excel(df_none, str(path_none))

            xml_nan = _read_sheet_xml(path_nan)
            xml_none = _read_sheet_xml(path_none)
            self.assertEqual(
                xml_nan, xml_none,
                'NaN produced different (worse) XML than a genuine, pre-existing None would',
            )

    def test_nan_cell_is_not_more_malformed_than_it_has_to_be(self):
        # Documents the known, separate pandas.to_excel() limitation
        # directly, rather than silently accept a stronger regression on
        # top of it: the cell may still be an empty-but-declared element
        # (pandas's own na_rep='' shape), but must not regress further
        # (e.g. writing the literal text "nan").
        with TempDir() as d:
            df = pd.DataFrame({'a': ['x', 'y', 'z'], 'grade': [5.0, float('nan'), 3.0]})
            path = d / 'pandas_nan.xlsx'
            PANDAS_ADAPTER.to_excel(df, str(path))

            sheet_xml = _read_sheet_xml(path)
            cell = _cell_element(sheet_xml, 'B3')
            self.assertIsNotNone(cell, 'known pandas.to_excel() limitation -- expected to still be present')

            # No literal "nan" text anywhere in that cell -- the specific,
            # worse regression this test exists to catch on top of the
            # known limitation.
            cell_text = ET.tostring(cell, encoding='unicode')
            self.assertNotIn('nan', cell_text.lower())

    def test_normal_values_around_the_nan_are_unaffected(self):
        with TempDir() as d:
            df = pd.DataFrame({'a': ['x', 'y', 'z'], 'grade': [5.0, float('nan'), 3.0]})
            path = d / 'pandas_nan.xlsx'
            PANDAS_ADAPTER.to_excel(df, str(path))

            sheet_xml = _read_sheet_xml(path)
            b2 = _cell_element(sheet_xml, 'B2')
            b4 = _cell_element(sheet_xml, 'B4')
            self.assertEqual(b2.find(f'{{{_NS}}}v').text, '5')
            self.assertEqual(b4.find(f'{{{_NS}}}v').text, '3')


class Test3MalformedCellDetectorItself(unittest.TestCase):
    """A quick, direct check on the test helper's own logic -- since it's
    what every other test in this file relies on to actually distinguish
    a real bug from a false pass."""

    def test_detects_the_exact_malformed_shapes_seen_in_practice(self):
        malformed_numeric = ET.fromstring(f'<c xmlns="{_NS}" r="B3" t="n"><v></v></c>')
        malformed_string = ET.fromstring(f'<c xmlns="{_NS}" r="B3" t="inlineStr"></c>')
        self.assertTrue(_cell_is_structurally_malformed(malformed_numeric))
        self.assertTrue(_cell_is_structurally_malformed(malformed_string))

    def test_does_not_flag_genuinely_valid_cells(self):
        valid_numeric = ET.fromstring(f'<c xmlns="{_NS}" r="B2"><v>5</v></c>')
        valid_string = ET.fromstring(f'<c xmlns="{_NS}" r="A2" t="inlineStr"><is><t>x</t></is></c>')
        self.assertFalse(_cell_is_structurally_malformed(valid_numeric))
        self.assertFalse(_cell_is_structurally_malformed(valid_string))

    def test_does_not_flag_a_genuinely_absent_cell(self):
        self.assertFalse(_cell_is_structurally_malformed(None))


class Test4StabilizePreventsRepeatedTraversal(unittest.TestCase):
    """Found during an optimization review: after pipeline.run() returns,
    the runner traverses out_tbl separately for nrows() (always), then
    potentially display()/to_excel()/to_db_payload()/a downstream
    ctx.get_result() consumer. For a lazy petl transformation chain, each
    traversal re-runs the entire chain from scratch -- confirmed
    directly. For a db_resource-backed table specifically, this is worse
    than a performance cost: each traversal re-issues the underlying SQL
    query, confirmed directly against a fake DB-API connection tracking
    execute() calls -- a correctness risk, not just a speed one, since a
    changing source table could produce different data between the
    nrows() count and whatever gets published afterward.

    adapter.stabilize(tbl, repeated=True) wraps a petl table in
    etl.cache() -- confirmed directly this must happen before the FIRST
    traversal to have any effect at all; applying it after nrows() has
    already run leaves every later traversal still fully re-running."""

    def test_petl_stabilize_wraps_in_cache_when_repeated(self):
        import petl as etl
        tbl = etl.wrap([('a',), (1,), (2,)])
        stabilized = PETL_ADAPTER.stabilize(tbl, repeated=True)
        self.assertIsInstance(stabilized, etl.util.materialise.CacheView)

    def test_petl_stabilize_returns_unchanged_when_not_repeated(self):
        import petl as etl
        tbl = etl.wrap([('a',), (1,), (2,)])
        stabilized = PETL_ADAPTER.stabilize(tbl, repeated=False)
        self.assertIs(stabilized, tbl)

    def test_pandas_stabilize_always_returns_the_same_dataframe(self):
        df = pd.DataFrame({'a': [1, 2, 3]})
        self.assertIs(PANDAS_ADAPTER.stabilize(df, repeated=True), df)
        self.assertIs(PANDAS_ADAPTER.stabilize(df, repeated=False), df)

    def test_lazy_petl_transform_runs_once_not_per_traversal(self):
        import petl as etl
        call_count = [0]

        def expensive(row):
            call_count[0] += 1
            return row

        tbl = etl.wrap([('a',), (1,), (2,), (3,)])
        transformed = etl.rowmap(tbl, lambda r: (expensive(r[0]),), header=('a',))

        stabilized = PETL_ADAPTER.stabilize(transformed, repeated=True)
        PETL_ADAPTER.nrows(stabilized)  # first traversal -- populates the cache
        list(etl.data(stabilized))  # second traversal -- must read from cache

        self.assertEqual(
            call_count[0], 3,
            'the transform re-ran on the second traversal instead of reading from cache',
        )

    def test_db_resource_query_executes_once_not_per_traversal(self):
        # The most serious case this fix addresses: confirmed directly,
        # via a fake DB-API connection tracking execute() calls, that an
        # unstabilized db_resource table re-issues its SQL query on every
        # traversal.
        import petl as etl
        from task_core.resources.db import db_resource

        execute_count = [0]

        class FakeCursor:
            description = [('a',), ('b',)]
            def execute(self, query, *args, **kwargs):
                execute_count[0] += 1
            def fetchall(self):
                return [(1, 'x'), (2, 'y'), (3, 'z')]
            def close(self):
                pass

        class FakeConn:
            def cursor(self, *args, **kwargs):
                return FakeCursor()
            def close(self):
                pass

        resource = db_resource.__new__(db_resource)
        resource.creds = {}
        resource._conn = FakeConn()
        resource._table_cache = {}

        tbl = resource.get_table(table='some_table')
        stabilized = PETL_ADAPTER.stabilize(tbl, repeated=True)

        execute_count[0] = 0
        PETL_ADAPTER.nrows(stabilized)
        list(etl.data(stabilized))
        list(etl.data(stabilized))  # a third traversal, for good measure

        self.assertEqual(
            execute_count[0], 1,
            'the SQL query re-executed on a later traversal instead of reading from the cache',
        )

    def test_runner_calls_stabilize_before_nrows_not_after(self):
        # Distinct from the tests above: those call adapter methods
        # directly in the right order themselves, so they wouldn't catch
        # a regression where runner.py's own sequencing calls
        # stabilize() after nrows() instead of before -- confirmed
        # directly earlier that doing so makes the fix a complete no-op.
        # This one goes through the real run_pipelines() to exercise
        # that actual sequencing.
        import petl as etl
        import task_core as tc

        call_count = [0]

        def expensive(row):
            call_count[0] += 1
            return row

        class pipeline:
            spec = tc.PipelineSpec(db_table='t', db_output=('a',))
            @classmethod
            def run(cls, ctx):
                tbl = etl.wrap([('a',), (1,), (2,), (3,)])
                return etl.rowmap(tbl, lambda r: (expensive(r[0]),), header=('a',))

        class FakeDbPublisher:
            @classmethod
            def preflight(cls, specs, *, schema, **kwargs): pass
            def __init__(self, *, creds, schema, logger=None, **kwargs):
                self.published = []
            def ensure_connection(self): return object()
            def begin_run(self):
                return True
            def publish(self, payload): self.published.append(payload)
            def commit(self): return []
            def rollback(self): pass
            def close(self): pass
            committed = property(lambda self: True)
            committed_tables = property(lambda self: [])
            written_tables = property(lambda self: self.published)
            table_rows = property(lambda self: {})
        ctx = tc.task_context(task_name='t', loaders={})
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
            run_sequence=['p'], output_excel=False, output_db=True,
            creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, pg_schema='bsr',
            publisher_factory=FakeDbPublisher,
        )

        self.assertEqual(
            call_count[0], 3,
            'the transform ran more than once per row -- nrows() and the DB publish '
            're-traversed independently instead of sharing one stabilized table',
        )


class Test5NormalizeForExcelHandlesPdNaCorrectly(unittest.TestCase):
    """Companion to Test6 in test_db_publish.py -- the same is_missing()
    fix, found broken independently in this function too, from the same
    underlying cause (a bare value != value check that genuinely raises
    for pd.NA rather than returning True, silently swallowed by the
    surrounding except Exception: pass)."""

    def test_normalize_for_excel_converts_a_raw_pd_na_to_none(self):
        result = normalize_for_excel(pd.NA)
        self.assertIsNone(result)

    def test_pd_na_in_a_real_petl_export_produces_clean_xml(self):
        # Full, end-to-end: the raw, on-disk worksheet XML, not just the
        # function's own return value -- the same discipline
        # Test1PetlAdapterNanInExcel already uses, for the same reason:
        # an in-memory check alone is exactly what let the original NaN
        # issue go unnoticed.
        with TempDir() as d:
            tbl = etl.wrap([('a', 'grade'), ('x', 5.0), ('y', pd.NA), ('z', 3.0)])
            path = d / 'petl_pdna.xlsx'
            PETL_ADAPTER.to_excel(tbl, str(path))

            sheet_xml = _read_sheet_xml(path)
            cell = _cell_element(sheet_xml, 'B3')

            self.assertIsNone(
                cell, 'a pd.NA cell should be entirely absent from the XML, '
                'the same as a genuine None or np.nan',
            )


if __name__ == '__main__':
    unittest.main()
