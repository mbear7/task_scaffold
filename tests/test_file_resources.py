# -*- coding: utf-8 -*-
"""
File-resource selection and fingerprinting -- which physical input gets
processed, and whether a fingerprint change correctly triggers a rerun.
Real temp directories and real files throughout, not mocks: filesystem
timing, ordering, and stat behavior are exactly the kind of thing a mock
would have to reimplement correctly to be worth testing at all.
"""

import os
import tempfile
import time
import unittest
from pathlib import Path

import task_core as tc
from task_core.file_access import file_access as FileAccessImpl, NoMatchingFilesError
from task_core.resources.excel import build_latest_xlsx_resource
from task_core.resources.file_set import build_file_set_resource


class TempDir:
    def __enter__(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        return Path(self._tmpdir.name)

    def __exit__(self, *exc):
        self._tmpdir.cleanup()


def _write(path, content=b'x'):
    path.write_bytes(content)


def _write_xlsx(path):
    from openpyxl import Workbook
    Workbook().save(path)


class _FixedOrderAccess:
    """Wraps select_file_infos to hand results back in a caller-specified
    order, regardless of whatever order the real filesystem's glob
    happens to produce. Needed because these tests must genuinely prove
    mtime-based selection, not pass by coincidence of glob's own,
    filesystem-dependent order -- found by deliberately breaking
    build_latest_xlsx_resource's selection logic entirely and discovering
    the first version of these tests still passed, since glob on this
    filesystem happened to return the newer file first anyway."""

    def __init__(self, order):
        self._real = tc.LOCAL_FILE_ACCESS
        self._order = order

    def select_file_infos(self, *a, **kw):
        infos = self._real.select_file_infos(*a, **kw)
        by_name = {Path(f.path).name: f for f in infos}
        return [by_name[n] for n in self._order]

    def __getattr__(self, name):
        return getattr(self._real, name)


class Test1LatestFileByModificationTime(unittest.TestCase):
    def test_most_recently_modified_file_is_selected(self):
        with TempDir() as d:
            older = d / 'a.xlsx'
            newer = d / 'b.xlsx'
            _write_xlsx(older)
            _write_xlsx(newer)
            base_time = time.time()
            os.utime(older, (base_time, base_time))
            os.utime(newer, (base_time + 10, base_time + 10))

            # older listed first -- a selection that just picked
            # file_infos[0] would pick the wrong (older) one here.
            access = _FixedOrderAccess(['a.xlsx', 'b.xlsx'])
            resource = build_latest_xlsx_resource(str(d), pattern='*.xlsx', source_access=access)
            self.assertEqual(resource.file_path, str(newer))


class Test2DeterministicTieBreak(unittest.TestCase):
    def test_equal_modification_times_break_tie_by_path(self):
        with TempDir() as d:
            a = d / 'a.xlsx'
            z = d / 'z.xlsx'
            _write_xlsx(a)
            _write_xlsx(z)
            same_time = time.time()
            os.utime(a, (same_time, same_time))
            os.utime(z, (same_time, same_time))

            # a.xlsx listed first -- max()'s tie-break key is
            # (mtime, path), so the lexicographically larger path (z)
            # must win regardless of which one was seen first.
            access = _FixedOrderAccess(['a.xlsx', 'z.xlsx'])
            resource = build_latest_xlsx_resource(str(d), pattern='*.xlsx', source_access=access)
            self.assertEqual(resource.file_path, str(z))

    def test_tie_break_is_stable_regardless_of_input_order(self):
        # Same fixture, opposite listing order -- confirms the result
        # doesn't depend on which order the files happen to be seen in.
        with TempDir() as d:
            z = d / 'z.xlsx'
            a = d / 'a.xlsx'
            _write_xlsx(z)
            _write_xlsx(a)
            same_time = time.time()
            os.utime(a, (same_time, same_time))
            os.utime(z, (same_time, same_time))

            access = _FixedOrderAccess(['z.xlsx', 'a.xlsx'])
            resource = build_latest_xlsx_resource(str(d), pattern='*.xlsx', source_access=access)
            self.assertEqual(resource.file_path, str(z))


class Test3FileSetOrderIndependentOfDiscoveryOrder(unittest.TestCase):
    def test_sorted_output_regardless_of_scrambled_input_order(self):
        with TempDir() as d:
            for name in ('c.xlsx', 'a.xlsx', 'b.xlsx'):
                _write(d / name)

            class ScramblingAccess(FileAccessImpl):
                """Wraps select_file_infos to hand results back in a
                deliberately different order each call, simulating
                unstable filesystem enumeration order."""

                def __init__(self, order):
                    super().__init__()
                    self._order = order

                def select_file_infos(self, *a, **kw):
                    infos = super().select_file_infos(*a, **kw)
                    by_name = {Path(f.path).name: f for f in infos}
                    return [by_name[n] for n in self._order]

            access1 = ScramblingAccess(['c.xlsx', 'a.xlsx', 'b.xlsx'])
            access2 = ScramblingAccess(['b.xlsx', 'c.xlsx', 'a.xlsx'])

            resource1 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=access1)
            resource2 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=access2)

            names1 = [Path(f.path).name for f in resource1.files]
            names2 = [Path(f.path).name for f in resource2.files]
            self.assertEqual(names1, names2, 'file-set order depended on discovery order')
            self.assertEqual(names1, ['a.xlsx', 'b.xlsx', 'c.xlsx'])


class Test4FingerprintChangesOnAddOrRemove(unittest.TestCase):
    def test_adding_a_file_changes_the_fingerprint(self):
        with TempDir() as d:
            _write(d / 'a.xlsx')
            resource1 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig1 = resource1.source_fingerprint('files').source_signature

            _write(d / 'b.xlsx')
            resource2 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig2 = resource2.source_fingerprint('files').source_signature

            self.assertNotEqual(sig1, sig2)

    def test_removing_a_file_changes_the_fingerprint(self):
        with TempDir() as d:
            _write(d / 'a.xlsx')
            _write(d / 'b.xlsx')
            resource1 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig1 = resource1.source_fingerprint('files').source_signature

            (d / 'b.xlsx').unlink()
            resource2 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig2 = resource2.source_fingerprint('files').source_signature

            self.assertNotEqual(sig1, sig2)


class Test5FingerprintChangesOnSizeOrMtime(unittest.TestCase):
    def test_changed_file_size_changes_the_fingerprint(self):
        with TempDir() as d:
            _write(d / 'a.xlsx', b'short')
            resource1 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig1 = resource1.source_fingerprint('files').source_signature

            _write(d / 'a.xlsx', b'a much longer piece of content than before')
            resource2 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig2 = resource2.source_fingerprint('files').source_signature

            self.assertNotEqual(sig1, sig2)

    def test_changed_modification_time_changes_the_fingerprint(self):
        with TempDir() as d:
            path = d / 'a.xlsx'
            _write(path, b'same content throughout')
            resource1 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig1 = resource1.source_fingerprint('files').source_signature

            new_time = time.time() + 3600
            os.utime(path, (new_time, new_time))
            resource2 = build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)
            sig2 = resource2.source_fingerprint('files').source_signature

            self.assertNotEqual(sig1, sig2, 'same content, only mtime changed, but fingerprint stayed the same')


class Test6FingerprintStableAcrossDiscoveryOrder(unittest.TestCase):
    def test_fingerprint_signature_identical_regardless_of_scrambled_input_order(self):
        with TempDir() as d:
            for name in ('c.xlsx', 'a.xlsx', 'b.xlsx'):
                _write(d / name)

            class ScramblingAccess(FileAccessImpl):
                def __init__(self, order):
                    super().__init__()
                    self._order = order

                def select_file_infos(self, *a, **kw):
                    infos = super().select_file_infos(*a, **kw)
                    by_name = {Path(f.path).name: f for f in infos}
                    return [by_name[n] for n in self._order]

            resource1 = build_file_set_resource(
                str(d), pattern='*.xlsx', source_access=ScramblingAccess(['c.xlsx', 'a.xlsx', 'b.xlsx']),
            )
            resource2 = build_file_set_resource(
                str(d), pattern='*.xlsx', source_access=ScramblingAccess(['b.xlsx', 'c.xlsx', 'a.xlsx']),
            )

            sig1 = resource1.source_fingerprint('files').source_signature
            sig2 = resource2.source_fingerprint('files').source_signature
            self.assertEqual(sig1, sig2, 'fingerprint signature depended on discovery order')


class Test7FingerprintUsesAlreadySelectedFiles(unittest.TestCase):
    def test_file_set_resource_lists_the_directory_exactly_once(self):
        with TempDir() as d:
            _write(d / 'a.xlsx')
            _write(d / 'b.xlsx')

            call_count = [0]

            class CountingAccess(FileAccessImpl):
                def select_file_infos(self, *a, **kw):
                    call_count[0] += 1
                    return super().select_file_infos(*a, **kw)

            resource = build_file_set_resource(str(d), pattern='*.xlsx', source_access=CountingAccess())
            self.assertEqual(call_count[0], 1)

            # Using both .files and .source_fingerprint() afterward must
            # not trigger a second listing -- both read the same,
            # already-selected tuple.
            _ = resource.files
            _ = resource.source_fingerprint('files')
            self.assertEqual(call_count[0], 1, 'accessing .files/.source_fingerprint() re-listed the directory')

    def test_latest_xlsx_resource_lists_the_directory_exactly_once(self):
        with TempDir() as d:
            _write_xlsx(d / 'a.xlsx')

            call_count = [0]

            class CountingAccess(FileAccessImpl):
                def select_file_infos(self, *a, **kw):
                    call_count[0] += 1
                    return super().select_file_infos(*a, **kw)

            resource = build_latest_xlsx_resource(str(d), pattern='*.xlsx', source_access=CountingAccess())
            self.assertEqual(call_count[0], 1)

            _ = resource.source_fingerprint('files')
            self.assertEqual(call_count[0], 1, 'source_fingerprint() re-listed the directory instead of reusing the selection')


class Test8NoMatchAndOnEmpty(unittest.TestCase):
    def test_no_matching_files_raises_by_default(self):
        with TempDir() as d:
            with self.assertRaises(NoMatchingFilesError):
                build_file_set_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)

    def test_on_empty_empty_returns_a_valid_empty_resource_instead(self):
        with TempDir() as d:
            resource = build_file_set_resource(
                str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS, on_empty='empty',
            )
            self.assertEqual(resource.files, ())

    def test_latest_xlsx_no_match_raises_clearly(self):
        with TempDir() as d:
            with self.assertRaises(NoMatchingFilesError):
                build_latest_xlsx_resource(str(d), pattern='*.xlsx', source_access=tc.LOCAL_FILE_ACCESS)


if __name__ == '__main__':
    unittest.main()
