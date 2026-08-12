"""The SMB branch of file_access, offline, against a strict fake.

Everything gated by `_use_smb()` used to be invisible to this suite: it
runs without a network, so every other test takes the local branch. The
one exception was the errno guard added alongside these, and it exists
*because* an assumption about `smbclient` turned out to be wrong -- the
`except FileNotFoundError` around a remote stat could never fire, since
`SMBOSError` subclasses `OSError` directly. It read as working code for
as long as nobody ran it against a share.

So the fake here is written from measurement, not from what seemed
reasonable. Every behaviour it models was observed against a real server:

- `stat()` returns an object carrying `st_mode`, `st_size`, `st_mtime`
  and a populated `st_file_attributes` (measured `0x20`, ARCHIVE, on an
  ordinary file -- so the hidden and system filters genuinely work over
  SMB rather than silently passing everything through);
- a missing path raises `SMBOSError` with `errno == ENOENT`, and that
  class is **not** a `FileNotFoundError`;
- an over-long or malformed name raises the same class with `errno == 0`,
  which is what makes the non-ENOENT branch reachable at all;
- `walk()` yields `(dirpath, dirnames, filenames)`;
- `open_file()` is a context manager over a binary handle.

The fake is deliberately strict: any call it was not told to expect, any
missing connection keyword, or a `Path` where a `str` is required, fails
the test rather than being quietly tolerated. An unverified fake
behaviour defaults to permissive, which is the direction that hides bugs.
"""

import errno
import io
import stat as stat_module
import unittest
from contextlib import contextmanager
from pathlib import Path

from task_core.file_access import NoMatchingFilesError
from task_core.file_access import source_access as SourceAccess

CREDS = {'username': 'DOMAIN\\svc', 'password': 'secret'}
SHARE = '\\\\server\\share\\folder'


class SMBOSError(OSError):
    """What smbclient raises. Measured: an OSError, not a FileNotFoundError.

    Reproduced as its own class rather than imported so this suite never
    needs smbclient installed -- and so the one property that matters
    cannot drift: it must not be a FileNotFoundError.
    """


class FakeStat:
    def __init__(self, *, size=10, mtime=1_000_000, directory=False,
                 attributes=0x20):
        self.st_mode = (stat_module.S_IFDIR if directory else stat_module.S_IFREG) | 0o644
        self.st_size = size
        self.st_mtime = mtime
        self.st_atime = mtime
        self.st_ctime = mtime
        self.st_file_attributes = attributes


class StrictSmbClient:
    """A fake smbclient that fails on anything it was not told to expect."""

    def __init__(self, *, stats=None, walk=None, files=None,
                 expect_port=445):
        self._stats = stats or {}
        self._walk = walk or {}
        self._files = files or {}
        self._expect_port = expect_port
        self.calls = []
        self.open_handles = []

    # -- connection contract ------------------------------------------

    # Every guard below raises explicitly instead of using `assert`, and
    # that is not a style choice. Under `python -O` the interpreter strips
    # assert statements, so an assert-based fake stops checking anything
    # while still reporting OK -- the strictness quietly evaporates in
    # exactly the mode this repository requires every suite to pass in.
    # Found by running -O: two tests errored because the fake had become
    # permissive, and the other twenty-six were passing against a fake
    # that was no longer verifying a single thing.

    def _fail(self, message):
        raise AssertionError(message)

    def _check_conn(self, where, kwargs):
        if set(kwargs) != {'username', 'password', 'port'}:
            self._fail(
                f'{where}: connection keywords were {sorted(kwargs)}; '
                f'task_core forwards exactly username, password and port'
            )
        if kwargs['username'] != CREDS['username']:
            self._fail(f'{where}: wrong username {kwargs["username"]!r}')
        if kwargs['password'] != CREDS['password']:
            self._fail(f'{where}: wrong password')
        if kwargs['port'] != self._expect_port:
            self._fail(
                f'{where}: port {kwargs["port"]!r}, expected '
                f'{self._expect_port!r}'
            )

    def _check_path(self, where, path):
        # smbclient's DFS-referral resolution does raw string slicing on
        # whatever it is handed and breaks on a Path -- a real production
        # crash, which is why the public entry points coerce with str().
        # Nothing else enforces that, so the fake does.
        if not isinstance(path, str):
            self._fail(
                f'{where}: got {type(path).__name__}, but a Path object '
                f'crashes smbclient on a genuine DFS referral'
            )

    # -- the three entry points task_core uses ------------------------

    def stat(self, path, **kwargs):
        self._check_path('stat', path)
        self._check_conn('stat', kwargs)
        self.calls.append(('stat', path))
        if path not in self._stats:
            raise SMBOSError(errno.ENOENT, 'No such file or directory', path)
        result = self._stats[path]
        if isinstance(result, Exception):
            raise result
        return result

    def walk(self, base, **kwargs):
        """os.walk semantics, including pruning via the yielded dirnames.

        This matters and was got wrong first time. The scanner turns
        recursion off with `dirnames[:] = []`, which only prunes if the
        walker re-reads that list *after* the yield -- exactly what
        os.walk does, and what smbclient.walk mirrors. A first version of
        this fake yielded a pre-built list, so pruning silently did
        nothing and every non-recursive scan appeared to descend. The
        live campaign had already shown the real scanner prunes
        correctly, so the fake was the thing that was wrong.
        """
        self._check_path('walk', base)
        self._check_conn('walk', kwargs)
        self.calls.append(('walk', base))
        if base not in self._walk:
            self._fail(f'walk({base!r}) was not configured')

        def generate(current):
            dirnames, filenames = self._walk[current]
            live = list(dirnames)
            yield current, live, list(filenames)
            for name in live:          # re-read: caller pruning applies
                yield from generate(current + '\\' + name)

        return generate(base)

    @contextmanager
    def open_file(self, path, mode='rb', **kwargs):
        self._check_path('open_file', path)
        self._check_conn('open_file', kwargs)
        if mode != 'rb':
            self._fail(f'open_file mode was {mode!r}, expected rb')
        self.calls.append(('open_file', path))
        if path not in self._files:
            self._fail(f'open_file({path!r}) was not configured')
        handle = io.BytesIO(self._files[path])
        self.open_handles.append(handle)
        try:
            yield handle
        finally:
            handle.close()

    def __getattr__(self, name):
        raise AssertionError(
            f'task_core called smbclient.{name}(), which this fake does '
            f'not model; add it deliberately rather than by accident'
        )


def access(client, *, port=None):
    creds = dict(CREDS)
    if port is not None:
        creds['port'] = port
    source = SourceAccess(dfs_creds=creds)
    source._get_smbclient = lambda: client
    return source


class Test1TheModeSwitchIsPathAware(unittest.TestCase):
    """`_use_smb` is credentials AND a UNC path, not credentials alone."""

    def test_credentials_alone_do_not_route_to_smb(self):
        client = StrictSmbClient()
        source = access(client)
        # A local path with credentials present must never reach smbclient;
        # if it did, the fake's __getattr__ or _check_path would fire.
        self.assertFalse(source._use_smb('D:\\local\\file.xlsx'))
        self.assertTrue(source._use_smb(SHARE + '\\a.xlsx'))
        self.assertEqual(client.calls, [])

    def test_a_unc_path_without_credentials_stays_local(self):
        source = SourceAccess(dfs_creds=None)
        self.assertFalse(source._use_smb(SHARE + '\\a.xlsx'))


class Test2ConnectionKeywordsAndPort(unittest.TestCase):
    """Credentials and port forwarding, which nothing asserted before."""

    def test_the_default_port_is_forwarded(self):
        path = SHARE + '\\a.txt'
        client = StrictSmbClient(stats={path: FakeStat(size=7)})
        info = access(client).select_fixed_file_info(path)
        self.assertEqual(info.stat_result.st_size, 7)

    def test_an_explicit_port_is_forwarded(self):
        path = SHARE + '\\a.txt'
        client = StrictSmbClient(stats={path: FakeStat()}, expect_port=4445)
        access(client, port=4445).select_fixed_file_info(path)

    def test_every_public_entry_point_coerces_a_path_to_str(self):
        """The documented DFS crash, guarded at all three doors.

        smbclient's referral resolution slices the path as a string and
        breaks on a Path object -- but only on a genuine DFS referral, so
        a plain-share test would never see it. The coercion therefore has
        to be asserted rather than trusted.

        Checked at the public entry points specifically. `_stat` coerces
        too, but every caller has already done so by then, so reverting
        that inner one changes nothing observable -- a first version of
        this test reverted it and passed, proving only that it was
        redundant. These three are where the guarantee actually lives.
        """
        path = SHARE + '\\a.txt'

        client = StrictSmbClient(stats={path: FakeStat()})
        access(client).select_fixed_file_info(Path(path))
        self.assertEqual(client.calls, [('stat', path)])

        scan = StrictSmbClient(
            stats={SHARE: FakeStat(directory=True), path: FakeStat()},
            walk={SHARE: ([], ['a.txt'])},
        )
        access(scan).select_file_infos(Path(SHARE), pattern='*.txt')
        self.assertIn(('walk', SHARE), scan.calls)

        opener = StrictSmbClient(files={path: b'bytes'})
        with access(opener).open_binary(Path(path)) as handle:
            self.assertEqual(handle.read(), b'bytes')


class Test3RemoteStatErrorTranslation(unittest.TestCase):
    """Only ENOENT becomes FileNotFoundError; everything else is itself.

    Both halves matter. The translation is what makes a missing remote
    file behave like a missing local one; the pass-through is what stops
    a permission or transport failure being reported as 'file not found',
    which would be the most misleading thing the scaffold could say.
    """

    def test_enoent_becomes_filenotfounderror(self):
        client = StrictSmbClient(stats={})
        with self.assertRaises(FileNotFoundError) as raised:
            access(client).select_fixed_file_info(SHARE + '\\missing.txt')
        self.assertIn('File not found:', str(raised.exception))
        self.assertIsInstance(raised.exception.__cause__, SMBOSError)

    def test_a_missing_folder_becomes_filenotfounderror(self):
        client = StrictSmbClient(stats={})
        with self.assertRaises(FileNotFoundError) as raised:
            access(client).select_file_infos(SHARE + '\\nope', pattern='*')
        self.assertIn('Path not found:', str(raised.exception))

    def test_a_non_enoent_failure_keeps_its_own_type(self):
        """Measured live: an over-long name answers with errno 0."""
        path = SHARE + '\\x.txt'
        original = SMBOSError(0, 'Unmapped NT status', path)
        client = StrictSmbClient(stats={path: original})

        with self.assertRaises(OSError) as raised:
            access(client).select_fixed_file_info(path)
        self.assertIs(
            raised.exception, original,
            'a non-ENOENT failure was replaced rather than propagated',
        )
        self.assertNotIsInstance(raised.exception, FileNotFoundError)

    def test_a_permission_failure_keeps_its_own_type(self):
        path = SHARE + '\\denied.txt'
        original = SMBOSError(errno.EACCES, 'Access denied', path)
        client = StrictSmbClient(stats={path: original})

        with self.assertRaises(OSError) as raised:
            access(client).select_fixed_file_info(path)
        self.assertIs(raised.exception, original)
        self.assertNotIsInstance(raised.exception, FileNotFoundError)

    def test_a_directory_given_as_a_file_is_refused(self):
        path = SHARE + '\\sub'
        client = StrictSmbClient(stats={path: FakeStat(directory=True)})
        with self.assertRaises(FileNotFoundError) as raised:
            access(client).select_fixed_file_info(path)
        self.assertIn('Path is not a file:', str(raised.exception))


class Test4TheRemoteScannerUsesWalk(unittest.TestCase):
    """`_select_smb_file_infos`, including every filter.

    The live campaign proved these against a real share. These prove the
    same behaviours offline, so a regression is caught in CI rather than
    only when someone has credentials and a working referral.
    """

    def _client(self, *, mtimes=None, attributes=None):
        names = ['a.txt', 'b.txt', 'other.dat', '~$temp.txt',
                 'hidden.txt', 'system.txt']
        mtimes = mtimes or {}
        attributes = attributes or {
            'hidden.txt': 0x02,
            'system.txt': 0x04,
        }
        stats = {SHARE: FakeStat(directory=True),
                 SHARE + '\\nested': FakeStat(directory=True)}
        for name in names:
            stats[SHARE + '\\' + name] = FakeStat(
                mtime=mtimes.get(name, 1_000_000),
                attributes=attributes.get(name, 0x20),
            )
        stats[SHARE + '\\nested\\deep.txt'] = FakeStat()
        return StrictSmbClient(
            stats=stats,
            walk={
                SHARE: (['nested'], names),
                SHARE + '\\nested': ([], ['deep.txt']),
            },
        )

    def _names(self, **kwargs):
        infos = access(self._client()).select_file_infos(
            SHARE, pattern='*.txt', **kwargs)
        return sorted(i.relative_path for i in infos)

    def test_the_default_scan_excludes_temp_hidden_system_and_nonmatching(self):
        self.assertEqual(self._names(), ['a.txt', 'b.txt'])

    def test_include_temp_admits_the_excel_lock_file(self):
        self.assertEqual(
            self._names(include_temp=True), ['a.txt', 'b.txt', '~$temp.txt'])

    def test_include_hidden_reads_the_attribute_bit(self):
        self.assertEqual(
            self._names(include_hidden=True),
            ['a.txt', 'b.txt', 'hidden.txt'])

    def test_include_system_reads_the_attribute_bit(self):
        self.assertEqual(
            self._names(include_system=True),
            ['a.txt', 'b.txt', 'system.txt'])

    def test_recursion_is_off_by_default_and_joins_with_a_backslash(self):
        self.assertNotIn('nested\\deep.txt', self._names())
        self.assertIn('nested\\deep.txt', self._names(recursive=True))

    def test_min_age_seconds_excludes_a_file_that_is_too_new(self):
        import time
        fresh = time.time() + 3600
        client = self._client(mtimes={'b.txt': fresh})
        infos = access(client).select_file_infos(
            SHARE, pattern='*.txt', min_age_seconds=60)
        self.assertEqual(sorted(i.relative_path for i in infos), ['a.txt'])

    def test_zero_matches_raises_nomatchingfileserror(self):
        with self.assertRaises(NoMatchingFilesError):
            access(self._client()).select_file_infos(SHARE, pattern='*.none')

    def test_a_file_given_as_a_folder_is_refused(self):
        path = SHARE + '\\a.txt'
        client = StrictSmbClient(stats={path: FakeStat()})
        with self.assertRaises(ValueError) as raised:
            access(client).select_file_infos(path, pattern='*')
        self.assertIn('not a directory', str(raised.exception))


class Test5RemoteLatestSelection(unittest.TestCase):
    """Latest selection and its tie-break, over the fake."""

    def _client(self, mtimes):
        stats = {SHARE: FakeStat(directory=True)}
        for name, mtime in mtimes.items():
            stats[SHARE + '\\' + name] = FakeStat(mtime=mtime)
        return StrictSmbClient(
            stats=stats,
            walk={SHARE: ([], list(mtimes))},
        )

    def test_the_newest_file_wins(self):
        client = self._client({'a.txt': 100, 'b.txt': 300, 'c.txt': 200})
        info = access(client).select_latest_file_info(SHARE, '*.txt')
        self.assertEqual(info.relative_path, 'b.txt')

    def test_equal_mtimes_are_broken_by_path(self):
        """The tie-break is load-bearing over SMB too.

        Without it the winner depends on the order the server happened to
        enumerate the directory, and a source-change check that flips
        between two files reports a change on every run.
        """
        client = self._client({'aaa.txt': 500, 'zzz.txt': 500})
        info = access(client).select_latest_file_info(SHARE, '*.txt')
        self.assertEqual(info.relative_path, 'zzz.txt')

        reversed_client = self._client({'zzz.txt': 500, 'aaa.txt': 500})
        self.assertEqual(
            access(reversed_client).select_latest_file_info(
                SHARE, '*.txt').relative_path,
            'zzz.txt',
            'latest selection depended on server enumeration order',
        )


class Test6RemoteBinaryOpening(unittest.TestCase):
    """`open_binary`, both modes, and the handle lifecycle."""

    PAYLOAD = b'remote bytes' * 100

    def _client(self):
        path = SHARE + '\\big.bin'
        return path, StrictSmbClient(files={path: self.PAYLOAD})

    def test_streaming_yields_the_remote_handle_itself(self):
        """Identity, not behaviour: streaming must hand back the wire.

        Asserted by object identity because a content check cannot tell
        the two modes apart -- both yield the same bytes, and a copy is
        as seekable as the original. Identity is the only thing that
        distinguishes them.
        """
        path, client = self._client()
        with access(client).open_binary(path) as handle:
            self.assertIs(handle, client.open_handles[0])
            self.assertEqual(handle.read(), self.PAYLOAD)
        self.assertTrue(
            client.open_handles[0].closed,
            'the remote handle outlived its context',
        )

    def test_buffered_yields_a_copy_and_not_the_remote_handle(self):
        path, client = self._client()
        with access(client).open_binary(path, buffered=True) as handle:
            self.assertIsNot(
                handle, client.open_handles[0],
                'buffered mode handed back the remote handle instead of a '
                'copy, so nothing was buffered',
            )
            first = handle.read()
            handle.seek(0)
            self.assertEqual(handle.read(), first)
        self.assertEqual(first, self.PAYLOAD)

    def test_buffered_still_holds_the_remote_handle_for_the_whole_body(self):
        """Pins what buffered mode actually does, which is less than it sounds.

        The network read completes up front -- the caller reads from a
        BytesIO, never from the wire -- but the remote handle is *not*
        released early: `with smbclient.open_file(...)` wraps both
        branches of open_binary, so it stays open until the caller's body
        finishes either way.

        Asserted deliberately rather than assumed. This test was first
        written the opposite way round, on the reasonable-sounding theory
        that copying the bytes exists in order to let the handle go, and
        the code says otherwise. Recorded here so the next person forms
        the belief from the test instead of from the name.

        Whether it *should* release early is a separate question with a
        real trade-off, and not one a test should decide.
        """
        path, client = self._client()
        with access(client).open_binary(path, buffered=True) as handle:
            self.assertFalse(
                client.open_handles[0].closed,
                'buffered mode released the remote handle early -- if this '
                'became true deliberately, the docs claiming otherwise need '
                'updating with it',
            )
            self.assertFalse(handle.closed)
        self.assertTrue(client.open_handles[0].closed)

    def test_the_remote_handle_closes_even_when_the_body_raises(self):
        path, client = self._client()
        with self.assertRaises(RuntimeError):
            with access(client).open_binary(path):
                raise RuntimeError('boom')
        self.assertTrue(client.open_handles[0].closed)


class Test7TheFakeItselfIsStrict(unittest.TestCase):
    """A permissive fake is worse than none: it certifies whatever it sees.

    These prove the fake refuses the three things it exists to catch, so
    the assertions above cannot pass by the fake being lenient.
    """

    def test_an_unmodelled_smbclient_call_fails(self):
        client = StrictSmbClient()
        with self.assertRaises(AssertionError) as raised:
            client.listdir(SHARE)
        self.assertIn('does not model', str(raised.exception))

    def test_a_path_object_is_rejected(self):
        client = StrictSmbClient(stats={})
        with self.assertRaises(AssertionError) as raised:
            client.stat(Path(SHARE), username=CREDS['username'],
                        password=CREDS['password'], port=445)
        self.assertIn('crashes smbclient', str(raised.exception))

    def test_missing_connection_keywords_are_rejected(self):
        client = StrictSmbClient(stats={})
        with self.assertRaises(AssertionError) as raised:
            client.stat(SHARE, username=CREDS['username'])
        self.assertIn('connection keywords', str(raised.exception))

    def test_the_modelled_error_is_not_a_filenotfounderror(self):
        """The property the whole errno fix turns on."""
        self.assertTrue(issubclass(SMBOSError, OSError))
        self.assertFalse(issubclass(SMBOSError, FileNotFoundError))


if __name__ == '__main__':
    unittest.main()
