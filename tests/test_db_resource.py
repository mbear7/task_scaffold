# -*- coding: utf-8 -*-
"""
resources/db.py had no persistent test coverage at all before this file
-- confirmed directly (grep found nothing) before writing it.

FakeCursor implements the complete DB-API 2.0 cursor surface petl's own
duck-typing actually requires (execute, executemany, fetchone,
fetchmany, fetchall) -- confirmed directly a partial fake (missing any
one of these) is rejected outright by petl.errors.ArgumentError, not
silently accepted.
"""

import unittest

import petl as etl

from task_core.resources.db import db_resource


class FakeCursor:
    def __init__(self, name=None):
        self.name = name
        self.itersize = None
        self.description = [('a',), ('b',)]
        self._rows = None
        self.executed_query = None
        self.itersize_at_execute_time = None
        self.closed = False

    def execute(self, query, *args, **kwargs):
        self.executed_query = query
        # What actually matters: itersize as it stood the moment
        # execute() ran, not merely that it was assigned at some point.
        self.itersize_at_execute_time = self.itersize
        self._rows = [(1, 'x'), (2, 'y')]

    def executemany(self, *a, **k):
        pass

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size=None):
        return []

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self):
        self.named_cursor_calls = []
        self.plain_cursor_calls = 0
        self.all_created_cursors = []
        self._last_cursor = None

    def cursor(self, name=None, *args, **kwargs):
        if name is not None:
            self.named_cursor_calls.append(name)
        else:
            self.plain_cursor_calls += 1
        c = FakeCursor(name=name)
        self.all_created_cursors.append(c)
        self._last_cursor = c
        return c

    def close(self):
        pass


def _build_resource():
    resource = db_resource.__new__(db_resource)
    resource.creds = {}
    resource._conn = FakeConn()
    resource._table_cache = {}
    return resource


class Test1ServerSideCursorOptIn(unittest.TestCase):
    """Found on the original optimization proposal's "smaller
    improvements" list: db_resource had no way to request a server-side
    (named) PostgreSQL cursor for a large read -- the plain
    etl.fromdb(conn, sql) path buffers the whole result client-side.
    Opt-in, defaulting to the pre-existing behavior unchanged: this
    changes real client/server memory behavior for a large read, not
    something to silently switch every caller into."""

    def test_default_path_is_completely_unaffected(self):
        resource = _build_resource()
        tbl = resource.get_table(table='plain_table')
        rows = list(etl.data(tbl))

        self.assertEqual(resource._conn.named_cursor_calls, [])
        self.assertGreaterEqual(
            resource._conn.plain_cursor_calls, 1,
            'the default path should still use an ordinary, unnamed cursor',
        )
        self.assertEqual(rows, [(1, 'x'), (2, 'y')])

    def test_server_side_cursor_uses_a_named_cursor(self):
        resource = _build_resource()
        tbl = resource.get_table(table='big_table', server_side_cursor=True)
        list(etl.data(tbl))  # a lazy DbView -- must actually traverse to trigger execute()

        self.assertGreaterEqual(len(resource._conn.named_cursor_calls), 1)
        self.assertEqual(resource._conn.plain_cursor_calls, 0)

    def test_itersize_is_genuinely_set_before_execute_runs(self):
        resource = _build_resource()
        tbl = resource.get_table(table='big_table', server_side_cursor=True, itersize=5000)
        list(etl.data(tbl))

        self.assertEqual(resource._conn._last_cursor.itersize_at_execute_time, 5000)

    def test_default_itersize_applied_when_not_specified(self):
        resource = _build_resource()
        tbl = resource.get_table(table='big_table', server_side_cursor=True)
        list(etl.data(tbl))

        self.assertEqual(resource._conn._last_cursor.itersize_at_execute_time, 2000)

    def test_cursor_names_are_unique_across_multiple_calls(self):
        resource = _build_resource()
        list(etl.data(resource.get_table(table='table_a', server_side_cursor=True)))
        list(etl.data(resource.get_table(table='table_b', server_side_cursor=True)))

        names = resource._conn.named_cursor_calls
        self.assertGreaterEqual(len(names), 2)
        self.assertEqual(len(set(names)), len(names), 'cursor names collided -- a named cursor must be unique per transaction')

    def test_cache_key_distinguishes_server_side_from_plain_for_the_same_table(self):
        resource = _build_resource()
        resource.get_table(table='t')
        resource.get_table(table='t', server_side_cursor=True)

        self.assertEqual(
            len(resource._table_cache), 2,
            'server_side_cursor=True and False for the same table should not share a cache entry',
        )

    def test_query_form_still_works_with_server_side_cursor_too(self):
        resource = _build_resource()
        tbl = resource.get_table(query='select * from something where x > 1', server_side_cursor=True)
        list(etl.data(tbl))

        self.assertEqual(resource._conn._last_cursor.executed_query, 'select * from something where x > 1')


class Test2ServerSideCursorUsesPetlsCallableForm(unittest.TestCase):
    """Found by a further review: the original server-side implementation
    passed a single, already-created named cursor directly to
    etl.fromdb(). A named cursor can only be iterated once, so the
    resulting DbView held exactly one, one-shot cursor for its whole
    lifetime -- any repeated traversal of that same DbView (including two
    different callers landing on the same cache entry) either silently
    got an empty result the second time, or would be sharing the exact
    same cursor object. Rewritten to pass a callable instead -- confirmed
    directly, reading petl's own source (petl/io/db.py), that this hits a
    distinct dispatch branch that calls the callable fresh on every
    traversal to get a brand new cursor each time, and explicitly closes
    it afterward."""

    def test_no_petl_warning_fires_for_the_callable_form(self):
        # petl logs its own "using a DB-API cursor with fromdb() is not
        # recommended" warning specifically for the direct-cursor
        # dispatch branch, via logger.warning() (petl/io/db.py's own
        # `logger = logging.getLogger(__name__)`) -- confirmed directly
        # this is logging, not the warnings module, so
        # warnings.catch_warnings() never actually intercepts it at all,
        # regardless of which implementation runs. assertNoLogs against
        # petl's own, exact logger name is what genuinely tests this.
        resource = _build_resource()
        with self.assertNoLogs('petl.io.db', level='WARNING'):
            tbl = resource.get_table(table='t', server_side_cursor=True)
            list(etl.data(tbl))

    def test_repeated_traversal_of_the_same_dbview_genuinely_works(self):
        # The actual, real-world consequence of the bug this fixes,
        # confirmed directly -- not just the mechanism in isolation.
        # Previously: the second traversal of the same returned table
        # silently got an empty result, since the one, shared cursor's
        # result set was already exhausted by the first.
        resource = _build_resource()
        tbl = resource.get_table(table='t', server_side_cursor=True)

        first = list(etl.data(tbl))
        second = list(etl.data(tbl))

        self.assertEqual(first, [(1, 'x'), (2, 'y')])
        self.assertEqual(
            second, first,
            'a second traversal of the same DbView returned something different -- the cursor was silently exhausted or reused',
        )

    def test_a_fresh_cursor_is_created_for_each_traversal(self):
        resource = _build_resource()
        tbl = resource.get_table(table='t', server_side_cursor=True)

        list(etl.data(tbl))
        cursors_after_first = list(resource._conn.all_created_cursors)
        list(etl.data(tbl))
        cursors_after_second = resource._conn.all_created_cursors

        self.assertGreater(
            len(cursors_after_second), len(cursors_after_first),
            'a second traversal should create at least one new cursor, not reuse the first',
        )

    def test_every_created_cursor_gets_closed(self):
        resource = _build_resource()
        tbl = resource.get_table(table='t', server_side_cursor=True)
        list(etl.data(tbl))

        self.assertTrue(resource._conn.all_created_cursors, 'no cursor was ever created')
        for c in resource._conn.all_created_cursors:
            self.assertTrue(c.closed, f'cursor {c.name!r} was never closed')

    def test_itersize_is_now_part_of_the_cache_key_for_server_side_cursor(self):
        # Found by a further review: previously excluded entirely, on
        # the reasoning that itersize only tunes how rows get fetched,
        # never which rows -- true for the plain connection path, where
        # every traversal gets its own fresh cursor regardless of this
        # resource's own cache. Not true here: a caller explicitly
        # requesting a different itersize for a server-side cursor is a
        # deliberate, explicit performance request, and silently
        # overriding it because some other caller requested the same
        # query first is a genuine surprise, not a harmless cache hit.
        resource = _build_resource()
        resource.get_table(table='t', server_side_cursor=True, itersize=1000)
        resource.get_table(table='t', server_side_cursor=True, itersize=9999)

        self.assertEqual(
            len(resource._table_cache), 2,
            'two different, explicitly-requested itersize values for a server-side cursor silently shared one cache entry',
        )

    def test_itersize_still_irrelevant_to_the_cache_key_for_the_plain_path(self):
        # The original reasoning still correctly holds for the case it
        # was actually about: itersize is never even used on the plain,
        # non-server-side path, so it shouldn't affect caching there.
        resource = _build_resource()
        resource.get_table(table='t', server_side_cursor=False, itersize=1000)
        resource.get_table(table='t', server_side_cursor=False, itersize=9999)

        self.assertEqual(len(resource._table_cache), 1)


class Test3CloseIsExceptionSafeAndClearsTheTableCache(unittest.TestCase):
    """close() used to be two unguarded statements:

        if self._conn is not None:
            self._conn.close()
            self._conn = None

    Two defects, both confirmed directly before fixing. A raising
    conn.close() left _conn still set, so the resource reported itself
    open and a second close() retried the same failing connection --
    unlike excel_resource.close(), which already cleared its own state in
    a finally: under exactly this failure. And _table_cache was never
    cleared on ANY path, success included.

    That cache is not inert. petl's DbView holds the connection it was
    built from on its own .dbo attribute -- confirmed directly, by reading
    the attribute off a cached table after close() and finding the closed
    connection object still there. So a later get_table() with the same
    key returned a table bound to a dead connection, which fails at
    traversal time rather than at the call that caused it, and kept the
    connection reachable after the resource claimed to be closed.
    """

    def test_close_clears_the_table_cache_on_success(self):
        resource = _build_resource()
        resource.get_table(table='some_table')
        self.assertEqual(len(resource._table_cache), 1)

        resource.close()

        self.assertIsNone(resource._conn)
        self.assertEqual(resource._table_cache, {})

    def test_a_cached_dbview_genuinely_holds_the_connection(self):
        # The premise for clearing the cache at all. If petl ever stops
        # holding the connection this way, the reasoning above changes and
        # this test should be the thing that says so.
        resource = _build_resource()
        conn = resource._conn
        tbl = resource.get_table(table='some_table')
        self.assertIs(tbl.dbo, conn)

    def test_a_failing_close_still_clears_state_and_still_raises(self):
        class FailingConn(FakeConn):
            def close(self):
                raise RuntimeError('connection close failed')

        resource = _build_resource()
        resource._conn = FailingConn()
        resource.get_table(table='some_table')

        with self.assertRaises(RuntimeError):
            resource.close()

        # The failure must surface -- task_context.close() routes resources
        # through attempt_all_cleanup(), which needs a real exception so
        # run_pipelines() can decide to log or raise it. But the resource
        # must not be left half-closed either.
        self.assertIsNone(resource._conn)
        self.assertEqual(resource._table_cache, {})

    def test_a_second_close_after_a_failure_is_a_no_op(self):
        closes = []

        class FailingConn(FakeConn):
            def close(self):
                closes.append(1)
                raise RuntimeError('connection close failed')

        resource = _build_resource()
        resource._conn = FailingConn()

        with self.assertRaises(RuntimeError):
            resource.close()
        resource.close()   # must not retry the same failing connection

        self.assertEqual(len(closes), 1)

    def test_get_table_after_close_builds_a_fresh_view_not_the_stale_one(self):
        resource = _build_resource()
        stale = resource.get_table(table='some_table')
        old_conn = resource._conn

        resource.close()

        resource._conn = FakeConn()   # reopened, as _ensure_conn() would
        fresh = resource.get_table(table='some_table')

        self.assertIsNot(fresh, stale)
        self.assertIs(fresh.dbo, resource._conn)
        self.assertIsNot(fresh.dbo, old_conn)



if __name__ == '__main__':
    unittest.main()
