# -*- coding: utf-8 -*-
"""
Tests against the real DbPublisher class (task_core/db_publish.py), not
a fake one -- this file exists specifically because the existing
FakeDbPublisher (tests/test_source_change_runner.py) called its fake
connection's commit() unconditionally, which meant it could never have
caught a genuine bug in DbPublisher's own commit()/rollback() logic. That
bug was real: source-check-only runs (source tracking enabled, but no
active pipeline has db_table) write the technical source-state row
through SourceStateStore, which goes straight through
publisher.ensure_connection() and never through publisher._ensure_
transaction() -- so DbPublisher._tx stayed None the entire time, and the
old commit()/rollback() (`if self._tx is None: return`) silently did
nothing. The write happened via SQLAlchemy's own autobegin (any execute()
implicitly opens a transaction), but nothing ever committed it, and
connection close() rolls back whatever's left open. The task returned
success; the fingerprint was never actually persisted; the next run would
see "unchanged" fail to match and rerun needlessly forever -- or worse,
silently never converge.

Found by external review, not here first. No real SQLAlchemy is available
in this sandbox (no network), so FakeSqlaConnection/FakeSqlaEngine below
model SQLAlchemy 2.0's real, documented autobegin behavior directly:
executing anything without an explicit begin() implicitly opens a
transaction; in_transaction() reports whether one (implicit or explicit)
is active; commit()/rollback() act on whatever's currently open; closing
a connection with an open, uncommitted transaction rolls it back. This is
not a simplification for convenience -- it's the specific behavior the
bug depended on, so the fake has to reproduce it precisely or it can't
tell a correct commit() from a broken one, the same way the original
FakeDbPublisher couldn't.
"""

import unittest

from task_core.db_publish import DbPublisher


class FakeSqlaTransaction:
    """Models sqlalchemy.engine.Transaction -- returned by conn.begin()
    for an *explicit* transaction, distinct from the implicit one
    autobegin opens."""

    def __init__(self, conn):
        self._conn = conn
        self._closed = False

    def commit(self):
        if self._closed:
            return
        self._conn._do_commit()
        self._closed = True

    def rollback(self):
        if self._closed:
            return
        self._conn._do_rollback()
        self._closed = True


class FakeSqlaConnection:
    """Models sqlalchemy.engine.Connection's real autobegin semantics."""

    def __init__(self, engine):
        self._engine = engine
        self._in_transaction = False
        self._pending = {}
        self.closed = False
        self.execute_log = []

    def execute(self, stmt, params=None):
        if not self._in_transaction:
            self._in_transaction = True  # autobegin -- the crux of the bug
        self.execute_log.append((str(stmt), params))
        self._engine._apply(stmt, params, self._pending)
        return self._engine._select(stmt, params, self._pending)

    def begin(self):
        self._in_transaction = True
        return FakeSqlaTransaction(self)

    def in_transaction(self):
        return self._in_transaction

    def commit(self):
        self._do_commit()

    def rollback(self):
        self._do_rollback()

    def _do_commit(self):
        self._engine.committed_rows = dict(self._pending)
        self._pending = {}
        self._in_transaction = False

    def _do_rollback(self):
        self._pending = {}
        self._in_transaction = False

    def close(self):
        if self._in_transaction:
            self._do_rollback()  # real SQLAlchemy behavior, not invented for this test
        self.closed = True


class FakeSqlaEngine:
    """Models sqlalchemy.engine.Engine -- just enough of it (.connect())
    for DbPublisher.ensure_connection() to work against directly."""

    def __init__(self):
        self.committed_rows = {}

    def connect(self):
        return FakeSqlaConnection(self)

    def dispose(self):
        pass

    def _apply(self, stmt, params, pending):
        sql = str(stmt).lower().strip()
        if sql.startswith('insert'):
            pending[(params['task_name'], params['source_key'])] = params['source_signature']
        elif sql.startswith('delete'):
            task_name = params['task_name']
            for key in list(pending):
                if key[0] == task_name:
                    del pending[key]

    def _select(self, stmt, params, pending):
        outer = self

        class _Result:
            def mappings(self_inner):
                view = dict(outer.committed_rows)
                view.update(pending)
                task_name = params.get('task_name') if params else None
                return [
                    {'source_key': k[1], 'source_signature': v}
                    for k, v in view.items() if k[0] == task_name
                ]
        return _Result()


def make_publisher_with_fake_engine():
    """A real DbPublisher, with its lazy engine construction bypassed by
    directly injecting a fake one -- DbPublisher has no constructor seam
    for this, so this is the same pattern used to discover the bug in
    the first place: set ._engine directly before ensure_connection() is
    ever called, so its own `if self._engine is None` lazy-init just uses
    the fake."""
    publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr')
    engine = FakeSqlaEngine()
    publisher._engine = engine
    return publisher, engine


class Test1SourceCheckOnlyStateGenuinelyCommits(unittest.TestCase):
    def test_implicit_autobegin_write_is_actually_committed(self):
        publisher, engine = make_publisher_with_fake_engine()
        conn = publisher.ensure_connection()

        # Simulates exactly what SourceStateStore.upsert_state() does:
        # execute() directly via the connection, never through
        # publisher._ensure_transaction() -- this IS the source-check-
        # only code path, not a simplification of it.
        conn.execute(
            "insert into bsr.task_scaffold_meta (...) values (...)",
            {'task_name': 't', 'source_key': 'files', 'source_signature': 'sig-v2'},
        )
        self.assertTrue(conn.in_transaction(), 'fixture problem: autobegin did not open a transaction')

        result = publisher.commit()

        self.assertTrue(publisher.committed)
        self.assertFalse(conn.in_transaction(), 'commit() did not actually close out the implicit transaction')
        self.assertEqual(
            engine.committed_rows, {('t', 'files'): 'sig-v2'},
            'the source-state write was never actually committed to the durable store',
        )
        # committed_tables stays empty regardless -- that's specifically
        # about DB *payload* publishing (publish()), not source state;
        # this scenario never calls publish() at all.
        self.assertEqual(result, [])

    def test_write_survives_publisher_close(self):
        # The most direct proof of the original bug: the old code left
        # the implicit transaction open, and SQLAlchemy rolls back an
        # open transaction on connection close -- so even if a test
        # missed checking commit()'s return value, the write vanishing
        # after close() is the actual, real-world consequence.
        publisher, engine = make_publisher_with_fake_engine()
        conn = publisher.ensure_connection()
        conn.execute(
            "insert into bsr.task_scaffold_meta (...) values (...)",
            {'task_name': 't', 'source_key': 'files', 'source_signature': 'sig-v2'},
        )
        publisher.commit()
        publisher.close()

        self.assertEqual(engine.committed_rows, {('t', 'files'): 'sig-v2'})


class Test2ImplicitTransactionRollback(unittest.TestCase):
    def test_rollback_discards_the_implicit_write(self):
        publisher, engine = make_publisher_with_fake_engine()
        engine.committed_rows = {('t', 'files'): 'sig-v1'}  # a prior, real committed run

        conn = publisher.ensure_connection()
        conn.execute(
            "insert into bsr.task_scaffold_meta (...) values (...)",
            {'task_name': 't', 'source_key': 'files', 'source_signature': 'sig-v2'},
        )
        self.assertTrue(conn.in_transaction())

        publisher.rollback()

        self.assertFalse(publisher.committed)
        self.assertFalse(conn.in_transaction(), 'rollback() did not close out the implicit transaction')
        self.assertEqual(
            engine.committed_rows, {('t', 'files'): 'sig-v1'},
            'rollback() should have discarded the staged write, leaving prior committed state untouched',
        )

    def test_explicit_transaction_from_publish_still_works_correctly(self):
        # commit()/rollback() must still handle the *explicit* _tx path
        # (opened by publish(), via _ensure_transaction()) exactly as
        # before -- the fix adds a new branch, it must not have broken
        # the original one.
        publisher, engine = make_publisher_with_fake_engine()
        conn = publisher._ensure_transaction()
        self.assertIsNotNone(publisher._tx, 'fixture problem: explicit transaction was not started')

        conn.execute(
            "insert into bsr.some_table (...) values (...)",
            {'task_name': 't', 'source_key': 'files', 'source_signature': 'sig-v2'},
        )
        publisher.commit()

        self.assertTrue(publisher.committed)
        self.assertIsNone(publisher._tx, 'explicit _tx was not cleared after commit')


class Test3DuplicateColumnsRejected(unittest.TestCase):
    """Found by external review: both adapters built row dicts from
    (columns, row) pairs without first checking columns were unique --
    a duplicate silently collapsed to whichever value came last in the
    dict, while `columns` still listed it twice, an internally
    inconsistent DbPayload that would only fail much later, inside
    SQLAlchemy's CREATE TABLE with two same-named columns."""

    def test_from_pandas_rejects_literal_duplicate_names(self):
        import pandas as pd
        from task_core.db_publish import from_pandas, DbPublishError

        df = pd.DataFrame([[1, 2]], columns=['a', 'a'])
        with self.assertRaises(DbPublishError):
            from_pandas(df, table_name='t', schema='s')

    def test_from_pandas_rejects_stringify_collision(self):
        # Distinct original labels (int 1, str '1') that stringify to
        # the same output column name -- columns is already the
        # stringified list by validation time, so this must be caught
        # identically to a literal duplicate.
        import pandas as pd
        from task_core.db_publish import from_pandas, DbPublishError

        df = pd.DataFrame([[1, 2]])
        df.columns = [1, '1']
        with self.assertRaises(DbPublishError):
            from_pandas(df, table_name='t', schema='s')

    def test_from_petl_rejects_duplicate_names(self):
        import petl as etl
        from task_core.db_publish import from_petl, DbPublishError

        tbl = etl.wrap([('a', 'a'), (1, 2)])
        with self.assertRaises(DbPublishError):
            from_petl(tbl, table_name='t', schema='s')

    def test_unique_columns_still_work_normally(self):
        import pandas as pd
        from task_core.db_publish import from_pandas

        df = pd.DataFrame([[1, 2]], columns=['a', 'b'])
        payload = from_pandas(df, table_name='t', schema='s')
        self.assertEqual(payload.columns, ['a', 'b'])
        self.assertEqual(payload.rows, [{'a': 1, 'b': 2}])


class Test4ChunkSizeValidation(unittest.TestCase):
    """Found by external review: chunk_size=0 crashes later, deep inside
    _chunked's range() call. chunk_size=-1 is worse -- it silently
    produces zero chunks, meaning zero rows get inserted while the table
    itself still gets created, so publication appears successful."""

    def test_zero_is_rejected(self):
        from task_core.db_publish import DbPublishError
        with self.assertRaises(DbPublishError):
            DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', chunk_size=0)

    def test_negative_is_rejected(self):
        from task_core.db_publish import DbPublishError
        with self.assertRaises(DbPublishError):
            DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', chunk_size=-1)

    def test_positive_still_works(self):
        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', chunk_size=100)
        self.assertEqual(publisher.chunk_size, 100)


class Test5CloseIsolatesItsOwnCleanupSteps(unittest.TestCase):
    """Found by external review, confirmed directly before fixing:
    close() called self._conn.close() then self._engine.dispose() as two
    unguarded, sequential statements -- if the connection failed to
    close, engine.dispose() was never even attempted, and since the
    runner has already made its one cleanup attempt at the publisher
    level, there was no retry. A genuine, permanent leak of the engine's
    connection pool."""

    def test_engine_dispose_still_attempted_when_connection_close_fails(self):
        class FailingConn:
            def close(self):
                raise OSError('connection close failed')

        class TrackedEngine:
            def __init__(self):
                self.dispose_calls = 0
            def dispose(self):
                self.dispose_calls += 1

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr')
        engine = TrackedEngine()
        publisher._engine = engine
        publisher._conn = FailingConn()

        with self.assertRaises(OSError):
            publisher.close()

        self.assertEqual(engine.dispose_calls, 1, 'engine.dispose() was never attempted')
        self.assertIsNone(publisher._conn, 'not cleared despite the close failure')
        self.assertIsNone(publisher._engine)

    def test_connection_close_still_attempted_when_engine_dispose_fails(self):
        class TrackedConn:
            def __init__(self):
                self.close_calls = 0
            def close(self):
                self.close_calls += 1

        class FailingEngine:
            def dispose(self):
                raise RuntimeError('engine dispose failed')

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr')
        conn = TrackedConn()
        publisher._conn = conn
        publisher._engine = FailingEngine()

        with self.assertRaises(RuntimeError):
            publisher.close()

        self.assertEqual(conn.close_calls, 1, 'connection.close() was never attempted')

    def test_both_failing_surface_together(self):
        class FailingConn:
            def close(self):
                raise OSError('connection close failed')

        class FailingEngine:
            def dispose(self):
                raise RuntimeError('engine dispose failed')

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr')
        publisher._conn = FailingConn()
        publisher._engine = FailingEngine()

        with self.assertRaises(ExceptionGroup) as cm:
            publisher.close()
        self.assertEqual(len(cm.exception.exceptions), 2)

    def test_normal_close_still_works(self):
        class GoodConn:
            def close(self):
                pass
        class GoodEngine:
            def dispose(self):
                pass

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr')
        publisher._conn = GoodConn()
        publisher._engine = GoodEngine()
        publisher.close()  # must not raise

    def test_close_with_nothing_ever_connected_does_not_raise(self):
        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr')
        publisher.close()


if __name__ == '__main__':
    unittest.main()
