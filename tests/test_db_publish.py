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
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
from sqlalchemy import exc as sa_exc

import task_core as tc
from task_core.db_publish import (
    MAX_IDENTIFIER_BYTES,
    STAGING_NAME_KIND,
    DbPublishError,
    DbPublishInvariantError,
    DbPublisher,
    new_run_token,
    staging_table_name,
    staging_target_token,
    validate_identifier,
    validate_portable_identifier,
    server_identifier_limit,
    DbPayload,
    from_pandas,
    is_missing,
    _infer_column_type,
    _normalize_value,
)


_CREDS = {'user': 'x', 'host': 'x', 'dbname': 'x'}


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
        if self._in_transaction:
            # Real SQLAlchemy 2.0 REJECTS this, it does not quietly hand back
            # a second Transaction -- confirmed directly against the genuine
            # package (2.0.43) driving a real SQLite engine:
            #
            #   InvalidRequestError: This connection has already initialized
            #   a SQLAlchemy Transaction() object via begin() or autobegin;
            #   can't call begin() here unless rollback() or commit() is
            #   called first.
            #
            # This fake previously just set the flag and returned, which made
            # it strictly MORE PERMISSIVE than the library it stands in for --
            # and that gap was load-bearing, not cosmetic: with it,
            # run_pipelines()'s publisher.discard_pending_read() call could be
            # deleted outright and all 217 tests still passed, while the real
            # library raises on the first publish() of any source-check-enabled
            # run that also has DB outputs. Confirmed directly both ways before
            # changing this.
            raise sa_exc.InvalidRequestError(
                'This connection has already initialized a SQLAlchemy '
                'Transaction() object via begin() or autobegin; '
                "can't call begin() here unless rollback() or commit() "
                'is called first.'
            )
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


class Test6IsMissingHandlesPdNaCorrectly(unittest.TestCase):
    """Found during an optimization review: _normalize_value() (and,
    independently, table_adapters.py's normalize_for_excel()) used a
    bare `value != value` check to detect a missing value. That's
    correct for a plain NaN, but pd.NA specifically breaks it outright
    -- pd.NA != pd.NA doesn't return True, it raises TypeError
    ("boolean value of NA is ambiguous"), confirmed directly. The
    surrounding except Exception: pass silently swallowed that,
    meaning a raw pd.NA fell through unconverted instead of becoming
    None -- a real, live gap, not just a stale comment; confirmed
    directly against both functions in isolation before this fix,
    since a prior "verification" of _normalize_value only ever tested
    it through from_pandas()'s full path, which has an earlier
    conversion step that already turns pd.NA into None before
    _normalize_value ever sees it."""

    def test_is_missing_recognizes_every_missing_representation(self):
        self.assertTrue(is_missing(pd.NA))
        self.assertTrue(is_missing(float('nan')))
        self.assertTrue(is_missing(None))
        self.assertTrue(is_missing(pd.NaT))

    def test_is_missing_does_not_flag_ordinary_values(self):
        self.assertFalse(is_missing(5))
        self.assertFalse(is_missing(0))
        self.assertFalse(is_missing(''))
        self.assertFalse(is_missing('text'))
        self.assertFalse(is_missing(datetime.now()))

    def test_is_missing_safely_handles_array_like_values(self):
        # pd.isna() itself returns an array, not a scalar, for these --
        # confirmed directly this must not raise, and must not
        # incorrectly report a real, non-missing value as missing.
        self.assertFalse(is_missing([1, 2, 3]))
        self.assertFalse(is_missing([5]))
        self.assertFalse(is_missing([]))
        self.assertFalse(is_missing({'a': 1}))

    def test_is_missing_does_not_corrupt_one_element_containers_holding_a_missing_value(self):
        # Found by a further review: distinct from the array test
        # above, which only ever tried [5] -- a non-missing value.
        # bool() on a MULTI-element array raises (caught by the
        # except Exception: pass fallback, confirmed by the test
        # above), but bool() on a SINGLE-element array succeeds rather
        # than raising, so a genuine, non-missing container holding one
        # missing value slipped past that fallback entirely and got
        # silently, incorrectly treated as itself being the missing
        # marker. A container is never itself "the missing marker"
        # regardless of its own size -- only ever something that might
        # hold missing values inside it, a separate question this
        # function was never meant to answer.
        import numpy as np
        self.assertFalse(is_missing([None]))
        self.assertFalse(is_missing([float('nan')]))
        self.assertFalse(is_missing(np.array([np.nan])))
        self.assertFalse(is_missing(pd.Index([None])))
        self.assertFalse(is_missing([pd.NA]))

        # And the actual, real-world consequence this caused --
        # confirmed directly, not just the predicate in isolation.
        self.assertEqual(_normalize_value([None]), [None])
        from task_core.table_adapters import normalize_for_excel
        self.assertEqual(normalize_for_excel([None]), [None])

    def test_normalize_value_converts_a_raw_pd_na_to_none(self):
        # Directly, in isolation -- not through from_pandas()'s own
        # prior conversion step, which would mask this exact gap.
        result = _normalize_value(pd.NA)
        self.assertIsNone(result)

    def test_from_pandas_with_a_nullable_int64_column_still_works(self):
        # Regression check: from_pandas() already handled this
        # correctly via its own prior .astype(object).where(...) step,
        # before _normalize_value ever saw the value -- confirm this
        # fix doesn't change that already-correct path.
        df = pd.DataFrame({'a': ['x', 'y'], 'rank': pd.array([1, None], dtype='Int64')})
        payload = from_pandas(df, table_name='t', schema='s')
        self.assertEqual(payload.rows, [{'a': 'x', 'rank': 1}, {'a': 'y', 'rank': None}])


class Test7NormalizeValueDoesNotCollapseNonScalarContainers(unittest.TestCase):
    """Found by a further review: is_missing() itself was already
    correct (Test6 above), but _normalize_value() has its own, separate
    duck-typed conversion logic after the missing-value check --
    to_pydatetime()/.item() -- that still silently collapsed a
    one-element container down to its sole element, via a completely
    different mechanism than is_missing()'s own pd.isna()/is_scalar()
    check. numpy's own .item() is genuinely designed to do exactly that
    for an array of size 1, confirmed directly: np.array([5]).item()
    succeeds and returns the plain int 5, silently discarding the array
    itself. Fixed with the same pd.api.types.is_scalar() idea, applied
    one level earlier -- stopping the whole conversion block, not just
    is_missing()'s own check, for anything that isn't genuinely a
    scalar in the first place."""

    def test_containers_are_preserved_intact(self):
        import numpy as np
        self.assertEqual(list(_normalize_value(np.array([5]))), [5])
        self.assertTrue(pd.isna(_normalize_value(np.array([np.nan]))).all())
        self.assertEqual(list(_normalize_value(pd.Index([None]))), [None])
        self.assertEqual(
            list(_normalize_value(pd.DatetimeIndex(['2020-01-01']))),
            list(pd.DatetimeIndex(['2020-01-01'])),
        )
        self.assertEqual(list(_normalize_value(pd.Series([5]))), [5])

    def test_genuine_scalars_still_normalize_correctly(self):
        # The fix must not be so broad it stops genuine scalar
        # conversion -- confirmed directly these still work.
        import numpy as np
        result = _normalize_value(pd.Timestamp('2020-01-01'))
        self.assertEqual(result, datetime(2020, 1, 1))
        self.assertNotIsInstance(result, pd.Timestamp)

        result2 = _normalize_value(np.int64(5))
        self.assertEqual(result2, 5)
        self.assertIsInstance(result2, int)
        self.assertNotIsInstance(result2, np.integer)

    def test_missing_values_still_correctly_become_none(self):
        # Regression check: this fix sits right after is_missing()'s own
        # check, must not interfere with it.
        self.assertIsNone(_normalize_value(pd.NA))
        self.assertIsNone(_normalize_value(float('nan')))
        self.assertIsNone(_normalize_value(None))


class Test8SampledTypeInferenceIsVerifiedAgainstUnsampledRows(unittest.TestCase):
    """Type inference samples the first `type_infer_sample_size` rows
    (default 5000). A column whose sampled prefix is narrower than its
    real data therefore produced a narrower column type than the data
    needs -- confirmed directly: 5000 int rows followed by a single 3.5
    inferred BigInteger, not Numeric.

    For most narrowings that is merely a loud failure at insert time
    ('N/A' into bigint errors). For exactly two of them it is silent data
    corruption, confirmed directly against a real PostgreSQL instance by
    the project owner rather than assumed from documentation:

        insert into (v bigint) values (3.5)  -> stores 4, no error
        insert into (v date) values (timestamp '2024-01-01 13:30')
                                             -> stores 2024-01-01, time dropped

    Both are data-dependent, surface only once a table grows past the
    sample, and leave the task reporting success with a correct row count.

    The fix keeps the sample as the inference window and adds a
    verification pass over the remaining rows for those two types only.
    These tests hold it to producing the identical answer a full scan
    would, which is the property that actually matters -- not merely that
    it differs from the old, sampled-only behavior.
    """

    SAMPLE = 10  # small sample so these tests stay fast; the mechanism is
                 # identical at the real default of 5000

    def _both(self, rows):
        """(verified answer, full-scan answer) for the same rows."""
        verified = _infer_column_type(rows, 'v', sample_size=self.SAMPLE)
        full = _infer_column_type(rows, 'v', sample_size=None)
        return type(verified).__name__, type(full).__name__

    def test_int_column_with_a_float_beyond_the_sample_becomes_numeric(self):
        # The exact silent-rounding case: without verification this stays
        # BigInteger and PostgreSQL rounds 3.5 to 4 on insert.
        rows = [{'v': i} for i in range(self.SAMPLE)] + [{'v': 3.5}]
        verified, full = self._both(rows)
        self.assertEqual(verified, 'Numeric')
        self.assertEqual(verified, full)

    def test_int_column_with_a_decimal_beyond_the_sample_becomes_numeric(self):
        # Decimal is the other member of the 'numeric' family and reaches
        # the same rounding cast; covered separately so a fix that only
        # special-cased float wouldn't pass.
        rows = [{'v': i} for i in range(self.SAMPLE)] + [{'v': Decimal('2.5')}]
        verified, full = self._both(rows)
        self.assertEqual(verified, 'Numeric')
        self.assertEqual(verified, full)

    def test_date_column_with_a_datetime_beyond_the_sample_becomes_datetime(self):
        # The silent time-truncation case. datetime is a subclass of date,
        # which is exactly why the verification uses `type(v) is date`
        # rather than isinstance() -- isinstance would accept the datetime
        # as consistent with a Date column and let the truncation through.
        rows = [{'v': date(2024, 1, 1)} for _ in range(self.SAMPLE)]
        rows.append({'v': datetime(2024, 1, 1, 13, 30)})
        verified, full = self._both(rows)
        self.assertEqual(verified, 'DateTime')
        self.assertEqual(verified, full)

    def test_int_column_with_text_beyond_the_sample_becomes_text(self):
        # Would have failed loudly at insert time rather than corrupting
        # anything, but the verification pass must still resolve it the
        # way a full scan does -- widening to Numeric here would be wrong.
        rows = [{'v': i} for i in range(self.SAMPLE)] + [{'v': 'N/A'}]
        verified, full = self._both(rows)
        self.assertEqual(verified, 'Text')
        self.assertEqual(verified, full)

    def test_int_column_with_a_bool_beyond_the_sample_becomes_text(self):
        # bool is a subclass of int, so an isinstance()-based verification
        # would treat True as consistent with BigInteger and never
        # re-infer. A full scan resolves {'int', 'bool'} to Text.
        rows = [{'v': i} for i in range(self.SAMPLE)] + [{'v': True}]
        verified, full = self._both(rows)
        self.assertEqual(verified, 'Text')
        self.assertEqual(verified, full)

    def test_clean_columns_are_unchanged_and_keep_their_narrow_type(self):
        # The common case: verification must not widen anything on its own.
        # If it did, every int column in the project would silently become
        # Numeric, which is a worse regression than the bug being fixed.
        int_rows = [{'v': i} for i in range(self.SAMPLE * 3)]
        self.assertEqual(self._both(int_rows), ('BigInteger', 'BigInteger'))

        date_rows = [{'v': date(2024, 1, 1)} for _ in range(self.SAMPLE * 3)]
        self.assertEqual(self._both(date_rows), ('Date', 'Date'))

    def test_nones_beyond_the_sample_do_not_trigger_rewidening(self):
        # None is absent data, not an incompatible value -- the scan
        # already skips it, and the verification pass must too, or every
        # nullable int column would re-infer needlessly on every publish.
        #
        # Asserting the resulting TYPE alone would be a vacuous test: a
        # re-inference triggered by None still returns BigInteger, so the
        # outcome is identical either way and the assertion could never
        # fail. Confirmed directly by trying it. What actually
        # distinguishes correct from broken here is the COST -- whether
        # the full re-inference pass ran at all -- so this counts reads
        # instead, the same way test_verification_only_scans_rows_beyond_
        # the_sample does.
        class CountingDict(dict):
            reads = 0

            def get(self, key, default=None):
                type(self).reads += 1
                return super().get(key, default)

        rows = [CountingDict(v=i) for i in range(self.SAMPLE)]
        rows += [CountingDict(v=None) for _ in range(self.SAMPLE * 5)]

        CountingDict.reads = 0
        result = _infer_column_type(rows, 'v', sample_size=self.SAMPLE)

        self.assertEqual(type(result).__name__, 'BigInteger')
        # Sample, plus one verification sweep of the unsampled rows --
        # and crucially NOT a third, full re-inference pass over all of
        # them, which is what a None-triggered re-widen would cost.
        self.assertEqual(CountingDict.reads, len(rows))

    def test_verification_only_scans_rows_beyond_the_sample(self):
        # The whole point of this shape over a full scan is that the
        # expensive part is skipped for every non-narrowable column.
        # A text column must never reach the verification loop at all.
        class CountingDict(dict):
            reads = 0

            def get(self, key, default=None):
                type(self).reads += 1
                return super().get(key, default)

        rows = [CountingDict(v=f'text {i}') for i in range(self.SAMPLE * 10)]
        CountingDict.reads = 0
        result = _infer_column_type(rows, 'v', sample_size=self.SAMPLE)
        self.assertEqual(type(result).__name__, 'Text')
        # Sample only: the inferred type isn't narrowable, so no row past
        # the sample is ever read.
        self.assertEqual(CountingDict.reads, self.SAMPLE)

    def test_explicit_full_scan_still_means_full_scan(self):
        # sample_size=None must keep its original meaning exactly -- the
        # verification pass is skipped because there is nothing left to
        # verify, not because inference got weaker.
        rows = [{'v': i} for i in range(self.SAMPLE)] + [{'v': 3.5}]
        result = _infer_column_type(rows, 'v', sample_size=None)
        self.assertEqual(type(result).__name__, 'Numeric')



class Test9DiscardPendingReadEnablesTheFirstExplicitTransaction(unittest.TestCase):
    """run_pipelines() calls publisher.discard_pending_read() between the
    source-state read and the pipeline loop. Nothing tested it. Confirmed
    directly by deleting that call from runner.py: all 217 tests still
    passed, because FakeSqlaConnection.begin() used to accept a second
    begin() on an already-transacted connection where real SQLAlchemy
    rejects it.

    The real behavior, confirmed against genuine SQLAlchemy 2.0.43 driving
    a real SQLite engine (not reasoned about from documentation):

        conn.execute(...)        -> autobegin, in_transaction() is True
        conn.begin()             -> InvalidRequestError

    That matters because SourceStateStore's reads and DDL go through
    publisher.ensure_connection(), never through _ensure_transaction(), so
    a source-check-enabled run reaches the pipeline loop with an implicit
    transaction already open on the connection. The first publish() then
    calls _ensure_transaction() -> conn.begin() and, without the discard,
    raises -- on every such run, in production, immediately.

    These tests drive the real DbPublisher against the corrected fake, so
    the mechanism is covered here regardless of whether the sandbox has a
    real SQLAlchemy available.
    """

    def _publisher_on_open_connection(self):
        """A DbPublisher whose connection has an autobegun transaction --
        exactly the state a source-state read leaves behind."""
        publisher = DbPublisher(creds=_CREDS, schema='bsr')
        publisher._engine = FakeSqlaEngine()
        conn = publisher.ensure_connection()
        conn.execute('select 1 from task_scaffold_meta', {'task_name': 't'})
        return publisher, conn

    def test_a_source_state_read_leaves_an_implicit_transaction_open(self):
        # The precondition the discard exists for. If this ever stops being
        # true, the rest of this class is testing nothing.
        publisher, conn = self._publisher_on_open_connection()
        self.assertIsNone(publisher._tx)
        self.assertTrue(conn.in_transaction())

    def test_begin_without_discarding_the_pending_read_is_rejected(self):
        publisher, _ = self._publisher_on_open_connection()
        with self.assertRaises(sa_exc.InvalidRequestError):
            publisher._ensure_transaction()

    def test_discard_pending_read_lets_the_first_publish_open_its_transaction(self):
        publisher, conn = self._publisher_on_open_connection()
        publisher.discard_pending_read()
        self.assertFalse(conn.in_transaction())
        publisher._ensure_transaction()
        self.assertIsNotNone(publisher._tx)
        self.assertTrue(conn.in_transaction())

    def test_discard_pending_read_does_not_touch_an_explicit_transaction(self):
        # Once _tx exists, the pipeline loop owns the transaction and the
        # discard must be a no-op -- rolling back here would silently throw
        # away an already-published table mid-run.
        publisher = DbPublisher(creds=_CREDS, schema='bsr')
        publisher._engine = FakeSqlaEngine()
        publisher._ensure_transaction()
        tx_before = publisher._tx
        conn = publisher._conn

        publisher.discard_pending_read()

        self.assertIs(publisher._tx, tx_before)
        self.assertTrue(conn.in_transaction())

    def test_discard_pending_read_is_safe_before_any_connection_exists(self):
        # run_pipelines() can reach the discard with a publisher that has
        # never connected (create_if_missing=False, nothing executed yet).
        publisher = DbPublisher(creds=_CREDS, schema='bsr')
        self.assertIsNone(publisher._conn)
        publisher.discard_pending_read()   # must not raise, must not connect
        self.assertIsNone(publisher._conn)



class Test10ZeroDimensionalArraysAreScalarsNotContainers(unittest.TestCase):
    """The scalar guard added for one-element containers
    (Test7 above) drew its line at pd.api.types.is_scalar(), which says
    False for np.array(5). Correct as a statement about types -- it is an
    ndarray -- and wrong for what these two functions need to decide. A
    zero-dimensional array wraps exactly one scalar and has no container
    semantics to preserve, so np.array(5) reached the DB driver as an
    array instead of the int 5, while np.array([5]) is a genuine
    one-element container and must stay one. `.ndim == 0` is exactly that
    line, and it is the same line for both callers.

    Raised by external review as a narrow normalization-policy question.
    Investigating it turned up a second, older asymmetry the review did
    not reach, confirmed directly across dtypes: pd.isna() returns a plain
    numpy bool for a TYPED zero-dim array (float64, datetime64) but a
    zero-dim ARRAY for an object-dtype one. So np.array(np.nan) already
    normalized to None while np.array(pd.NaT), np.array(None) and
    np.array(pd.NA) did not -- four values that all hold nothing,
    behaving two different ways based only on the dtype numpy inferred.

    The review's own suggested one-liner would have fixed the first half
    and made the second half worse: with the guard relaxed but is_missing()
    left alone, np.array(pd.NaT) stops being an array and becomes a bare
    pd.NaT handed to the driver -- confirmed directly before choosing the
    shared-predicate shape instead.
    """

    def test_a_zero_dim_array_normalizes_to_its_scalar(self):
        import numpy as np
        self.assertEqual(_normalize_value(np.array(5)), 5)
        self.assertIsInstance(_normalize_value(np.array(5)), int)
        self.assertEqual(_normalize_value(np.array('x')), 'x')

    def test_every_zero_dim_missing_value_normalizes_to_none_regardless_of_dtype(self):
        # The asymmetry. float64 and datetime64 already worked; the three
        # object-dtype cases did not.
        import numpy as np
        for value in (np.array(np.nan), np.array(pd.NaT), np.array(None), np.array(pd.NA)):
            with self.subTest(dtype=str(value.dtype)):
                self.assertTrue(is_missing(value))
                self.assertIsNone(_normalize_value(value))

    def test_one_element_containers_are_still_preserved(self):
        # The property Test7 exists for must survive this change -- a
        # one-element container is not a scalar no matter how it is spelled.
        import numpy as np
        for value in (np.array([5]), np.array([None]), pd.Series([5]), [5], [None]):
            with self.subTest(value=repr(value)):
                self.assertFalse(is_missing(value))
                result = _normalize_value(value)
                self.assertIs(type(result), type(value))

    def test_multi_element_containers_are_still_preserved(self):
        import numpy as np
        for value in (np.array([1, 2]), [1, 2], pd.Series([1, 2])):
            with self.subTest(value=repr(value)):
                self.assertFalse(is_missing(value))
                self.assertIs(type(_normalize_value(value)), type(value))

    def test_genuine_numpy_and_pandas_scalars_are_unaffected(self):
        # numpy scalars report .ndim == 0 too, but are already is_scalar(),
        # so the first branch short-circuits and nothing changes for them.
        import numpy as np
        result = _normalize_value(np.int64(5))
        self.assertEqual(result, 5)
        self.assertIsInstance(result, int)
        self.assertNotIsInstance(result, np.integer)
        self.assertEqual(_normalize_value(pd.Timestamp('2020-01-01')), datetime(2020, 1, 1))
        for missing in (pd.NA, float('nan'), pd.NaT, None):
            self.assertIsNone(_normalize_value(missing))



class Test11IdentifierValidationPreflightAndRuntime(unittest.TestCase):
    """Every PostgreSQL identifier this code constructs or accepts is
    validated against the byte limit before it reaches SQL, and its
    uniqueness-bearing suffix is never truncated.

    Two tiers, because neither alone is sufficient. Preflight is pure and
    connection-free, so a bad declaration fails before any resource is
    built -- but it cannot see column names that come from data, and it
    has to assume a limit rather than read one. Runtime sees the real
    columns and the real server value, but only after the pipeline has
    already done its work.

    The failure being prevented is silent: PostgreSQL truncates an
    over-long identifier and emits a NOTICE, not an error, and psycopg2
    exposes notices on the connection where nothing here reads them. The
    name would simply differ from the one this code believes it created.
    """

    def _payload(self, **kwargs):
        import petl as etl
        from task_core.db_publish import from_petl
        kwargs.setdefault('table_name', 't')
        kwargs.setdefault('schema', None)
        tbl = kwargs.pop('tbl', etl.wrap([['v'], ['x']]))
        return from_petl(tbl, **kwargs)

    def _publisher(self, **kwargs):
        # A real SQLAlchemy engine (in-memory SQLite, no driver needed)
        # rather than FakeSqlaEngine: these tests drive genuine DDL through
        # sa.Table.create(), which needs a real bind. It also means the
        # server-limit fallback is exercised for real -- SQLite has no
        # max_identifier_length, so _effective_identifier_limit() takes its
        # documented fallback to the configured value.
        import sqlalchemy as sa
        publisher = DbPublisher(creds=_CREDS, schema=None, **kwargs)
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    # --- the naming rule itself -------------------------------------

    def test_only_the_readable_prefix_is_ever_shortened(self):
        run_token = new_run_token()
        name = staging_table_name('bsr', 'x' * 200, run_token)
        self.assertLessEqual(len(name.encode('utf-8')), MAX_IDENTIFIER_BYTES)
        # The uniqueness-bearing suffix survives whole -- truncating it
        # would defeat the entire reason it exists.
        self.assertTrue(name.endswith(f'_{run_token}'))
        self.assertIn(f'__{STAGING_NAME_KIND}_', name)

    def test_truncation_is_by_bytes_and_never_splits_a_character(self):
        # Characters would not be enough: confirmed directly that a
        # 62-character Cyrillic name is 116 UTF-8 bytes.
        name = staging_table_name('bsr', 'ы' * 200, new_run_token())
        self.assertLessEqual(len(name.encode('utf-8')), MAX_IDENTIFIER_BYTES)
        self.assertEqual(name.encode('utf-8').decode('utf-8'), name)

    def test_the_target_token_does_not_depend_on_the_run(self):
        # This is what makes static collision checking exact rather than
        # probabilistic. Fold the run into the hash and two targets could
        # collide under one run and not another.
        first = staging_target_token('bsr', 'sales')
        second = staging_target_token('bsr', 'sales')
        self.assertEqual(first, second)
        self.assertNotEqual(first, staging_target_token('bsr', 'other'))
        self.assertNotEqual(first, staging_target_token('other_schema', 'sales'))

    def test_two_targets_sharing_a_truncated_prefix_get_distinct_names(self):
        run_token = new_run_token()
        a = 'sales_pipeline_report_by_region_and_quarter_northern'
        b = 'sales_pipeline_report_by_region_and_quarter_southern'
        self.assertNotEqual(
            staging_table_name('bsr', a, run_token),
            staging_table_name('bsr', b, run_token),
        )

    def test_concurrent_runs_get_different_physical_names(self):
        name = 'sales'
        self.assertNotEqual(
            staging_table_name('bsr', name, new_run_token()),
            staging_table_name('bsr', name, new_run_token()),
        )

    # --- preflight ---------------------------------------------------

    def test_preflight_rejects_an_over_long_declared_table(self):
        specs = {'p': tc.PipelineSpec(db_table='x' * 70)}
        with self.assertRaises(DbPublishError) as caught:
            DbPublisher.preflight(specs, schema='bsr')
        self.assertIn('p:', str(caught.exception))

    def test_preflight_rejects_a_non_portable_declared_name(self):
        for spec in (tc.PipelineSpec(db_table='отчет'),
                     tc.PipelineSpec(db_table='Sales'),
                     tc.PipelineSpec(db_table='t', db_output=['Блок']),
                     tc.PipelineSpec(db_table='t', db_updated_at='Загружено')):
            with self.subTest(spec=spec):
                with self.assertRaises(DbPublishError):
                    DbPublisher.preflight({'p': spec}, schema='bsr')

    def test_quoted_mode_permits_non_portable_names_but_not_over_long_ones(self):
        DbPublisher.preflight(
            {'p': tc.PipelineSpec(db_table='отчет', db_output=['Блок'],
                                  db_identifier_mode='quoted')},
            schema='bsr',
        )
        with self.assertRaises(DbPublishError):
            DbPublisher.preflight(
                {'p': tc.PipelineSpec(db_table='ы' * 40, db_identifier_mode='quoted')},
                schema='bsr',
            )

    def test_preflight_validates_the_schema_as_portable_regardless_of_mode(self):
        # Schema is task-wide, so no per-spec flag may reach it.
        with self.assertRaises(DbPublishError):
            DbPublisher.preflight(
                {'p': tc.PipelineSpec(db_table='t', db_identifier_mode='quoted')},
                schema='Схема',
            )

    def test_preflight_is_a_no_op_when_nothing_declares_a_db_table(self):
        # A genuinely DB-free task should not have backend policy applied.
        DbPublisher.preflight({'p': tc.PipelineSpec(excel_name='out.xlsx')}, schema='Схема')

    def test_preflight_catches_a_staging_collision_before_anything_is_built(self):
        specs = {
            'a': tc.PipelineSpec(db_table='sales_pipeline_report_by_region_x_northern'),
            'b': tc.PipelineSpec(db_table='sales_pipeline_report_by_region_x_northern'),
        }
        # Same target twice is caught upstream by validate_pipeline_classes;
        # reaching preflight directly proves preflight itself sees it too.
        with self.assertRaises(DbPublishError):
            DbPublisher.preflight(specs, schema='bsr')

    def test_preflight_opens_no_connection(self):
        engine_calls = []

        class ExplodingEngine:
            def connect(self):
                engine_calls.append(1)
                raise AssertionError('preflight must not connect')

        DbPublisher.preflight({'p': tc.PipelineSpec(db_table='hr_staff')}, schema='bsr')
        self.assertEqual(engine_calls, [])

    # --- runtime -----------------------------------------------------

    def test_column_names_are_validated_after_the_contract_is_applied(self):
        # Placement is load-bearing: run before the contract and this
        # rejects raw Cyrillic spreadsheet headers, which is 77 of the 79
        # source names in this project and would break every hr_task
        # pipeline. Run after, the renamed targets pass.
        publisher = self._publisher()
        import petl as etl
        renamed = self._payload(tbl=etl.wrap([['Блок'], ['x']]), db_contract={'Блок': 'block'})
        self.assertEqual(renamed.columns, ['block'])
        publisher.publish(renamed)

        publisher = self._publisher()
        unrenamed = self._payload(tbl=etl.wrap([['Блок'], ['x']]))
        with self.assertRaises(DbPublishError):
            publisher.publish(unrenamed)

    def test_quoted_payload_mode_permits_a_non_portable_column(self):
        import petl as etl
        publisher = self._publisher()
        payload = self._payload(tbl=etl.wrap([['Блок'], ['x']]), identifier_mode='quoted')
        publisher.publish(payload)

    def test_an_injected_limit_tightens_validation(self):
        publisher = self._publisher(max_identifier_bytes=20)
        with self.assertRaises(DbPublishError):
            publisher.publish(self._payload(table_name='a_fairly_long_table_name'))

    def test_the_generated_name_registry_catches_a_repeated_target(self):
        # Impossible by construction and already excluded by preflight --
        # which is exactly why it is asserted. Raises the INVARIANT error,
        # not the task-author-facing one.
        publisher = self._publisher()
        payload = self._payload(table_name='dup')
        publisher.publish(payload)
        with self.assertRaises(DbPublishInvariantError) as caught:
            publisher.publish(payload)
        self.assertIn('internal invariant violated', str(caught.exception))

    def test_the_invariant_error_is_still_a_db_publish_error(self):
        # Existing cleanup paths catch DbPublishError; they must keep
        # catching this, while isinstance still tells the two apart.
        self.assertTrue(issubclass(DbPublishInvariantError, DbPublishError))

    def test_validate_identifier_rejects_empty_and_nul_in_both_modes(self):
        for bad in ('', 'a\x00b'):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    validate_identifier(bad, MAX_IDENTIFIER_BYTES, kind='table name')



class Test12StagingSwapAndReviewCorrections(unittest.TestCase):
    """The publication phase itself, driven through the real DbPublisher
    against a real SQLAlchemy engine -- previously covered only indirectly,
    through fakes in runner tests and through naming/preflight logic. The
    method at the centre of the publication architecture had no direct
    test.

    Also covers the corrections from external review, each confirmed
    directly before fixing.
    """

    def _publisher(self, **kwargs):
        import sqlalchemy as sa
        publisher = DbPublisher(creds=_CREDS, schema=None, **kwargs)
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    def _seed(self, publisher, ddl, insert):
        import sqlalchemy as sa
        conn = publisher.ensure_connection()
        conn.execute(sa.text(ddl))
        conn.execute(sa.text(insert))
        publisher.commit()
        publisher.discard_pending_read()
        return conn

    def _payload(self, table_name, tbl):
        from task_core.db_publish import from_petl
        return from_petl(tbl, table_name=table_name, schema=None)

    def _tables(self, conn):
        import sqlalchemy as sa
        return conn.execute(sa.text(
            "select name from sqlite_master where type='table'")).scalars().all()

    # --- the swap itself ---------------------------------------------

    def test_the_live_table_is_untouched_until_commit(self):
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v bigint)', 'insert into t values (1),(2),(3)')

        publisher.publish(self._payload('t', etl.wrap([['v'], [9]])))
        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [1, 2, 3])

        publisher.commit()
        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [9])

    def test_schema_evolution_survives_the_swap(self):
        # The property that made DROP+CREATE worth keeping over TRUNCATE.
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v bigint)', 'insert into t values (1)')

        publisher.publish(self._payload('t', etl.wrap([['v', 'extra'], [1.5, 'a']])))
        publisher.commit()

        columns = conn.execute(sa.text('select name from pragma_table_info("t")')).scalars().all()
        self.assertEqual(columns, ['v', 'extra'])

    def test_no_staging_table_survives_a_successful_run(self):
        import petl as etl
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v int)', 'insert into t values (1)')
        publisher.publish(self._payload('t', etl.wrap([['v'], [9]])))
        publisher.commit()
        self.assertEqual(self._tables(conn), ['t'])

    def test_rollback_after_staging_leaves_the_live_table_untouched(self):
        """Asserts the live table only, deliberately, NOT the absence of a
        staging table.

        Orphan-residue behavior cannot be tested on this backend and it
        would be dishonest to pretend otherwise. Confirmed directly:
        pysqlite does not begin a transaction for DDL, so CREATE TABLE is
        auto-committed and survives a rollback, while DML rolls back
        correctly. PostgreSQL has genuine transactional DDL and does not
        behave this way -- but that is documentation, not something this
        suite can demonstrate, and this session has already turned up two
        cases where documented and actual needed checking separately.

        What this test does cover is the property that matters either way:
        a rolled-back run never modifies the live table.
        """
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v int)', 'insert into t values (1)')

        publisher.publish(self._payload('t', etl.wrap([['v'], [9]])))
        publisher.rollback()

        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [1])

    def test_several_tables_swap_together(self):
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table a (v int)', 'insert into a values (1)')
        conn.execute(sa.text('create table b (v int)'))
        conn.execute(sa.text('insert into b values (2)'))
        publisher.commit()
        publisher.discard_pending_read()

        publisher.publish(self._payload('a', etl.wrap([['v'], [10]])))
        publisher.publish(self._payload('b', etl.wrap([['v'], [20]])))
        publisher.commit()

        self.assertEqual(conn.execute(sa.text('select v from a')).scalar(), 10)
        self.assertEqual(conn.execute(sa.text('select v from b')).scalar(), 20)
        self.assertEqual(sorted(self._tables(conn)), ['a', 'b'])

    def test_commit_alone_publishes_without_any_separate_finalize_call(self):
        """The regression that made finalization private. With the swap
        exposed as a public method the runner had to remember to call,
        publish() + commit() reported committed=True and
        committed_tables=['None.t'] while the live table still held its old
        rows and the staging table was committed permanently -- a publisher
        reporting success having published nothing. Confirmed directly
        before the fix.
        """
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v int)', 'insert into t values (1)')

        publisher.publish(self._payload('t', etl.wrap([['v'], [99]])))
        publisher.commit()

        self.assertTrue(publisher.committed)
        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [99])
        self.assertEqual(self._tables(conn), ['t'])

    def test_the_publisher_protocol_did_not_grow_a_finalize_method(self):
        # publisher_factory is an advertised testing and extension seam.
        # A publisher written against the previous contract died with
        # AttributeError at the end of an otherwise successful run when
        # finalization was public -- confirmed directly.
        self.assertFalse(hasattr(DbPublisher, 'finalize_published_tables'))

    # --- review corrections ------------------------------------------

    def test_a_portable_identifier_may_not_carry_a_trailing_newline(self):
        # Python's `$` matches before a trailing newline, so match()
        # accepted 'foo\n'. fullmatch() is the fix, in both users of the
        # shared pattern.
        with self.assertRaises(DbPublishError):
            validate_portable_identifier('foo\n', kind='table name')

    def test_an_unrecognised_payload_identifier_mode_is_rejected(self):
        # `mode == 'portable'` meant any typo silently selected the
        # permissive branch. PipelineSpec validates its own field, but a
        # DbPayload built directly does not pass through it.
        publisher = self._publisher()
        payload = DbPayload(table_name='t', schema=None, columns=['x'],
                            rows=[{'x': 1}], identifier_mode='portbale')
        with self.assertRaises(DbPublishError):
            publisher.publish(payload)

    def test_the_source_state_table_is_a_reserved_target(self):
        """A pipeline declaring the source-state table as its db_table
        destroyed the stored fingerprints and still reported success.
        Reproduced end to end: the run updated fingerprints in the real
        table, then the swap dropped it and renamed the pipeline's staging
        table over it. The staging design is what lets this succeed
        silently -- under direct publication the later upsert would likely
        have failed on missing columns.
        """
        specs = {'p': tc.PipelineSpec(db_table='task_scaffold_meta')}
        with self.assertRaises(DbPublishError) as caught:
            DbPublisher.preflight(specs, schema='bsr',
                                  source_state_target=('bsr', 'task_scaffold_meta'))
        self.assertIn('source-state', str(caught.exception))

        # A different table in the same schema is fine.
        DbPublisher.preflight({'p': tc.PipelineSpec(db_table='hr_staff')}, schema='bsr',
                              source_state_target=('bsr', 'task_scaffold_meta'))

    def test_source_state_identifiers_are_length_checked(self):
        # SourceStateStore validated the regex and nothing else, so a
        # 64-byte lower-case source-state table name was accepted and would
        # have been silently truncated -- the exact failure this mechanism
        # exists to prevent, still reachable through the technical table.
        with self.assertRaises(DbPublishError):
            DbPublisher.preflight({}, schema='bsr', source_state_target=('bsr', 'a' * 64))

    def test_preflight_still_runs_when_only_the_source_state_table_exists(self):
        # A source-check-only run creates and writes a real table, so
        # skipping preflight because no pipeline declares db_table left it
        # unvalidated.
        with self.assertRaises(DbPublishError):
            DbPublisher.preflight({'p': tc.PipelineSpec(excel_name='out.xlsx')},
                                  schema='bsr', source_state_target=('bsr', 'Отчет'))

    def test_a_leading_null_run_does_not_force_a_text_column(self):
        # If the whole sample is null, Text is a guess rather than an
        # observation. Sparse columns routinely have long leading null runs
        # after sorting or monthly expansion.
        import datetime
        for tail, expected in ((1, 'BigInteger'), (1.5, 'Numeric'),
                               (datetime.date(2024, 1, 1), 'Date')):
            with self.subTest(tail=tail):
                rows = [{'v': None}] * 5000 + [{'v': tail}]
                self.assertEqual(type(_infer_column_type(rows, 'v')).__name__, expected)

    def test_a_genuinely_text_column_is_still_text(self):
        # The all-null branch must not fire for a column that really is
        # text -- 'saw nothing' and 'saw text' both resolve to Text and
        # only one of them should trigger a rescan.
        rows = [{'v': 'x'}] * 5000 + [{'v': 'y'}]
        self.assertEqual(type(_infer_column_type(rows, 'v')).__name__, 'Text')

    def test_a_non_postgres_backend_does_not_fail_on_the_missing_setting(self):
        # SQLite has no max_identifier_length. The dialect branch means
        # this is not an error, while a real PostgreSQL failure is no
        # longer swallowed.
        publisher = self._publisher(max_identifier_bytes=40)
        self.assertEqual(publisher._effective_identifier_limit(), 40)



class Test13IdentifierValidationGapsFromReview(unittest.TestCase):
    """Three gaps between what the documentation claimed and what the code
    enforced, each confirmed directly before fixing.
    """

    def _publisher(self, **kwargs):
        import sqlalchemy as sa
        publisher = DbPublisher(creds=_CREDS, schema=None, **kwargs)
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    def test_a_direct_payload_cannot_carry_a_non_portable_table_name(self):
        """Runtime validation applied the portable pattern to columns but
        only length and NUL checks to payload.table_name -- so a directly
        constructed payload in the default strict mode published to a
        Cyrillic table. The runner path caught it through PipelineSpec
        preflight; direct payload construction does not go through that,
        and this function already validates the payload's own
        identifier_mode, which means direct use is part of the contract.
        """
        publisher = self._publisher()
        payload = DbPayload(table_name='Отчет', schema=None, columns=['x'],
                            rows=[{'x': 1}], identifier_mode='portable')
        with self.assertRaises(DbPublishError):
            publisher.publish(payload)

    def test_quoted_mode_still_permits_a_non_portable_table_name(self):
        publisher = self._publisher()
        payload = DbPayload(table_name='Отчет', schema=None, columns=['x'],
                            rows=[{'x': 1}], identifier_mode='quoted')
        publisher.publish(payload)

    def test_the_schema_is_portable_regardless_of_payload_mode(self):
        # Matching the rule preflight applies: schema is task-wide while
        # the mode is per-payload, so a per-payload flag must not relax it.
        publisher = self._publisher()
        payload = DbPayload(table_name='t', schema='Схема', columns=['x'],
                            rows=[{'x': 1}], identifier_mode='quoted')
        with self.assertRaises(DbPublishError):
            publisher.publish(payload)

    def test_identifier_modes_have_one_definition(self):
        # PipelineSpec.__post_init__ carried its own literal tuple while
        # db_publish defined the constant -- the same closed set in two
        # places, with nothing keeping them equal.
        from task_core.types import IDENTIFIER_MODES as from_types
        from task_core.db_publish import IDENTIFIER_MODES as from_db_publish
        self.assertIs(from_types, from_db_publish)
        self.assertEqual(from_types, ('portable', 'quoted'))

    def test_the_spec_accepts_exactly_the_shared_set_and_nothing_more(self):
        # Derived from the constant rather than listing values: a hardcoded
        # list of BAD values cannot catch the spec quietly ACCEPTING an
        # extra one, which is precisely the drift having two definitions
        # allowed. Confirmed by adding a third mode to the spec's own
        # check and watching an earlier version of this test pass.
        from task_core.types import IDENTIFIER_MODES

        for mode in IDENTIFIER_MODES:
            with self.subTest(accepted=mode):
                self.assertEqual(tc.PipelineSpec(db_identifier_mode=mode).db_identifier_mode, mode)

        for mode in ('portbale', '', None, 'PORTABLE', 'legacy', 'raw'):
            with self.subTest(rejected=mode):
                self.assertNotIn(mode, IDENTIFIER_MODES)
                with self.assertRaises(ValueError):
                    tc.PipelineSpec(db_identifier_mode=mode)

    def test_the_payload_rejects_exactly_the_same_set(self):
        # The other half: both validators must move together.
        from task_core.types import IDENTIFIER_MODES

        publisher = self._publisher()
        for mode in ('portbale', 'legacy', 'raw'):
            with self.subTest(mode=mode):
                self.assertNotIn(mode, IDENTIFIER_MODES)
                payload = DbPayload(table_name='t', schema=None, columns=['x'],
                                    rows=[{'x': 1}], identifier_mode=mode)
                with self.assertRaises(DbPublishError):
                    publisher.publish(payload)


class Test14ServerIdentifierLimitResolution(unittest.TestCase):
    """server_identifier_limit() is the authoritative runtime check. Its
    PostgreSQL failure branch was previously untested -- the only coverage
    confirmed that SQLite does not issue the statement, which exercises the
    fallback rather than the branch that matters.
    """

    class _Dialect:
        def __init__(self, name):
            self.name = name

    class _Conn:
        def __init__(self, dialect_name, result=None, error=None):
            self.dialect = Test14ServerIdentifierLimitResolution._Dialect(dialect_name)
            self._result = result
            self._error = error
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append(str(statement))
            if self._error is not None:
                raise self._error

            class _Result:
                def __init__(self, value):
                    self._value = value

                def scalar(self):
                    return self._value

            return _Result(self._result)

    def test_a_non_postgres_backend_is_not_asked(self):
        conn = self._Conn('sqlite')
        self.assertEqual(server_identifier_limit(conn, 63), 63)
        self.assertEqual(conn.statements, [], 'SHOW issued against a backend that has no such setting')

    def test_postgres_is_asked_and_the_answer_is_used(self):
        conn = self._Conn('postgresql', result=63)
        self.assertEqual(server_identifier_limit(conn, 63), 63)
        self.assertEqual(len(conn.statements), 1)
        self.assertIn('max_identifier_length', conn.statements[0])

    def test_configuration_can_only_tighten_never_raise(self):
        # A configured value larger than the server's would produce names
        # the server silently truncates -- the failure this exists to
        # prevent.
        self.assertEqual(server_identifier_limit(self._Conn('postgresql', result=63), 200), 63)
        self.assertEqual(server_identifier_limit(self._Conn('postgresql', result=63), 40), 40)

    def test_a_postgres_failure_raises_rather_than_falling_back(self):
        """The branch that had no coverage. A catch-all made the
        authoritative check non-authoritative -- any error silently
        restored the assumed value. And because the statement can run
        inside an open transaction, a failure leaves the PostgreSQL
        transaction aborted, so the next DDL fails with a secondary
        transaction-aborted error obscuring the real cause.
        """
        original = RuntimeError('connection reset')
        conn = self._Conn('postgresql', error=original)

        with self.assertRaises(DbPublishError) as caught:
            server_identifier_limit(conn, 63)

        self.assertIs(caught.exception.__cause__, original)
        self.assertIn('max_identifier_length', str(caught.exception))



if __name__ == '__main__':
    unittest.main()
