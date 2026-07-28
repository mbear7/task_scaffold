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

import sys
import time
import unittest
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import sqlalchemy as sa
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
    IdentifierPolicy,
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


class _FakeSqlaDialect:
    # Real SQLAlchemy connections carry one, and the staged model asks for
    # it to decide whether to issue PostgreSQL-only statements (advisory
    # locks, comments, catalog reads). Non-postgres takes the fallbacks.
    name = 'sqlite'


class FakeSqlaConnection:
    """Models sqlalchemy.engine.Connection's real autobegin semantics."""

    dialect = _FakeSqlaDialect()
    # A real Connection carries this, and the publisher checks it before
    # every reuse: SQLAlchemy transparently reconnects an invalidated
    # Connection, which would continue on a session holding none of this
    # run's advisory locks.
    invalidated = False

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
        # MERGE, not replace. The original assigned committed_rows outright,
        # so a second commit with nothing pending erased everything the
        # first had committed -- which no real database does. Harmless
        # while a run had exactly one commit; wrong the moment the staged
        # model gave a run several.
        self._engine.committed_rows.update(self._pending)
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
    publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t')
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
            DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t', chunk_size=0)

    def test_negative_is_rejected(self):
        from task_core.db_publish import DbPublishError
        with self.assertRaises(DbPublishError):
            DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t', chunk_size=-1)

    def test_positive_still_works(self):
        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t', chunk_size=100)
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

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t')
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

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t')
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

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t')
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

        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t')
        publisher._conn = GoodConn()
        publisher._engine = GoodEngine()
        publisher.close()  # must not raise

    def test_close_with_nothing_ever_connected_does_not_raise(self):
        publisher = DbPublisher(creds={'user': 'x', 'host': 'x', 'dbname': 'x'}, schema='bsr', task_name='t')
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
        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='t', **kwargs)
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
        publisher = self._publisher(identifier_policy=IdentifierPolicy(max_identifier_bytes=20))
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
        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='t', **kwargs)
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    def _seed(self, publisher, ddl, insert):
        import sqlalchemy as sa
        conn = publisher.ensure_connection()
        conn.execute(sa.text(ddl))
        conn.execute(sa.text(insert))
        publisher.commit()
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
        publisher = self._publisher(identifier_policy=IdentifierPolicy(max_identifier_bytes=40))
        self.assertEqual(publisher._effective_identifier_limit(), 40)



class Test13IdentifierValidationGapsFromReview(unittest.TestCase):
    """Three gaps between what the documentation claimed and what the code
    enforced, each confirmed directly before fixing.
    """

    def _publisher(self, **kwargs):
        import sqlalchemy as sa
        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='t', **kwargs)
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



class Test15StagedPublicationModel(unittest.TestCase):
    """Preparation transactions commit per target; one short publication
    transaction swaps everything.

    The single run-long transaction it replaces was atomic but unbounded:
    it stayed open across remote file reads, transformations and Excel
    exports, holding catalog locks, delaying vacuum, accumulating WAL and
    making a late rollback expensive. Committing each preparation bounds
    all of that to one table's load.

    What it costs is that rollback is no longer the cleanup mechanism --
    see rollback() and Test16.
    """

    def _publisher(self, **kwargs):
        import sqlalchemy as sa
        kwargs.setdefault('task_name', 'demo_task')
        publisher = DbPublisher(creds=_CREDS, schema=None, **kwargs)
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    def _seed(self, publisher, ddl, insert=None):
        import sqlalchemy as sa
        conn = publisher.ensure_connection()
        conn.execute(sa.text(ddl))
        if insert:
            conn.execute(sa.text(insert))
        publisher._commit_transaction()
        return conn

    def _payload(self, table_name, tbl):
        from task_core.db_publish import from_petl
        return from_petl(tbl, table_name=table_name, schema=None)

    def _staging_tables(self, conn):
        import sqlalchemy as sa
        return conn.execute(sa.text(
            "select name from sqlite_master where type='table' and name like '%__stg_%'"
        )).scalars().all()

    def test_preparation_commits_and_leaves_the_live_table_untouched(self):
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v int)', 'insert into t values (1)')

        publisher.publish(self._payload('t', etl.wrap([['v'], [99]])))

        # Checked FIRST: any query below autobegins a transaction of its
        # own, so asking afterwards would measure the test, not publish().
        self.assertFalse(conn.in_transaction(), 'preparation left a transaction open')

        # The distinguishing property of the staged model: the staging
        # table is COMMITTED while the live table is untouched.
        self.assertEqual(len(self._staging_tables(conn)), 1)
        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [1])

    def test_the_publication_phase_swaps_and_removes_staging(self):
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v int)', 'insert into t values (1)')

        publisher.publish(self._payload('t', etl.wrap([['v'], [99]])))
        publisher.commit()

        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [99])
        self.assertEqual(self._staging_tables(conn), [])

    def test_schema_evolution_still_works(self):
        # The property that made replacement worth keeping over TRUNCATE,
        # and which the staged model must not quietly cost.
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table t (v bigint)', 'insert into t values (1)')

        publisher.publish(self._payload('t', etl.wrap([['v', 'extra'], [1.5, 'a']])))
        publisher.commit()

        columns = conn.execute(sa.text('select name from pragma_table_info("t")')).scalars().all()
        self.assertEqual(columns, ['v', 'extra'])

    def test_several_targets_prepare_independently_and_swap_together(self):
        import petl as etl
        import sqlalchemy as sa
        publisher = self._publisher()
        conn = self._seed(publisher, 'create table a (v int)', 'insert into a values (1)')
        conn.execute(sa.text('create table b (v int)'))
        conn.execute(sa.text('insert into b values (2)'))
        publisher._commit_transaction()

        publisher.publish(self._payload('a', etl.wrap([['v'], [10]])))
        # After the FIRST preparation commits, the second live table is
        # still its old self -- the swap has not happened for either.
        self.assertEqual(conn.execute(sa.text('select v from a')).scalar(), 1)
        publisher.publish(self._payload('b', etl.wrap([['v'], [20]])))

        publisher.commit()
        self.assertEqual(conn.execute(sa.text('select v from a')).scalar(), 10)
        self.assertEqual(conn.execute(sa.text('select v from b')).scalar(), 20)

    def test_the_publication_plan_runs_inside_the_publication_transaction(self):
        """The source-state write is queued rather than executed by the
        runner, so it lands in the same transaction as the swaps. A failed
        publication must not advance the stored fingerprints.
        """
        import petl as etl
        from task_core.db_publish import PublicationPlan

        plan = PublicationPlan()
        performed = []
        plan.add('source state', lambda: performed.append('source-state'))

        publisher = self._publisher(publication_plan=plan)
        self._seed(publisher, 'create table t (v int)')
        publisher.publish(self._payload('t', etl.wrap([['v'], [5]])))

        self.assertEqual(performed, [], 'queued work ran before the publication phase')
        publisher.commit()
        self.assertEqual(performed, ['source-state'])
        self.assertEqual(len(plan), 0, 'the plan was not cleared after running')

    def test_a_run_with_no_prepared_targets_still_runs_the_plan(self):
        # The source-check-only shape: no db_table anywhere, but the
        # fingerprints still have to be written and committed.
        from task_core.db_publish import PublicationPlan

        plan = PublicationPlan()
        performed = []
        plan.add('source state', lambda: performed.append('source-state'))

        publisher = self._publisher(publication_plan=plan)
        self._seed(publisher, 'create table t (v int)')
        publisher.commit()

        self.assertEqual(performed, ['source-state'])
        self.assertTrue(publisher.committed)


class Test16RollbackIsCleanupNotRollback(unittest.TestCase):
    """rollback()'s meaning changed with the staged model.

    Preparation transactions are already committed, so it cannot undo them
    transactionally -- it must DROP this run's staging tables. That makes
    it capable of failing for new reasons, notably a lost connection, so it
    never raises: the exception that caused the abort matters more than a
    cleanup failure, and losing it would be the worse outcome.
    """

    def _publisher(self, **kwargs):
        import sqlalchemy as sa
        kwargs.setdefault('task_name', 'demo_task')
        publisher = DbPublisher(creds=_CREDS, schema=None, **kwargs)
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    def _prepared(self):
        import petl as etl
        import sqlalchemy as sa
        from task_core.db_publish import from_petl

        publisher = self._publisher()
        conn = publisher.ensure_connection()
        conn.execute(sa.text('create table t (v int)'))
        conn.execute(sa.text('insert into t values (1)'))
        publisher._commit_transaction()
        publisher.publish(from_petl(etl.wrap([['v'], [99]]), table_name='t', schema=None))
        return publisher, conn

    def _staging_count(self, conn):
        import sqlalchemy as sa
        return conn.execute(sa.text(
            "select count(*) from sqlite_master where type='table' and name like '%__stg_%'"
        )).scalar()

    def test_rollback_drops_committed_staging_tables(self):
        import sqlalchemy as sa
        publisher, conn = self._prepared()
        self.assertEqual(self._staging_count(conn), 1)

        publisher.rollback()

        self.assertEqual(self._staging_count(conn), 0)
        self.assertEqual(conn.execute(sa.text('select v from t')).scalars().all(), [1])

    def test_rollback_never_raises_even_when_dropping_fails(self):
        publisher, conn = self._prepared()

        class _Exploding:
            dialect = conn.dialect
            invalidated = False

            def execute(self, *args, **kwargs):
                raise RuntimeError('connection reset mid-cleanup')

            def commit(self):
                raise RuntimeError('connection reset mid-cleanup')

            def in_transaction(self):
                return False

            def close(self):
                pass

        publisher._conn = _Exploding()
        publisher.rollback()   # must not raise

    def test_rollback_with_a_lost_connection_does_nothing_and_does_not_raise(self):
        publisher, _conn = self._prepared()
        publisher.mark_connection_lost()
        publisher.rollback()

    def test_rollback_clears_the_publication_plan(self):
        from task_core.db_publish import PublicationPlan
        plan = PublicationPlan()
        plan.add('source state', lambda: None)
        publisher = self._publisher(publication_plan=plan)
        publisher.rollback()
        self.assertEqual(len(plan), 0, 'a queued source-state write survived the abort')



class Test17PreparationValidationAndOwnership(unittest.TestCase):
    """The PostgreSQL-only half of preparation: exact ordered column
    verification, row-count verification, and the ownership comment that
    makes cleanup possible without a registry table.

    Driven through a connection fake reporting a postgresql dialect,
    because these paths deliberately no-op elsewhere and SQLite therefore
    exercises none of them -- confirmed by deleting the whole block and
    watching the suite stay green.
    """

    class _Conn:
        """A REAL SQLAlchemy SQLite connection that reports itself as
        postgresql, with the four PostgreSQL-only statements intercepted.

        A wholly synthetic connection cannot bind real DDL --
        sa.Table.drop() reaches for _run_ddl_visitor -- so the parts under
        test would end up exercising the fake rather than the code. This
        way CREATE/INSERT/DROP/RENAME are genuinely executed and only the
        catalog interactions are stood in for.
        """

        def __init__(self, columns=None):
            import sqlalchemy as sa
            self._engine = sa.create_engine('sqlite://')
            self._real = self._engine.connect()
            self.columns = columns
            self.comments = {}
            self.statements = []

        invalidated = False

        @property
        def dialect(self):
            return type('D', (), {'name': 'postgresql'})()

        def execute(self, statement, params=None):
            import sqlalchemy as sa
            text = str(statement)
            self.statements.append(text)
            lowered = text.lower()

            if 'max_identifier_length' in lowered:
                return _Scalar(63)
            if lowered.startswith('select to_regclass'):
                # The lock phase probes which targets already exist. SQLite
                # has no to_regclass, and the proxied connection would
                # otherwise raise.
                name = (params or {}).get('name', '').split('.')[-1]
                found = self._real.execute(sa.text(
                    "select name from sqlite_master where type='table' and name = :n"
                ), {'n': name}).scalar()
                return _Scalar(found)
            if lowered.startswith('lock table'):
                return None
            if lowered.startswith('set local'):
                return None
            if 'pg_try_advisory_lock' in lowered:
                return _Scalar(True)
            if 'pg_class' in lowered and 'relname like' in lowered:
                return _Rows([])            # predecessor scan: nothing left behind
            if 'pg_class' in lowered and 'relname = ' in lowered:
                target = (params or {}).get('table', '')
                return _Scalar(self.comments.get(target))
            if 'information_schema.columns' in lowered:
                return [(name,) for name in (self.columns or [])]
            if lowered.startswith('comment on table'):
                # Split on the FIRST ' is ', and take the identifier from
                # the left of it. Parsing from the right lands inside the
                # JSON body, which is full of double quotes -- an earlier
                # version of this fake did exactly that and silently
                # recorded a timestamp as the table name.
                split_at = lowered.index(' is ')
                head, body = text[:split_at], text[split_at + 4:]
                name = head[len('comment on table '):].strip().strip('"')
                self.comments[name] = body.strip()[1:-1].replace("''", "'")
                return None
            if 'obj_description' in lowered:
                target = (params or {}).get('name', '')
                return _Scalar(self.comments.get(target.split('.')[-1]))
            if ' rename to ' in lowered:
                # PostgreSQL keeps comments on the OID, so a rename CARRIES
                # the comment to the new name. Modelled because that is
                # precisely why the published-table comment has to be
                # replaced: without it every published table would still
                # wear its staging ownership metadata and look to cleanup
                # like an abandoned artifact.
                head, new_name = text.split(' rename to ', 1)
                old_name = head[len('alter table '):].strip().strip('"')
                new_name = new_name.strip().strip('"')
                if old_name in self.comments:
                    self.comments[new_name] = self.comments.pop(old_name)

            # Everything else runs for real, against SQLite -- but with the
            # schema stripped, since SQLite has none.
            text = text.replace('"bsr".', '').replace('bsr.', '')
            return self._real.execute(sa.text(text), params or {})

        def _run_ddl_visitor(self, visitorcallable, element, **kwargs):
            return self._real._run_ddl_visitor(visitorcallable, element, **kwargs)

        def begin(self):
            return _NoopTx(self)

        def in_transaction(self):
            return self._real.in_transaction()

        def commit(self):
            self._real.commit()

        def rollback(self):
            self._real.rollback()

        def close(self):
            self._real.close()

    def _publisher(self, conn):
        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='demo_task')
        publisher._conn = conn
        publisher._engine = object()
        # publish() now enforces the unconditional-lock contract itself
        # rather than trusting the caller, so the fixture must claim the
        # task the way a real run does.
        publisher.begin_run()
        return publisher

    def _payload(self, columns=('a', 'b')):
        from task_core.db_publish import DbPayload
        return DbPayload(
            table_name='target', schema=None, columns=list(columns),
            rows=[{name: 1 for name in columns}],
        )

    def test_a_prepared_table_carries_this_run_s_ownership_metadata(self):
        from task_core.db_publish import parse_staging_comment
        conn = self._Conn(columns=['a', 'b'])
        publisher = self._publisher(conn)

        publisher.publish(self._payload())

        self.assertEqual(len(conn.comments), 1, 'no ownership comment was attached')
        owner = parse_staging_comment(next(iter(conn.comments.values())))
        self.assertIsNotNone(owner, 'the comment is not parseable as ownership metadata')
        self.assertEqual(owner['task'], 'demo_task')
        self.assertEqual(owner['target_table'], 'target')
        self.assertIsNone(owner['target_schema'])
        self.assertIn('created_at', owner)

    def test_column_names_out_of_order_are_rejected(self):
        # Exact ORDERED equality. Ordinal position is trustworthy here
        # precisely because the staging table was just created.
        conn = self._Conn(columns=['b', 'a'])
        publisher = self._publisher(conn)
        with self.assertRaises(DbPublishInvariantError):
            publisher.publish(self._payload(('a', 'b')))

    def test_a_missing_column_is_rejected(self):
        conn = self._Conn(columns=['a'])
        publisher = self._publisher(conn)
        with self.assertRaises(DbPublishInvariantError):
            publisher.publish(self._payload(('a', 'b')))

    def test_a_short_load_is_rejected(self):
        """Row count is authoritative because the payload is fully
        materialized: len(payload.rows) is the exact set of dicts handed to
        the driver. Counted in the chunking loop, not taken from the
        driver, because SQLAlchemy reports supports_sane_multi_rowcount as
        False for psycopg2 -- so a driver count would measure the rewritten
        statement rather than the logical rows. This guards our chunking.
        """
        conn = self._Conn(columns=['a'])
        publisher = self._publisher(conn)
        publisher.chunk_size = 1

        from task_core.db_publish import DbPayload
        payload = DbPayload(table_name='target', schema=None, columns=['a'],
                            rows=[{'a': 1}, {'a': 2}, {'a': 3}])

        real_chunked = sys.modules['task_core.db_publish']._chunked
        try:
            sys.modules['task_core.db_publish']._chunked = (
                lambda rows, size: real_chunked(rows[:-1], size)   # silently drops one
            )
            with self.assertRaises(DbPublishInvariantError) as caught:
                publisher.publish(payload)
            self.assertIn('loaded 2 rows', str(caught.exception))
        finally:
            sys.modules['task_core.db_publish']._chunked = real_chunked

    def test_publication_refuses_a_staging_table_that_lost_its_metadata(self):
        conn = self._Conn(columns=['a'])
        publisher = self._publisher(conn)
        publisher.publish(self._payload(('a',)))

        conn.comments.clear()   # someone dropped or replaced it
        with self.assertRaises(DbPublishError):
            publisher.commit()

    def test_a_published_table_no_longer_carries_staging_ownership(self):
        # ALTER TABLE ... RENAME preserves comments, so without replacement
        # every published table would look to cleanup like an abandoned
        # staging artifact.
        from task_core.db_publish import parse_staging_comment
        conn = self._Conn(columns=['a'])
        publisher = self._publisher(conn)
        publisher.publish(self._payload(('a',)))
        publisher.commit()

        self.assertIsNone(
            parse_staging_comment(conn.comments.get('target')),
            'the published table still carries staging ownership metadata',
        )


class Test18ConnectionLossIsFatal(unittest.TestCase):
    """One of three interlocking rules that close the stale-publisher trap:
    a session-scoped advisory lock, cleanup performed under it, and a
    refusal to reconnect after the session is gone.

    Reconnecting would silently continue without the lock, and the lock is
    what guarantees no other run of this task is live -- which is what
    makes predecessor cleanup safe. A reconnect looks harmless, which is
    exactly why refusing has to be explicit.
    """

    def _publisher(self):
        import sqlalchemy as sa
        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='demo_task')
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        return publisher

    def test_ensure_connection_refuses_after_the_connection_is_lost(self):
        publisher = self._publisher()
        publisher.ensure_connection()
        publisher.mark_connection_lost()

        with self.assertRaises(DbPublishError) as caught:
            publisher.ensure_connection()
        self.assertIn('advisory lock', str(caught.exception))

    def test_a_lost_connection_drops_the_lock_flag(self):
        publisher = self._publisher()
        publisher.try_acquire_task_lock()
        self.assertTrue(publisher.lock_held)

        publisher.mark_connection_lost()
        self.assertFalse(publisher.lock_held, 'the run still believes it holds the lock')

    def test_publish_after_a_lost_connection_raises_rather_than_reconnecting(self):
        import petl as etl
        from task_core.db_publish import from_petl
        publisher = self._publisher()
        publisher.ensure_connection()
        publisher.mark_connection_lost()

        with self.assertRaises(DbPublishError):
            publisher.publish(from_petl(etl.wrap([['v'], [1]]), table_name='t', schema=None))


class _Rows:
    """Result stand-in where only .all() is reached."""

    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _Scalar:
    """Minimal stand-in for a SQLAlchemy Result where only .scalar() is
    reached."""

    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _NoopTx:
    def __init__(self, conn):
        self._conn = conn

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()



class Test19TaskAdvisoryLockAndPredecessorCleanup(unittest.TestCase):
    """Three rules interlock to close the stale-publisher trap, and
    removing any one of them reopens it:

      1. the task lock is SESSION-scoped, so it survives the staged
         model's many committed transactions;
      2. connection loss is FATAL, so a run whose session was reaped
         cannot wake up and publish;
      3. predecessor cleanup runs UNDER the lock, so anything this task
         left behind belongs to a run that is definitively gone.

    Traced: a stalled process with a live session still holds the lock, so
    nothing newer can start. If the server reaps the session instead, the
    lock releases and the connection dies with it -- and rule 2 stops that
    process from executing anything on waking. A newer run then acquires
    the lock and drops the predecessor's artifacts, so even a resumed run
    finds them gone at its publication check.

    That is why predecessor cleanup doubles as the generation guard and no
    separate generation column is needed. A future maintainer would
    reasonably 'simplify' any one of the three; this class is what should
    stop them.
    """

    class _Conn:
        """Postgres-dialect fake recording lock calls and catalog reads."""

        def __init__(self, tables=None, lock_granted=True):
            self.tables = tables or {}          # relname -> comment
            self.lock_granted = lock_granted
            self.lock_calls = []
            self.dropped = []

        invalidated = False

        @property
        def dialect(self):
            return type('D', (), {'name': 'postgresql'})()

        def execute(self, statement, params=None):
            text = str(statement)
            lowered = text.lower()

            if 'max_identifier_length' in lowered:
                return _Scalar(63)
            if 'pg_try_advisory_lock' in lowered:
                self.lock_calls.append(('acquire', params))
                return _Scalar(self.lock_granted)
            if 'pg_advisory_unlock' in lowered:
                self.lock_calls.append(('release', params))
                return _Scalar(True)
            if 'pg_class' in lowered and 'obj_description' in lowered:
                return _Rows([(name, comment) for name, comment in self.tables.items()])
            if lowered.startswith('drop table'):
                name = text.split('"')[-2]
                self.dropped.append(name)
                self.tables.pop(name, None)
                return None
            return None

        def in_transaction(self):
            return False

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    def _publisher(self, conn, task_name='demo_task'):
        publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name=task_name)
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()
        return publisher

    def _owned_by(self, task, run='deadbeef', target='hr_staff'):
        from task_core.db_publish import build_staging_comment
        return build_staging_comment(
            task_name=task, run_token=run, schema='bsr', table_name=target,
        )

    def _conforming_name(self, target='hr_staff', run='deadbeef'):
        """A name with the exact staging shape the strict rule requires:
        __stg_<8 hex>_<8 hex>, with the target token actually derived from
        the logical target."""
        from task_core.db_publish import staging_target_token
        return f'{target}__stg_{staging_target_token("bsr", target)}_{run}'

    def test_begin_run_acquires_the_lock_and_reports_success(self):
        conn = self._Conn()
        publisher = self._publisher(conn)

        self.assertTrue(publisher.begin_run())
        self.assertTrue(publisher.lock_held)
        self.assertEqual([kind for kind, _ in conn.lock_calls], ['acquire'])

    def test_begin_run_reports_failure_without_cleaning_anything(self):
        # A run that did not win the lock must not touch another run's
        # artifacts -- that is the whole safety argument for cleanup.
        conn = self._Conn(
            tables={self._conforming_name(): self._owned_by('demo_task')},
            lock_granted=False,
        )
        publisher = self._publisher(conn)

        self.assertFalse(publisher.begin_run())
        self.assertFalse(publisher.lock_held)
        self.assertEqual(conn.dropped, [], 'cleanup ran without holding the lock')

    def test_the_lock_key_is_namespaced_and_stable(self):
        from task_core.db_publish import advisory_lock_key
        first = advisory_lock_key('hr_task')
        self.assertEqual(first, advisory_lock_key('hr_task'))
        self.assertNotEqual(first, advisory_lock_key('ops_task'))
        # Two-int form: advisory locks are database-wide and shared with
        # anything else using them, so a bare hash could collide with an
        # unrelated application and present as this task mysteriously
        # refusing to run.
        namespace, key = first
        self.assertEqual(namespace, advisory_lock_key('ops_task')[0])
        self.assertTrue(-2**31 <= key < 2**31, 'key does not fit PostgreSQL int4')

    def test_cleanup_drops_only_this_task_s_positively_identified_artifacts(self):
        mine = self._conforming_name()
        conn = self._Conn(tables={
            mine: self._owned_by('demo_task'),
            self._conforming_name('other'): self._owned_by('a_different_task', target='other'),
            self._conforming_name('mystery'): 'not json at all',
            self._conforming_name('plain'): None,
        })
        publisher = self._publisher(conn)
        publisher.begin_run()

        self.assertEqual(conn.dropped, [mine])

    def test_a_name_without_the_exact_staging_shape_is_never_dropped(self):
        """The catalog scan uses a broad LIKE because SQL cannot express
        the token shapes. Without a strict check here, any table whose name
        merely CONTAINED the infix could be dropped on the strength of a
        syntactically valid comment -- confirmed directly with
        'not_really__stg_whatever'.
        """
        for relname in ('not_really__stg_whatever',
                        'x__stg_short_deadbeef',
                        'x__stg_DEADBEEF_deadbeef',
                        'x__stg_deadbeef_deadbeef_extra'):
            with self.subTest(relname=relname):
                conn = self._Conn(tables={relname: self._owned_by('demo_task')})
                self._publisher(conn).begin_run()
                self.assertEqual(conn.dropped, [], f'{relname} was dropped')

    def test_the_name_and_the_comment_must_agree(self):
        # Each being well-formed on its own is not positive identification.
        from task_core.db_publish import build_staging_comment
        mine = self._conforming_name('hr_staff')

        wrong_run = build_staging_comment(
            task_name='demo_task', run_token='cafebabe', schema='bsr', table_name='hr_staff',
        )
        wrong_schema = build_staging_comment(
            task_name='demo_task', run_token='deadbeef', schema='other', table_name='hr_staff',
        )
        wrong_target = build_staging_comment(
            task_name='demo_task', run_token='deadbeef', schema='bsr', table_name='something_else',
        )
        for label, comment in (('run', wrong_run), ('schema', wrong_schema),
                               ('target', wrong_target)):
            with self.subTest(disagreement=label):
                conn = self._Conn(tables={mine: comment})
                self._publisher(conn).begin_run()
                self.assertEqual(conn.dropped, [])

    def test_an_unparseable_comment_is_never_dropped(self):
        # Unknown ownership is not permission. This is the rule that keeps
        # cleanup from becoming an outage.
        for comment in ('not json', '{}', '{"marker":"something_else"}',
                        '{"marker":"task_core","v":999}', ''):
            with self.subTest(comment=comment):
                conn = self._Conn(tables={self._conforming_name('x'): comment})
                publisher = self._publisher(conn)
                publisher.begin_run()
                self.assertEqual(conn.dropped, [])

    def test_cleanup_never_drops_this_run_s_own_artifacts(self):
        conn = self._Conn()
        publisher = self._publisher(conn)
        conn.tables[self._conforming_name('mine', run=publisher._run_token)] = (
            self._owned_by('demo_task', run=publisher._run_token, target='mine')
        )

        publisher.begin_run()
        self.assertEqual(conn.dropped, [])

    def test_close_releases_the_lock_explicitly(self):
        # Not left to session end. make_engine() uses NullPool so the two
        # are equivalent today, but the dedicated-connection contract rests
        # on NullPool and an explicit unlock keeps it honest.
        conn = self._Conn()
        publisher = self._publisher(conn)
        publisher.begin_run()

        publisher.close()

        self.assertIn('release', [kind for kind, _ in conn.lock_calls])
        self.assertFalse(publisher.lock_held)

    def test_a_lost_connection_does_not_attempt_an_unlock(self):
        conn = self._Conn()
        publisher = self._publisher(conn)
        publisher.begin_run()
        publisher.mark_connection_lost()

        publisher.release_task_lock()
        self.assertEqual([kind for kind, _ in conn.lock_calls], ['acquire'])



class Test20GapsFoundReviewingTheStagedModel(unittest.TestCase):
    """Six findings against 0.3.1, each confirmed directly before fixing.
    Two of them undermined the correctness argument of the staged model
    itself rather than merely tightening it.
    """

    class _Conn:
        """Postgres-dialect fake covering the catalog surface the publisher
        touches, over a real SQLite connection so DDL executes."""

        invalidated = False

        def __init__(self, existing_comments=None, catalog=None):
            import sqlalchemy as sa
            self._engine = sa.create_engine('sqlite://')
            self._real = self._engine.connect()
            self.comments = dict(existing_comments or {})
            self.catalog = list(catalog or [])
            self.columns = None
            self.statements = []

        @property
        def dialect(self):
            return type('D', (), {'name': 'postgresql'})()

        def execute(self, statement, params=None):
            import sqlalchemy as sa
            text = str(statement)
            self.statements.append(text)
            lowered = text.lower()

            if 'max_identifier_length' in lowered:
                return _Scalar(63)
            if lowered.startswith('select to_regclass'):
                name = (params or {}).get('name', '').split('.')[-1]
                found = self._real.execute(sa.text(
                    "select name from sqlite_master where type='table' and name = :n"
                ), {'n': name}).scalar()
                return _Scalar(found)
            if lowered.startswith('lock table') or lowered.startswith('set local'):
                return None
            if 'pg_try_advisory_lock' in lowered or 'pg_advisory_unlock' in lowered:
                return _Scalar(True)
            if 'relname like' in lowered:
                return _Rows(self.catalog)
            if 'relname = ' in lowered:
                return _Scalar(self.comments.get((params or {}).get('table')))
            if 'information_schema.columns' in lowered:
                if self.columns is not None:
                    return [(name,) for name in self.columns]
                rows = self._real.execute(sa.text(
                    'select name from pragma_table_info(:t) order by cid'
                ), {'t': (params or {}).get('table')})
                return [(row[0],) for row in rows]
            if lowered.startswith('comment on table'):
                split_at = lowered.index(' is ')
                head, body = text[:split_at], text[split_at + 4:]
                name = head[len('comment on table '):].strip().strip('"')
                self.comments[name] = body.strip()[1:-1].replace("''", "'")
                return None
            if ' rename to ' in lowered:
                head, new_name = text.split(' rename to ', 1)
                old = head[len('alter table '):].strip().strip('"')
                new_name = new_name.strip().strip('"')
                if old in self.comments:
                    self.comments[new_name] = self.comments.pop(old)

            return self._real.execute(sa.text(text), params or {})

        def _run_ddl_visitor(self, visitorcallable, element, **kwargs):
            return self._real._run_ddl_visitor(visitorcallable, element, **kwargs)

        def begin(self):
            return _NoopTx(self)

        def in_transaction(self):
            return self._real.in_transaction()

        def commit(self):
            self._real.commit()

        def rollback(self):
            self._real.rollback()

        def close(self):
            self._real.close()

    def _publisher(self, conn, *, claim=True):
        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='demo_task')
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()
        if claim:
            publisher.begin_run()
        return publisher

    def _payload(self, table_name='target', schema=None):
        from task_core.db_publish import DbPayload
        return DbPayload(table_name=table_name, schema=schema,
                         columns=['a'], rows=[{'a': 1}])

    # --- 1: the source-state read phase must not span the run -----------

    def test_the_source_state_read_transaction_is_closed_before_pipelines_run(self):
        """The staged model's whole claim is that no transaction spans the
        run. It did not hold with source tracking on: SQLAlchemy autobegins
        on the first source-state statement and nothing committed it until
        the first publish() -- confirmed directly, in_transaction() was
        still True inside a running pipeline. A source-check-only task held
        it for the entire run.
        """
        import sqlalchemy as sa
        from task_core.source_state import SourceStateStore

        engine = sa.create_engine('sqlite://')
        conn = engine.connect()
        self.addCleanup(conn.close)
        conn.execute(sa.text(
            'create table meta (task_name text, source_key text, source_signature text)'
        ))
        conn.commit()

        store = SourceStateStore(conn, schema='main', table='meta')
        store.sources_unchanged('t', [])

        self.assertFalse(
            conn.in_transaction(),
            'the source-state read transaction is still open going into the pipeline loop',
        )

    # --- 2: a lost session must be terminal -----------------------------

    def test_an_invalidated_connection_cannot_be_reused(self):
        """mark_connection_lost() existed but production never called it,
        so the terminal state was unreachable. SQLAlchemy transparently
        reconnects an invalidated Connection on the next statement, which
        continues on a session holding none of this run's advisory locks --
        exactly the stale-publisher case decisions/0006 eliminates.
        """
        conn = self._Conn()
        publisher = self._publisher(conn)
        conn.invalidated = True

        with self.assertRaises(DbPublishError) as caught:
            publisher.ensure_connection()
        self.assertIn('advisory lock', str(caught.exception))
        self.assertFalse(publisher.lock_held, 'the run still believes it holds the lock')

    def test_publish_refuses_without_the_task_lock_on_postgres(self):
        # The runner always claims first, but publisher_factory is an
        # extension seam and DbPublisher is usable directly.
        conn = self._Conn()
        publisher = self._publisher(conn, claim=False)

        with self.assertRaises(DbPublishError) as caught:
            publisher.publish(self._payload())
        self.assertIn('advisory lock', str(caught.exception))

    # --- 3: quoted identifiers must survive verification ----------------

    def test_a_non_portable_staging_name_is_found_at_verification(self):
        """Verification used to_regclass() on an assembled string, which
        parses its argument as an identifier expression and down-cases
        anything unquoted. A mixed-case or Cyrillic staging name produced
        under db_identifier_mode='quoted' therefore prepared correctly and
        was then reported missing at commit().
        """
        conn = self._Conn()
        publisher = self._publisher(conn)

        from task_core.db_publish import DbPayload
        publisher.publish(DbPayload(
            table_name='Sales', schema=None, columns=['a'], rows=[{'a': 1}],
            identifier_mode='quoted',
        ))
        publisher.commit()   # must not raise

        lookups = [s for s in conn.statements if 'obj_description' in s.lower()]
        self.assertTrue(lookups)
        self.assertFalse(
            any('to_regclass' in s.lower() for s in lookups),
            'verification still parses an assembled relation name',
        )

    # --- 4: unknown ownership is never authority ------------------------

    def test_an_incomplete_ownership_comment_does_not_authorize_cleanup(self):
        from task_core.db_publish import parse_staging_comment

        incomplete = (
            '{"marker":"task_core","v":1,"task":"demo_task","run":"deadbeef"}',
            # Python considers True == 1 and 1.0 == 1, so a straight
            # equality check on the version field accepted both --
            # confirmed directly. A version that is not an integer is not a
            # version this code wrote.
            '{"marker":"task_core","v":true,"task":"d","run":"r",'
            '"target_table":"t","target_schema":null,"created_at":"n"}',
            '{"marker":"task_core","v":1.0,"task":"d","run":"r",'
            '"target_table":"t","target_schema":null,"created_at":"n"}',
            '{"marker":"task_core","v":1,"task":"demo_task","run":"x","target_table":"t"}',
            '{"marker":"task_core","v":1,"task":"","run":"x","target_table":"t",'
            '"target_schema":null,"created_at":"now"}',
            '{"marker":"task_core","v":1,"task":"demo_task","run":123,'
            '"target_table":"t","target_schema":null,"created_at":"now"}',
        )
        for comment in incomplete:
            with self.subTest(comment=comment[:48]):
                self.assertIsNone(parse_staging_comment(comment))

        conn = self._Conn(
            existing_comments={'x__stg_a_b': incomplete[0]},
            catalog=[('x__stg_a_b', incomplete[0])],
        )
        publisher = self._publisher(conn)
        self.assertNotIn('drop table', ' '.join(conn.statements).lower())

    def test_a_complete_ownership_comment_is_still_accepted(self):
        from task_core.db_publish import build_staging_comment, parse_staging_comment
        comment = build_staging_comment(
            task_name='demo_task', run_token='abc', schema=None, table_name='t',
        )
        parsed = parse_staging_comment(comment)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['task'], 'demo_task')

    def test_an_object_already_at_the_staging_name_is_never_erased(self):
        """Preparation used drop(checkfirst=True), bypassing the cleanup
        safety rule entirely -- an object at the generated name with no
        comment, an invalid one, or another owner was erased anyway.
        Reproduced: a table holding unrelated data was silently replaced.

        After predecessor cleanup and a fresh run token, an existing exact
        name is a collision, not something to tidy away.
        """
        import sqlalchemy as sa
        from task_core.db_publish import staging_table_name

        conn = self._Conn()
        publisher = self._publisher(conn)
        name = staging_table_name(None, 'target', publisher._run_token)
        conn.execute(sa.text(f'create table "{name}" (someone_elses_data text)'))
        conn.execute(sa.text(f"insert into \"{name}\" values ('precious')"))
        conn.commit()

        with self.assertRaises(Exception):
            publisher.publish(self._payload())

        survived = conn._real.execute(sa.text(f'select * from "{name}"')).all()
        self.assertEqual(survived, [('precious',)], 'unrelated data was destroyed')

    # --- 5: staging stays in the publisher's schema ---------------------

    def test_a_payload_for_another_schema_is_rejected(self):
        # Cleanup scans exactly one schema. A payload prepared elsewhere
        # would leave an orphan no later run ever scans.
        conn = self._Conn()
        publisher = self._publisher(conn)
        with self.assertRaises(DbPublishError) as caught:
            publisher.publish(self._payload(schema='somewhere_else'))
        self.assertIn('cleaned up', str(caught.exception))

    # --- 6: the limit is verified before any DDL ------------------------

    def test_the_server_limit_is_read_before_cleanup_ddl(self):
        conn = self._Conn()
        self._publisher(conn)

        lowered = [s.lower() for s in conn.statements]
        limit_at = next(i for i, s in enumerate(lowered) if 'max_identifier_length' in s)
        lock_at = next(i for i, s in enumerate(lowered) if 'pg_try_advisory_lock' in s)
        self.assertLess(limit_at, lock_at, 'the server limit was read after the task was claimed')



class Test21CleanupNeverReconnectsAfterSessionLoss(unittest.TestCase):
    """rollback() and release_task_lock() use self._conn directly, so they
    bypassed ensure_connection()'s invalidation check.

    Confirmed directly: rollback() executed DROP TABLE and COMMIT on a
    connection marked invalidated while _connection_lost stayed False.
    SQLAlchemy reconnects for those statements, running cleanup on a
    session holding none of this run's advisory locks -- which is the
    stale-publisher scenario decisions/0006 exists to eliminate, arriving
    through the one path that must never reconnect.
    """

    class _Invalidated:
        invalidated = True

        def __init__(self):
            self.statements = []
            self.dialect = type('D', (), {'name': 'postgresql'})()

        def execute(self, statement, params=None):
            self.statements.append(str(statement))
            return None

        def commit(self):
            self.statements.append('COMMIT')

        def in_transaction(self):
            return False

        def close(self):
            pass

    def _prepared_publisher(self):
        import petl as etl
        import sqlalchemy as sa
        from task_core.db_publish import from_petl

        publisher = DbPublisher(creds=_CREDS, schema=None, task_name='demo_task')
        publisher._engine = sa.create_engine('sqlite://')
        self.addCleanup(publisher.close)
        conn = publisher.ensure_connection()
        conn.execute(sa.text('create table t (v int)'))
        publisher._commit_transaction()
        publisher._lock_held = True
        publisher.publish(from_petl(etl.wrap([['v'], [1]]), table_name='t', schema=None))
        return publisher

    def test_rollback_runs_no_sql_on_an_invalidated_connection(self):
        publisher = self._prepared_publisher()
        lost = self._Invalidated()
        publisher._conn = lost

        publisher.rollback()

        self.assertEqual(lost.statements, [], 'cleanup ran on a lost session')
        self.assertTrue(publisher._connection_lost)
        self.assertFalse(publisher.lock_held)

    def test_releasing_the_lock_is_skipped_on_an_invalidated_connection(self):
        # PostgreSQL releases a session-scoped lock when the session dies;
        # attempting an explicit unlock would only force a reconnect.
        publisher = self._prepared_publisher()
        lost = self._Invalidated()
        publisher._conn = lost

        publisher.release_task_lock()

        self.assertEqual(lost.statements, [])
        self.assertFalse(publisher.lock_held)

    def test_close_does_not_reconnect_to_unlock(self):
        publisher = self._prepared_publisher()
        lost = self._Invalidated()
        publisher._conn = lost

        publisher.close()

        self.assertEqual(lost.statements, [])


class Test22ReadPhaseFailuresAreNotSwallowed(unittest.TestCase):
    """A failed read-phase commit reported a successful comparison.

    Confirmed directly: sources_unchanged() returned True with the commit
    having raised, so a run could report sources_unchanged and skip on the
    strength of a database call that failed. It also meant the phase was
    not definitely closed before pipeline execution -- the guarantee the
    commit exists to provide.
    """

    class _Conn:
        invalidated = False
        dialect = type('D', (), {'name': 'sqlite'})()

        def __init__(self, commit_error=None):
            self._commit_error = commit_error

        def execute(self, statement, params=None):
            class _Result:
                def mappings(self_inner):
                    return []
            return _Result()

        def in_transaction(self):
            return True

        def commit(self):
            if self._commit_error is not None:
                raise self._commit_error

        def close(self):
            pass

    def test_a_failed_read_phase_commit_raises(self):
        from task_core.source_state import SourceStateStore
        from task_core.types import SourceCheckError

        original = RuntimeError('server closed the connection')
        store = SourceStateStore(self._Conn(commit_error=original),
                                 schema='main', table='meta')

        with self.assertRaises(SourceCheckError) as caught:
            store.sources_unchanged('t', [])
        self.assertIs(caught.exception.__cause__, original)

    def test_a_clean_read_phase_still_returns_the_comparison(self):
        from task_core.source_state import SourceStateStore
        store = SourceStateStore(self._Conn(), schema='main', table='meta')
        self.assertTrue(store.sources_unchanged('t', []))


class Test23ProtocolAndTransactionRefinements(unittest.TestCase):

    def test_commit_requires_the_lock_for_a_plan_only_publication(self):
        """The lock check was gated on _pending_swaps, so a publication
        plan carrying only the source-state update could be committed by a
        direct caller without ever claiming the task. Queued work writes to
        the database exactly as a swap does.
        """
        from task_core.db_publish import PublicationPlan

        plan = PublicationPlan()
        plan.add('source state', lambda: None)

        publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name='demo_task',
                                publication_plan=plan)
        publisher._conn = Test20GapsFoundReviewingTheStagedModel._Conn()
        publisher._engine = type('E', (), {'dispose': lambda self: None})()

        with self.assertRaises(DbPublishError) as caught:
            publisher.commit()
        self.assertIn('advisory lock', str(caught.exception))

    def test_type_inference_happens_before_the_transaction_opens(self):
        """_build_table() runs type inference, which scans the payload and
        may be O(rows). That needs no transaction, and holding one across
        it works against the point of bounding preparation to database
        work.
        """
        import inspect
        source = inspect.getsource(DbPublisher.publish)
        self.assertLess(
            source.index('_build_table('),
            source.index('self._ensure_transaction()'),
            'the preparation transaction opens before type inference runs',
        )



class Test24InvariantsEnforcedRatherThanAssumed(unittest.TestCase):
    """Guarantees that held only because callers happened to do the right
    thing, or that were advertised as exact and were not."""

    def test_cleanup_refuses_without_the_task_lock(self):
        """begin_run() calls cleanup in the right order, so the runner path
        was safe -- but the method drops tables and could be called
        directly, which would delete another live run's artifacts.
        Confirmed directly: it dropped a staging table with lock_held
        False. A load-bearing invariant left to caller ordering is not
        enforced.
        """
        conn = Test19TaskAdvisoryLockAndPredecessorCleanup._Conn()
        publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name='demo_task')
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()

        with self.assertRaises(DbPublishError) as caught:
            publisher.cleanup_predecessor_artifacts()
        self.assertIn('advisory lock', str(caught.exception))
        self.assertEqual(conn.dropped, [])

    def test_an_unusable_task_name_is_rejected_at_construction(self):
        """An empty task name derived a lock key and staged successfully,
        then wrote an ownership comment that parse_staging_comment()
        REJECTS -- so the run reported its own artifact as unowned and
        failed at commit(), after every pipeline had already run.
        """
        for bad in ('', '   ', 123, []):
            with self.subTest(task_name=bad):
                with self.assertRaises(DbPublishError):
                    DbPublisher(creds=_CREDS, schema='bsr', task_name=bad)

        # None is no longer allowed either. This test used to assert it
        # was, on the stated grounds that such a publisher 'does not
        # participate in locking or ownership' -- which was false:
        # begin_run() derived an advisory key from '' and staging wrote
        # "task": "" into ownership metadata its own parser rejects. The
        # claim was wrong in the test and in the code it described.
        with self.assertRaises(DbPublishError):
            DbPublisher(creds=_CREDS, schema='bsr', task_name=None)

    def test_a_task_name_need_not_be_a_portable_identifier(self):
        # It only has to be a non-empty stable string; it never becomes a
        # SQL identifier.
        DbPublisher(creds=_CREDS, schema='bsr', task_name='Отчёт по HR / weekly')

    def test_the_staging_name_rule_is_literally_exact(self):
        """`$` also matches immediately before a trailing newline, and a
        quoted PostgreSQL identifier may contain one -- so a name
        advertised as exactly `__stg_<8 hex>_<8 hex>` accepted a trailing
        newline.
        """
        from task_core.db_publish import owned_staging_tokens

        self.assertIsNotNone(owned_staging_tokens('x__stg_deadbeef_cafebabe'))
        for rejected in ('x__stg_deadbeef_cafebabe\n',
                         'x__stg_deadbeef_cafebabe ',
                         'x__stg_deadbeef_cafebabe\t'):
            with self.subTest(relname=repr(rejected)):
                self.assertIsNone(owned_staging_tokens(rejected))

    def test_a_boolean_identifier_limit_is_rejected(self):
        # bool subclasses int, so isinstance() accepted True and produced
        # an effective one-byte limit instead of rejecting the config.
        from task_core.db_publish import IdentifierPolicy

        for bad in (True, False, 1.0, '63', None, 0, -1):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    IdentifierPolicy(bad)

        self.assertEqual(IdentifierPolicy(63).max_identifier_bytes, 63)



class Test25TheLockProvesIdentityNotMerelyPresence(unittest.TestCase):
    """`_lock_held` was a boolean, while the lock methods took an arbitrary
    task name. Holding SOME lock is not authority over THIS task's
    artifacts.

    Confirmed directly: a publisher configured for task_a, holding task_a's
    lock, dropped task_b's staging table on request -- the cross-run
    cleanup risk the guard exists to remove, reached through a direct
    caller rather than through ordering. And release_task_lock('task_b')
    cleared the flag while task_a stayed locked for the rest of the
    session.

    The methods no longer take a task name at all; they use self.task_name,
    and the publisher records which task it actually locked.
    """

    def _publisher(self, task_name='task_a'):
        conn = Test19TaskAdvisoryLockAndPredecessorCleanup._Conn()
        publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name=task_name)
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()
        return publisher, conn

    def test_the_lock_records_which_task_it_is_for(self):
        publisher, _conn = self._publisher()
        self.assertIsNone(publisher.locked_task_name)

        publisher.begin_run()
        self.assertEqual(publisher.locked_task_name, 'task_a')
        self.assertTrue(publisher.lock_held)

    def test_the_lock_methods_take_no_task_argument(self):
        # The signature is the fix: a caller cannot name a task other than
        # the one the publisher is configured for, because there is nowhere
        # to name it.
        import inspect
        for name in ('try_acquire_task_lock', 'release_task_lock',
                     'cleanup_predecessor_artifacts'):
            with self.subTest(method=name):
                params = list(inspect.signature(getattr(DbPublisher, name)).parameters)
                self.assertEqual(params, ['self'], f'{name} still accepts a task name')

    def test_a_mismatched_lock_identity_is_an_invariant_violation(self):
        publisher, _conn = self._publisher()
        publisher.begin_run()
        publisher._locked_task_name = 'task_b'   # only reachable by tampering

        with self.assertRaises(DbPublishInvariantError) as caught:
            publisher.cleanup_predecessor_artifacts()
        self.assertIn('task_b', str(caught.exception))

    def test_releasing_unlocks_the_task_that_was_actually_locked(self):
        publisher, conn = self._publisher()
        publisher.begin_run()
        publisher.release_task_lock()

        from task_core.db_publish import advisory_lock_key
        released = [params for kind, params in conn.lock_calls if kind == 'release']
        self.assertEqual(len(released), 1)
        namespace, key = advisory_lock_key('task_a')
        self.assertEqual(released[0], {'ns': namespace, 'key': key})
        self.assertFalse(publisher.lock_held)

    def test_acquiring_twice_is_rejected(self):
        """PostgreSQL counts session advisory locks: acquiring the same one
        twice requires releasing it twice. A second acquisition would leave
        the server holding a lock after release_task_lock() while this
        object reported itself unlocked.

        Loud rather than silently idempotent, because a second begin_run()
        also repeats predecessor cleanup — it signals incorrect lifecycle
        use, not a harmless retry.
        """
        publisher, conn = self._publisher()
        self.assertTrue(publisher.begin_run())

        with self.assertRaises(DbPublishInvariantError) as caught:
            publisher.try_acquire_task_lock()
        self.assertIn('already held', str(caught.exception))

        # And no second acquisition reached the server.
        acquisitions = [k for k, _ in conn.lock_calls if k == 'acquire']
        self.assertEqual(len(acquisitions), 1)

    def test_a_second_begin_run_is_rejected(self):
        publisher, conn = self._publisher()
        publisher.begin_run()
        with self.assertRaises(DbPublishInvariantError):
            publisher.begin_run()

    def test_the_lock_can_be_reacquired_after_release(self):
        # Rejecting a repeat must not make the lifecycle single-use.
        publisher, conn = self._publisher()
        publisher.begin_run()
        publisher.release_task_lock()
        self.assertTrue(publisher.try_acquire_task_lock())
        self.assertEqual(publisher.locked_task_name, 'task_a')

    def test_a_failed_acquisition_records_no_identity(self):
        conn = Test19TaskAdvisoryLockAndPredecessorCleanup._Conn(lock_granted=False)
        publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name='task_a')
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()

        self.assertFalse(publisher.begin_run())
        self.assertIsNone(publisher.locked_task_name)


class Test26TaskNameIsRequiredAndUsable(unittest.TestCase):
    """The staged PostgreSQL lifecycle cannot operate without a task
    identity, so permitting None only moved the failure later.

    Confirmed directly: with task_name=None, begin_run() derived an
    advisory key from '', staging wrote "task": "" into ownership metadata,
    and parse_staging_comment() rejected it -- so the run prepared
    successfully and declared its own artifact unowned at publication.
    """

    def test_task_name_is_required(self):
        import inspect
        parameter = inspect.signature(DbPublisher.__init__).parameters['task_name']
        self.assertIs(parameter.default, inspect.Parameter.empty)

    def test_none_and_empty_are_both_rejected(self):
        for bad in (None, '', '   ', 123, []):
            with self.subTest(task_name=bad):
                with self.assertRaises(DbPublishError):
                    DbPublisher(creds=_CREDS, schema='bsr', task_name=bad)

    def test_a_usable_name_round_trips_through_ownership_metadata(self):
        # The property that was actually broken: a publisher's own comment
        # must parse back as its own.
        from task_core.db_publish import build_staging_comment, parse_staging_comment

        for name in ('hr_task', 'Отчёт по HR / weekly', 'a'):
            with self.subTest(task_name=name):
                publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name=name)
                comment = build_staging_comment(
                    task_name=publisher.task_name, run_token='deadbeef',
                    schema='bsr', table_name='t',
                )
                parsed = parse_staging_comment(comment)
                self.assertIsNotNone(parsed, 'a publisher wrote metadata it cannot read back')
                self.assertEqual(parsed['task'], name)




class Test27PostgresqlCommentDDL(unittest.TestCase):
    def test_numeric_json_fields_are_not_parsed_as_bind_parameters(self):
        import sqlalchemy as sa
        from sqlalchemy.dialects import postgresql
        from task_core.db_publish import build_published_comment

        class CapturingConnection:
            dialect = postgresql.dialect()

            def __init__(self):
                self.statement = None

            def execute(self, statement, params=None):
                self.statement = statement

        conn = CapturingConnection()
        publisher = DbPublisher.__new__(DbPublisher)
        publisher.ensure_connection = lambda: conn

        comment = build_published_comment(
            task_name='demo_task', run_token='deadbeef', rows=7,
        )
        publisher._set_comment('bsr', 'demo_table', comment)

        self.assertIsInstance(conn.statement, sa.schema.SetTableComment)
        compiled = conn.statement.compile(dialect=conn.dialect)
        self.assertIsNone(compiled.params)
        sql = str(compiled)
        self.assertIn('"v":1', sql)
        self.assertIn('"rows":7', sql)
        self.assertNotIn('%(1)s', sql)
        self.assertNotIn('%(7)s', sql)

class Test27PublicationLockIsBounded(unittest.TestCase):
    """PostgreSQL queues new ACCESS SHARE behind a waiting ACCESS
    EXCLUSIVE, so a publisher waiting on one long reader blocks every
    reader that arrives afterwards. Bounding the wait is what stops one
    slow query turning a publication into a read outage.
    """

    class _Conn:
        invalidated = False

        def __init__(self, lock_failures=0, sqlstate='55P03'):
            self.statements = []
            self._lock_failures = lock_failures
            self._sqlstate = sqlstate
            self.lock_attempts = 0

        @property
        def dialect(self):
            return type('D', (), {'name': 'postgresql'})()

        def execute(self, statement, params=None):
            text = str(statement)
            self.statements.append(text)
            lowered = text.lower()

            if 'max_identifier_length' in lowered:
                return _Scalar(63)
            if 'advisory' in lowered:
                return _Scalar(True)
            if 'from pg_class c' in lowered and 'relname = :table' in lowered and 'oid from' in lowered:
                return _Scalar('oid')      # every target already exists
            if lowered.startswith('lock table'):
                self.lock_attempts += 1
                if self.lock_attempts <= self._lock_failures:
                    raise _DbapiError(self._sqlstate)
                return None
            if 'relname like' in lowered:
                return _Rows([])
            if 'relname = ' in lowered:
                return _Scalar(self.owner_comment)
            return None

        def in_transaction(self): return False
        def begin(self): return _NoopTx(self)
        def commit(self): pass
        def rollback(self): pass
        def close(self): pass

    def _publisher(self, conn, policy=None):
        from task_core.db_publish import PublicationLockPolicy
        publisher = DbPublisher(
            creds=_CREDS, schema='bsr', task_name='demo_task',
            publication_lock_policy=policy or PublicationLockPolicy(
                lock_timeout_ms=10, acquisition_timeout_ms=100,
                retry_horizon_seconds=30,
                retry_delay_min_seconds=0, retry_delay_max_seconds=0,
            ),
        )
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()
        publisher.begin_run()
        from task_core.db_publish import build_staging_comment, staging_table_name
        staging = staging_table_name('bsr', 'target', publisher._run_token)
        conn.owner_comment = build_staging_comment(
            task_name='demo_task', run_token=publisher._run_token,
            schema='bsr', table_name='target',
        )
        publisher._pending_swaps = [('bsr', 'target', staging, 1)]
        publisher._generated_names = {('bsr', staging)}
        return publisher

    def _lock_statement(self, conn):
        return next(s for s in conn.statements if s.lower().startswith('lock table'))

    def test_all_targets_are_locked_in_one_sorted_statement(self):
        """One statement, not one lock per DROP. The incremental form holds
        locks on already-swapped tables while queuing for the next, and
        each held lock blocks new readers -- so the amplification compounds.
        Sorted, which is what stops two tasks with overlapping targets
        deadlocking against each other.
        """
        conn = self._Conn()
        publisher = self._publisher(conn)
        from task_core.db_publish import build_staging_comment, staging_table_name
        for table in ('zebra', 'alpha'):
            staging = staging_table_name('bsr', table, publisher._run_token)
            publisher._pending_swaps.append(('bsr', table, staging, 1))
        conn.owner_comment = None   # verification is not what this test is about
        publisher._verify_prepared_artifacts = lambda: None

        publisher.commit()

        statement = self._lock_statement(conn)
        self.assertEqual(conn.lock_attempts, 1, 'locks were acquired incrementally')
        self.assertIn('access exclusive mode', statement.lower())
        self.assertLess(statement.index('"alpha"'), statement.index('"target"'))
        self.assertLess(statement.index('"target"'), statement.index('"zebra"'))

    def test_both_timeouts_are_set_local_before_locking(self):
        conn = self._Conn()
        publisher = self._publisher(conn)
        publisher.commit()

        before_lock = conn.statements[:conn.statements.index(self._lock_statement(conn))]
        joined = ' '.join(s.lower() for s in before_lock)
        self.assertIn('set local lock_timeout', joined)
        self.assertIn('set local statement_timeout', joined)

    def test_the_budgets_are_lifted_once_the_locks_are_held(self):
        # The horizon bounds the WAIT, not the work. Cancelling the swap
        # halfway would be strictly worse than letting it finish.
        conn = self._Conn()
        self._publisher(conn).commit()
        after = conn.statements[conn.statements.index(self._lock_statement(conn)) + 1:]
        joined = ' '.join(s.lower() for s in after)
        self.assertIn('set local lock_timeout = 0', joined)
        self.assertIn('set local statement_timeout = 0', joined)

    def test_a_first_ever_publication_locks_nothing(self):
        class _Fresh(Test27PublicationLockIsBounded._Conn):
            def execute(self, statement, params=None):
                lowered = str(statement).lower()
                if 'from pg_class c' in lowered and 'oid from' in lowered:
                    self.statements.append(str(statement))
                    return _Scalar(None)      # target does not exist yet
                return super().execute(statement, params)

        conn = _Fresh()
        self._publisher(conn).commit()
        self.assertFalse(any(s.lower().startswith('lock table') for s in conn.statements))

    def test_a_lock_timeout_retries_the_whole_publication(self):
        """Retry is cheap only because preparation already committed: a
        failed publication discards a swap, not the run. Under the
        single-transaction design this replaced, a lock timeout would have
        meant redoing every pipeline.
        """
        conn = self._Conn(lock_failures=2)
        publisher = self._publisher(conn)

        publisher.commit()

        self.assertEqual(conn.lock_attempts, 3)
        self.assertTrue(publisher.committed)

    def test_an_operator_cancellation_is_terminal(self):
        """57014 is NOT uniquely statement_timeout -- pg_cancel_backend(),
        a client cancel, or a role-level statement_timeout all produce it.
        Retrying would mean arguing with a human who deliberately stopped
        the run.
        """
        conn = self._Conn(lock_failures=1, sqlstate='57014')
        publisher = self._publisher(conn)

        with self.assertRaises(Exception) as caught:
            publisher.commit()
        self.assertNotIsInstance(caught.exception, DbPublishError)
        self.assertEqual(conn.lock_attempts, 1, 'an explicit cancellation was retried')

    def test_a_deadlock_is_terminal(self):
        # Sorted order prevents deadlock between task_core publications, so
        # one means something outside the scaffold takes exclusive locks on
        # these tables -- which a retry does not fix.
        conn = self._Conn(lock_failures=1, sqlstate='40P01')
        publisher = self._publisher(conn)

        with self.assertLogs(publisher.log, level='ERROR') as captured:
            with self.assertRaises(Exception):
                publisher.commit()

        self.assertEqual(conn.lock_attempts, 1)
        # Loud as well as terminal. Terminality alone would be satisfied by
        # the catch-all branch, so asserting only that leaves the ERROR
        # unverified -- confirmed by removing 40P01 from the loud set and
        # watching an earlier version of this test still pass.
        self.assertIn('40P01', ' '.join(captured.output))

    def test_the_horizon_gates_completion_not_merely_starting(self):
        """A horizon that only gated permission to START an attempt would
        let one begun just inside it run well past -- a hint, not a limit.
        """
        from task_core.db_publish import PublicationLockPolicy
        conn = self._Conn(lock_failures=99)
        publisher = self._publisher(conn, PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_horizon_seconds=0.3,
            retry_delay_min_seconds=0.25, retry_delay_max_seconds=0.25,
        ))

        started = time.monotonic()
        with self.assertRaises(DbPublishError) as caught:
            publisher.commit()
        elapsed = time.monotonic() - started

        # Stopped rather than slept past the horizon: sleeping in order to
        # give up wastes the wait and holds the advisory lock longer.
        self.assertLess(elapsed, 0.25)
        self.assertIn('elapsed', str(caught.exception))

    def test_an_exhausted_horizon_refuses_to_start_a_useless_attempt(self):
        # A remaining budget too small for lock_timeout to sit below
        # statement_timeout cannot produce a well-formed attempt, so there
        # is nothing worth starting.
        from task_core.db_publish import PublicationLockPolicy
        conn = self._Conn(lock_failures=99)
        publisher = self._publisher(conn, PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_horizon_seconds=0.02,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        with self.assertRaises(DbPublishError) as caught:
            publisher.commit()
        self.assertIn('usable timeout budget', str(caught.exception))
        self.assertEqual(conn.lock_attempts, 0, 'a useless attempt was issued')

    def test_exhaustion_reports_actual_elapsed_and_attempts(self):
        # Budgets are derived from the remaining horizon, so reporting the
        # configured policy would not reconcile with what happened.
        from task_core.db_publish import PublicationLockPolicy
        conn = self._Conn(lock_failures=99)
        publisher = self._publisher(conn, PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_horizon_seconds=1.0,
            retry_delay_min_seconds=0.05, retry_delay_max_seconds=0.05,
        ))
        with self.assertRaises(DbPublishError) as caught:
            publisher.commit()
        message = str(caught.exception)
        self.assertIn('attempts', message)
        self.assertIn('elapsed', message)
        self.assertGreater(conn.lock_attempts, 1)

    def test_max_attempts_is_a_defensive_ceiling(self):
        from task_core.db_publish import PublicationLockPolicy
        conn = self._Conn(lock_failures=99)
        publisher = self._publisher(conn, PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_horizon_seconds=60, max_attempts=3,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        with self.assertRaises(DbPublishError) as caught:
            publisher.commit()
        self.assertEqual(conn.lock_attempts, 3)
        self.assertIn('max_attempts', str(caught.exception))


class _DbapiError(sa.exc.DBAPIError):
    """A real SQLAlchemy DBAPIError carrying a SQLSTATE.

    Subclasses the genuine class rather than Exception: commit() catches
    sa.exc.DBAPIError specifically, so a stand-in that merely looked like
    one would sail past the handler under test and prove nothing.
    """

    def __init__(self, pgcode):
        self.orig = type('Orig', (), {'pgcode': pgcode})()
        self.statement = None
        self.params = None
        self.connection_invalidated = False
        Exception.__init__(self, f'simulated {pgcode}')



class Test28LockPhaseFindingsFromReview(unittest.TestCase):
    """Three release blockers plus their neighbours, each confirmed
    directly before fixing."""

    def test_a_quoted_live_target_is_found_and_therefore_locked(self):
        """to_regclass() folds unquoted input to lower case, so a live
        table named "Sales" was passed as bsr.Sales, looked up as
        bsr.sales, and reported missing -- then EXCLUDED from the bounded
        LOCK, leaving its DROP to acquire ACCESS EXCLUSIVE with no timeout.
        The one table most needing the bound escaped it.

        Same defect already fixed once in _verify_prepared_artifacts() and
        reintroduced here: assembling a name for the parser is the trap,
        not any particular call site.
        """
        probes = []

        class _Probe(Test27PublicationLockIsBounded._Conn):
            def execute(self, statement, params=None):
                lowered = str(statement).lower()
                if 'from pg_class c' in lowered and 'oid from' in lowered:
                    probes.append(dict(params or {}))
                return super().execute(statement, params)

        for table in ('Sales', 'sales report', 'a"quote', 'portable_name'):
            with self.subTest(table=table):
                probes.clear()
                conn = _Probe()
                publisher = self._publisher(conn, table_name=table)
                publisher._verify_prepared_artifacts = lambda: None
                publisher.commit()

                # Passed as an exact value, never assembled into a name the
                # parser will re-interpret.
                self.assertIn({'schema': 'bsr', 'table': table}, probes)
                statement = next(
                    s for s in conn.statements if s.lower().startswith('lock table')
                )
                self.assertIn(table.replace('"', '""'), statement)

    def _publisher(self, conn, table_name='target', policy=None):
        from task_core.db_publish import (
            PublicationLockPolicy, build_staging_comment, staging_table_name,
        )
        publisher = DbPublisher(
            creds=_CREDS, schema='bsr', task_name='demo_task',
            publication_lock_policy=policy or PublicationLockPolicy(
                lock_timeout_ms=10, acquisition_timeout_ms=100,
                retry_delay_min_seconds=0, retry_delay_max_seconds=0,
            ),
        )
        publisher._conn = conn
        publisher._engine = type('E', (), {'dispose': lambda self: None})()
        publisher.begin_run()
        staging = staging_table_name('bsr', table_name, publisher._run_token)
        conn.owner_comment = build_staging_comment(
            task_name='demo_task', run_token=publisher._run_token,
            schema='bsr', table_name=table_name,
        )
        publisher._pending_swaps = [('bsr', table_name, staging, 1)]
        publisher._generated_names = {('bsr', staging)}
        return publisher

    def test_a_policy_that_cannot_cover_the_worst_case_warns(self):
        """A WORST-CASE capacity check, not a contention measurement.

        The publisher knows n -- existing targets -- and cannot know k, how
        many will actually contend. Claiming "mis-sized for n contended
        targets" would assert something it has not observed, and would fire
        on every publication for a task with many uncontended tables. The
        warning says what it means: this policy cannot cover the case where
        all of them contend.

        The margin belongs in the requirement too. n x lock_timeout exactly
        equal to the aggregate leaves nothing for statement execution,
        lock-manager work or driver latency -- which is why the defaults
        cover nine sequentially contended targets, not ten.
        """
        from task_core.db_publish import PublicationLockPolicy, staging_table_name

        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=500, acquisition_timeout_ms=600,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        for table in ('second', 'third'):
            staging = staging_table_name('bsr', table, publisher._run_token)
            publisher._pending_swaps.append(('bsr', table, staging, 1))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertLogs(publisher.log, level='WARNING') as captured:
            publisher.commit()

        message = ' '.join(captured.output)
        self.assertIn('worst case', message)
        self.assertIn('all 3 existing target', message)
        # It must NOT claim to have observed contention it cannot see.
        self.assertNotIn('3 contended target', message)
        # 'may', not 'will': even fully contended, an individual wait can
        # reach lock_timeout and raise retryable 55P03 before the aggregate
        # is exhausted. The inequality only shows the full per-conflict
        # budget cannot be RESERVED for every target.
        self.assertIn('may then exhaust', message)
        self.assertNotIn('the aggregate budget expires first', message)
        # Lowering is offered first, raising the aggregate last and only as
        # a deliberate acceptance.
        self.assertIn('lower lock_timeout_ms', message.lower())
        # And the aggregate is not described as the TOTAL ceiling: acquired
        # locks are held through the swap that follows.
        self.assertIn('held through the swap', message)

    def test_the_boundary_case_does_not_warn(self):
        # A == n*L + M exactly satisfies the requirement.
        from task_core.db_publish import PublicationLockPolicy, staging_table_name

        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=500, acquisition_timeout_ms=1050,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        staging = staging_table_name('bsr', 'second', publisher._run_token)
        publisher._pending_swaps.append(('bsr', 'second', staging, 1))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertNoLogs(publisher.log, level='WARNING'):
            publisher.commit()   # 2*500 + 50 == 1050

    def test_an_infeasible_recommendation_says_so_instead_of_suggesting_1ms(self):
        """When the aggregate cannot accommodate even a 1ms per-conflict
        wait across n targets, no positive lock_timeout_ms satisfies
        n*L + M <= A. Clamping the recommendation to 1 would be
        recommending something that still does not fit.
        """
        from task_core.db_publish import PublicationLockPolicy, staging_table_name

        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        for index in range(120):
            table = f'extra_{index}'
            staging = staging_table_name('bsr', table, publisher._run_token)
            publisher._pending_swaps.append(('bsr', table, staging, 1))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertLogs(publisher.log, level='WARNING') as captured:
            publisher.commit()

        message = ' '.join(captured.output)
        self.assertIn('no positive lock_timeout_ms can cover', message)
        self.assertNotIn('lower lock_timeout_ms to about 1ms', message)

    def test_the_worst_case_requirement_includes_the_margin(self):
        # n x lock_timeout exactly equal to the aggregate leaves nothing for
        # execution overhead, so it must still warn.
        from task_core.db_publish import PublicationLockPolicy, staging_table_name

        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=500, acquisition_timeout_ms=1000,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        staging = staging_table_name('bsr', 'second', publisher._run_token)
        publisher._pending_swaps.append(('bsr', 'second', staging, 1))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertLogs(publisher.log, level='WARNING'):
            publisher.commit()   # 2 x 500 == 1000, but + margin does not fit

    def test_the_policy_warning_is_emitted_once_per_run(self):
        # This method runs on every retry attempt; repeating a static
        # configuration warning would bury the contention messages that
        # actually vary.
        from task_core.db_publish import PublicationLockPolicy, staging_table_name

        conn = Test27PublicationLockIsBounded._Conn(lock_failures=2)
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=500, acquisition_timeout_ms=600,
            retry_horizon_seconds=30,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        staging = staging_table_name('bsr', 'second', publisher._run_token)
        publisher._pending_swaps.append(('bsr', 'second', staging, 1))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertLogs(publisher.log, level='WARNING') as captured:
            publisher.commit()

        self.assertEqual(conn.lock_attempts, 3, 'the retries under test did not happen')
        policy_warnings = [line for line in captured.output if 'worst case' in line]
        self.assertEqual(len(policy_warnings), 1, 'the policy warning repeated per attempt')

    def test_a_well_sized_policy_does_not_warn(self):
        from task_core.db_publish import PublicationLockPolicy, staging_table_name

        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=200, acquisition_timeout_ms=5000,
            retry_delay_min_seconds=0, retry_delay_max_seconds=0,
        ))
        for table in ('second', 'third'):
            staging = staging_table_name('bsr', table, publisher._run_token)
            publisher._pending_swaps.append(('bsr', table, staging, 1))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertNoLogs(publisher.log, level='WARNING'):
            publisher.commit()

    def test_the_timeout_ordering_is_a_configuration_invariant(self):
        """PostgreSQL fires statement_timeout first once it reaches
        lock_timeout, which here converts retryable 55P03 into terminal
        57014 -- ordinary contention would end the run instead of being
        retried.
        """
        from task_core.db_publish import PublicationLockPolicy
        for acquisition in (100, 500, 549):
            with self.subTest(acquisition_timeout_ms=acquisition):
                with self.assertRaises(DbPublishError) as caught:
                    PublicationLockPolicy(lock_timeout_ms=500,
                                          acquisition_timeout_ms=acquisition)
                self.assertIn('55P03', str(caught.exception))
        PublicationLockPolicy(lock_timeout_ms=500, acquisition_timeout_ms=551)

    def test_derived_budgets_never_invert_the_ordering(self):
        # Clamping both to the same remaining budget made them EQUAL on a
        # short final attempt -- the same inversion, arrived at by
        # arithmetic instead of configuration.
        from task_core.db_publish import PublicationLockPolicy
        policy = PublicationLockPolicy()
        for remaining in (60, 5, 1, 0.6, 0.55, 0.051):
            with self.subTest(remaining=remaining):
                budgets = policy.attempt_budgets_ms(remaining)
                if budgets is None:
                    continue
                statement_ms, lock_ms = budgets
                self.assertLess(lock_ms, statement_ms)

    def test_no_attempt_is_started_after_the_deadline(self):
        from task_core.db_publish import PublicationLockPolicy
        policy = PublicationLockPolicy()
        self.assertIsNone(policy.attempt_budgets_ms(0))
        self.assertIsNone(policy.attempt_budgets_ms(-5))

    def test_source_state_work_happens_before_the_targets_are_locked(self):
        """The publication plan runs create-if-not-exists, a DELETE and an
        upsert against the source-state table. With the locks already held
        -- and both timeouts reset to zero on acquisition -- any wait in
        that work kept every live target under ACCESS EXCLUSIVE for its
        duration, recreating the outage this is meant to bound.
        """
        from task_core.db_publish import PublicationPlan

        order = []
        plan = PublicationPlan()
        plan.add('source state', lambda: order.append('plan'))

        class _Ordered(Test27PublicationLockIsBounded._Conn):
            def execute(self, statement, params=None):
                if str(statement).lower().startswith('lock table'):
                    order.append('lock')
                return super().execute(statement, params)

        conn = _Ordered()
        publisher = self._publisher(conn)
        publisher.publication_plan = plan
        publisher.commit()

        self.assertEqual(order, ['plan', 'lock'])

    def test_the_jitter_range_is_derived_from_what_remains(self):
        """Sampling first and rejecting threw the run away whenever the
        draw exceeded the remaining horizon -- with 4s left and a 1-5s
        range, a 4.5s draw ended it while a shorter delay and another
        bounded attempt would have fitted.
        """
        from task_core.db_publish import PublicationLockPolicy
        conn = Test27PublicationLockIsBounded._Conn(lock_failures=1)
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_horizon_seconds=1.0,
            retry_delay_min_seconds=0.01, retry_delay_max_seconds=30.0,
        ))

        publisher.commit()   # a 30s draw would have ended the run

        self.assertEqual(conn.lock_attempts, 2)

    def test_every_unsuccessful_attempt_leaves_no_open_transaction(self):
        """commit() caught only DBAPIError, so any other failure left the
        publication transaction OPEN -- after _publish_once() had already
        opened it, verified the staging artifacts, and possibly run the
        source-state plan. The runner's cleanup reaches rollback()
        eventually; a direct caller, or one catching the exception, was
        left holding a dirty transaction.
        """
        from task_core.db_publish import PublicationLockPolicy

        # Horizon too short to form an attempt -> DbPublishError, not DBAPI.
        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn, policy=PublicationLockPolicy(
            lock_timeout_ms=10, acquisition_timeout_ms=100,
            retry_horizon_seconds=0.001,
        ))
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertRaises(DbPublishError):
            publisher.commit()
        self.assertIsNone(publisher._tx, 'the publication transaction was left open')

    def test_a_non_dbapi_failure_is_still_terminal_and_clean(self):
        # Rolling back on everything must not turn an invariant violation
        # or an interruption into a retry.
        from task_core.db_publish import PublicationPlan

        plan = PublicationPlan()
        plan.add('boom', lambda: (_ for _ in ()).throw(KeyboardInterrupt()))

        conn = Test27PublicationLockIsBounded._Conn()
        publisher = self._publisher(conn)
        publisher.publication_plan = plan
        publisher._verify_prepared_artifacts = lambda: None

        with self.assertRaises(KeyboardInterrupt):
            publisher.commit()
        self.assertIsNone(publisher._tx)
        self.assertEqual(conn.lock_attempts, 0, 'a non-DBAPI failure was retried')

    def test_the_deadlock_message_does_not_name_the_target_locks(self):
        """The publication plan runs BEFORE locking, so a deadlock is
        reachable with zero lock attempts. Blaming the target locks would
        misdirect the investigation.
        """
        conn = Test27PublicationLockIsBounded._Conn(lock_failures=1, sqlstate='40P01')
        publisher = self._publisher(conn)

        with self.assertLogs(publisher.log, level='ERROR') as captured:
            with self.assertRaises(Exception):
                publisher.commit()

        message = ' '.join(captured.output)
        self.assertIn('deadlock', message.lower())
        self.assertNotIn('exclusive locks on these tables', message)

    def test_the_margin_boundary_matches_its_own_error_message(self):
        # The check rejected a difference of exactly the margin while the
        # message said "by at least" it.
        from task_core.db_publish import PublicationLockPolicy
        margin = PublicationLockPolicy.TIMEOUT_MARGIN_MS
        PublicationLockPolicy(lock_timeout_ms=500, acquisition_timeout_ms=500 + margin)
        with self.assertRaises(DbPublishError):
            PublicationLockPolicy(lock_timeout_ms=500, acquisition_timeout_ms=500 + margin - 1)

    def test_the_config_rejects_a_missing_policy(self):
        from task_core.db_publish import PublisherConfig
        for kwargs in ({'identifier_policy': None},
                       {'publication_lock_policy': None},
                       {'publisher_factory': 3}):
            with self.subTest(**kwargs):
                with self.assertRaises(DbPublishError):
                    PublisherConfig(**kwargs)

    def test_the_policy_rejects_non_finite_numbers(self):
        from task_core.db_publish import PublicationLockPolicy
        for field in ('retry_horizon_seconds', 'retry_delay_min_seconds',
                      'retry_delay_max_seconds'):
            for value in (float('nan'), float('inf')):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(DbPublishError):
                        PublicationLockPolicy(**{field: value})



if __name__ == '__main__':
    unittest.main()
