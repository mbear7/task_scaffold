# -*- coding: utf-8 -*-
"""
Source-change execution paths and runner transaction/failure behavior.
Both categories share the same underlying mechanism -- source-state
update and DB payload publishing happen inside the same, single
publisher.commit() at the end of run_pipelines(), so a failure anywhere
during the pipeline loop rolls back both together. Kept in one file
rather than split, matching that shared mechanism rather than fragmenting
by which module happens to implement which half.

FakeSourceStateConn exercises source_state.py's real SourceStateStore
code (ensure_table/read_state/upsert_state) against an in-memory dict, not
a second, parallel reimplementation of its logic -- verified directly
against SourceStateStore's real methods before being wired into these
tests. sqlalchemy's local sandbox stub (see /sqlalchemy/__init__.py) was
extended with a minimal text()/bindparam() for exactly this reason: without
them, SourceStateStore's own code crashes on construction, before ever
reaching the logic these tests actually care about.
"""

import unittest

import pandas as pd

import task_core as tc


class FakeSourceStateConn:
    """In-memory conn simulating just enough SQL behavior for
    SourceStateStore's real code to run against -- no real database.

    Genuinely transaction-aware, not just a dict mutated in place:
    execute() writes to pending_rows, layered over committed_rows for
    reads (matching real SQL's read-your-own-writes-within-a-transaction
    semantics). Only commit() promotes pending_rows to committed_rows;
    rollback() discards pending_rows entirely. This distinction is the
    whole point -- an earlier version of this fake mutated a single dict
    immediately on every execute(), which meant FakeDbPublisher.rollback()
    had nothing to actually undo, and no test could tell the difference
    between "rollback works" and "rollback is a no-op because nothing
    was ever staged in the first place." See
    Test4SourceStateGenuinelyRollsBack below, which exists specifically
    because this gap existed and was found by external review, not
    caught here first.
    """

    def __init__(self):
        self.committed_rows = {}  # durable state -- (task_name, source_key) -> source_signature
        self.pending_rows = None  # None = no writes this transaction; dict = staged, not yet committed
        self.table_created = False
        # Real SQLAlchemy autobegins an implicit transaction on ANY execute(),
        # read or write, and then rejects a later conn.begin() until it is
        # committed or rolled back -- confirmed directly against genuine
        # SQLAlchemy 2.0.43 on a real SQLite engine, not inferred from docs.
        # Tracked here (not just in test_db_publish.py's own fake) because
        # whether run_pipelines() clears it at the right point, between the
        # source-state read and the first publish(), is an ORDERING property
        # of the runner -- which is this file's scope -- and was previously
        # invisible to every test here.
        self.implicit_transaction_open = False

    def _current_view(self):
        if self.pending_rows is None:
            return dict(self.committed_rows)
        return dict(self.pending_rows)

    def _ensure_pending(self):
        if self.pending_rows is None:
            self.pending_rows = dict(self.committed_rows)

    def execute(self, query, params=None):
        sql = str(query).lower().strip()
        params = params or {}
        self.implicit_transaction_open = True   # autobegin, on reads too

        if 'create table' in sql:
            self.table_created = True
            return None
        if sql.startswith('select'):
            task_name = params['task_name']
            view = self._current_view()
            matching = [(sk, sig) for (tn, sk), sig in view.items() if tn == task_name]
            return _FakeResult(matching)
        if sql.startswith('delete'):
            self._ensure_pending()
            task_name = params['task_name']
            if 'keep_keys' in params:
                keep = set(params['keep_keys'])
                self.pending_rows = {
                    (tn, sk): sig for (tn, sk), sig in self.pending_rows.items()
                    if not (tn == task_name and sk not in keep)
                }
            else:
                self.pending_rows = {(tn, sk): sig for (tn, sk), sig in self.pending_rows.items() if tn != task_name}
            return None
        if sql.startswith('insert'):
            self._ensure_pending()
            self.pending_rows[(params['task_name'], params['source_key'])] = params['source_signature']
            return None
        raise AssertionError(f'FakeSourceStateConn: unrecognized query: {sql!r}')

    def commit(self):
        if self.pending_rows is not None:
            self.committed_rows = self.pending_rows
        self.pending_rows = None
        self.implicit_transaction_open = False

    def rollback(self):
        self.pending_rows = None
        self.implicit_transaction_open = False


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return [{'source_key': k, 'source_signature': v} for k, v in self._rows]


class FakeDbPublisher:
    # Records preflight rather than stubbing it: WHEN the runner invokes the
    # backend hook is an ordering property of the runner, which is this
    # file's scope. A bare no-op would hide it the way discard_pending_read()
    # was hidden.
    preflight_calls = []

    @classmethod
    def preflight(cls, specs, *, schema, **kwargs):
        cls.preflight_calls.append((sorted(specs), schema))

    """Same shape as db_publish.DbPublisher (ensure_connection/
    discard_pending_read/publish/commit/rollback/close/committed/
    committed_tables/written_tables/table_rows), plus call counters these
    tests need. ensure_connection() returns a real FakeSourceStateConn,
    not object() -- source-change tests need SourceStateStore's real code
    to actually run, not just avoid crashing on the connection handle.

    commit()/rollback() also drive self.conn's own commit()/rollback(),
    since a real DbPublisher's commit/rollback act on the one shared
    connection/transaction source-state and DB payloads both go through.

    fail_commit: if set, commit() raises instead of succeeding, once --
    simulates a real commit failure (e.g. a constraint violation) after
    source-state has already been staged via execute(), which is the
    only way to actually exercise rollback undoing a staged source-state
    write. A pipeline failing mid-loop never reaches update_source_state
    at all (it runs after the loop in runner.py), so that scenario alone
    can never prove this -- confirmed by inspecting runner.py directly,
    not assumed.

    Deliberately simpler than the real DbPublisher: this commit()/
    rollback() act unconditionally, unlike the real DbPublisher.commit(),
    which only commits when its own, explicit _tx exists -- the exact gap
    that let the real source-check-only commit bug through undetected by
    every test in this file, since none of them exercise DbPublisher's
    own commit()/rollback() logic at all, only whether run_pipelines()
    calls them at the right times. That's intentional scope, not
    permissiveness: this file tests the runner's orchestration (are
    commit/rollback called when they should be), and
    tests/test_db_publish.py separately tests DbPublisher's own,
    real commit()/rollback() implementation directly, against a fake
    SQLAlchemy connection that models the real autobegin quirk this fake
    doesn't need to. Blurring that boundary by making this fake replicate
    DbPublisher's own internals would just mean two tests checking the
    same thing two different, redundant ways."""

    def __init__(self, *, creds, schema, logger=None, fail_commit=False):
        self.creds = creds
        self.schema = schema
        self.log = logger
        self.conn = FakeSourceStateConn()
        self._written_tables = []
        self._committed_tables = []
        self._table_rows = {}
        self._committed = False
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0
        self.discard_calls = 0
        self.fail_commit = fail_commit

    def ensure_connection(self):
        return self.conn

    def discard_pending_read(self):
        # Mirrors the real DbPublisher.discard_pending_read(): with no
        # explicit transaction of its own, it rolls the connection back to
        # clear whatever the source-state read autobegan.
        self.discard_calls += 1
        self.conn.rollback()

    def publish(self, payload):
        # The real DbPublisher.publish() calls _ensure_transaction() ->
        # conn.begin(), which genuine SQLAlchemy REJECTS while an implicit
        # transaction from the source-state read is still open. Modelled
        # here at exactly that granularity -- not by replicating
        # DbPublisher's internals, but so this file can see whether the
        # RUNNER clears the pending read before publishing, which is an
        # ordering question and therefore this file's own scope.
        if self.conn.implicit_transaction_open:
            raise RuntimeError(
                'This connection has already initialized a SQLAlchemy '
                'Transaction() object via begin() or autobegin; '
                "can't call begin() here unless rollback() or commit() "
                'is called first.'
            )
        self._written_tables.append(payload)
        self._table_rows[payload.table_name] = len(payload.rows)

    def commit(self):
        self.commit_calls += 1
        if self.fail_commit:
            self.fail_commit = False  # fail once, matching a real transient commit failure
            raise RuntimeError('simulated commit failure')
        self._committed = True
        self._committed_tables = list(self._written_tables)
        self.conn.commit()
        return list(self._committed_tables)

    def rollback(self):
        self.rollback_calls += 1
        self._committed = False
        self._committed_tables = []
        self._written_tables = []
        self._table_rows = {}
        self.conn.rollback()

    def close(self):
        self.close_calls += 1

    @property
    def committed(self):
        return self._committed

    @property
    def committed_tables(self):
        return list(self._committed_tables)

    @property
    def written_tables(self):
        return list(self._written_tables)

    @property
    def table_rows(self):
        return dict(self._table_rows)


class FakeFileResource:
    """A resource with just enough shape to be a TrackedResourceSource
    target -- source_fingerprint(source_key) and close()."""

    def __init__(self, signature):
        self.signature = signature
        self.closed = False

    def source_fingerprint(self, source_key):
        return tc.source_tracking.SourceFingerprint(
            source_key=source_key,
            source_kind='file_set',
            root_path='/x',
            include_mask='*.xlsx',
            recursive=False,
            file_count=1,
            total_size_bytes=100,
            max_modified_at_utc=None,
            source_signature=self.signature,
            source_snapshot=None,
            store_snapshot=False,
        )

    def close(self):
        self.closed = True


def make_pipeline(*, db_table=None, raises=False, rows=None):
    spec = tc.PipelineSpec(db_table=db_table, table_adapter='pandas')

    class pipeline:
        pass

    pipeline.spec = spec

    def run(cls, ctx):
        if raises:
            raise RuntimeError('deliberate pipeline failure')
        return pd.DataFrame(rows or {'a': [1]})

    pipeline.run = classmethod(run)
    return pipeline


def make_context(*, resource_signature='sig-v1'):
    resource = FakeFileResource(resource_signature)
    ctx = tc.task_context(
        task_name='t',
        loaders={'files': lambda: resource},
        tracked_sources=[tc.TrackedResourceSource('files')],
    )
    return ctx, resource


class Test4SourceStateGenuinelyRollsBack(unittest.TestCase):
    """Exists because an earlier version of this file's fake conn wasn't
    actually transaction-aware -- it mutated a single dict immediately on
    every execute(), so FakeDbPublisher.rollback() had nothing to undo,
    and no test could tell "rollback works" apart from "rollback is a
    no-op because nothing was ever staged." Found by external review,
    not caught here first. This directly protects the invariant a failed
    task must never record its inputs as successfully processed."""

    def test_failed_commit_after_staging_leaves_committed_state_untouched(self):
        shared_conn = FakeSourceStateConn()
        shared_conn.committed_rows = {('t', 'files'): 'sig-v1'}  # simulates a prior, successful run

        class FailingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw, fail_commit=True)
                self.conn = shared_conn

        config = tc.SourceChangeCheckConfig(enabled=True)
        ran = []
        pipeline = make_pipeline()
        pipeline.run = classmethod(lambda cls, ctx: (ran.append(1), pd.DataFrame({'a': [1]}))[1])
        pipelines = {'p': pipeline}

        ctx, _ = make_context(resource_signature='sig-v2')  # genuinely changed vs the seeded sig-v1

        with self.assertRaises(RuntimeError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, source_change_check=config,
                publisher_factory=FailingPublisher,
            )

        self.assertEqual(len(ran), 1, 'pipeline should have run -- sources were genuinely changed')
        # The actual regression this test exists to catch: even though
        # update_source_state() genuinely staged sig-v2 (a real INSERT
        # against pending_rows, confirmed by the pipeline having run),
        # the forced commit() failure and subsequent rollback() must mean
        # the durable, committed state is still sig-v1, not sig-v2.
        self.assertEqual(
            shared_conn.committed_rows, {('t', 'files'): 'sig-v1'},
            'committed source state changed despite commit() failing -- rollback did not undo the staged write',
        )
        self.assertIsNone(shared_conn.pending_rows, 'rollback did not clear the pending transaction')


class FakeFailingFingerprintResource:
    """A resource whose source_fingerprint() itself raises -- for testing
    cleanup when fingerprint collection fails partway through, before any
    pipeline has even started."""

    def __init__(self):
        self.closed = False

    def source_fingerprint(self, source_key):
        raise RuntimeError('deliberate fingerprint failure')

    def close(self):
        self.closed = True


class Test5FingerprintCollectionFailureCleanup(unittest.TestCase):
    """collect_source_fingerprints() (runner.py, well before any pipeline
    runs and before build_source_state_store() is ever constructed) is a
    genuinely earlier failure point than anything else in this file
    exercises -- every other test here fails either during the pipeline
    loop or at the final commit(). The runner's exception handling is
    generic (one try/except/finally covering the whole function), so this
    should already work by construction, but that had never actually been
    proven with a test until now."""

    def test_second_resource_fingerprint_failure_cleans_up_correctly(self):
        good_resource = FakeFileResource('sig-ok')
        bad_resource = FakeFailingFingerprintResource()

        ctx = tc.task_context(
            task_name='t',
            loaders={'good': lambda: good_resource, 'bad': lambda: bad_resource},
            tracked_sources=[tc.TrackedResourceSource('good'), tc.TrackedResourceSource('bad')],
        )

        config = tc.SourceChangeCheckConfig(enabled=True)
        ran = []
        pipeline = make_pipeline()
        pipeline.run = classmethod(lambda cls, ctx: (ran.append(1), pd.DataFrame({'a': [1]}))[1])
        pipelines = {'p': pipeline}

        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        with self.assertRaises(RuntimeError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, source_change_check=config,
                publisher_factory=TrackingPublisher,
            )

        self.assertEqual(len(ran), 0, 'a pipeline ran despite fingerprint collection failing')

        pub = published[0]
        self.assertEqual(pub.rollback_calls, 1, 'publisher.rollback() was not called')
        self.assertEqual(pub.close_calls, 1, 'publisher.close() was not called')

        self.assertTrue(good_resource.closed, 'the already-loaded good resource was not closed')
        self.assertTrue(bad_resource.closed, 'the already-loaded bad resource was not closed')

        # Structural, not just empirically absent: collect_source_
        # fingerprints() happens before build_source_state_store() is
        # ever constructed (confirmed by reading runner.py directly), so
        # nothing here could have touched source state at all. Asserting
        # the fake conn genuinely was never written to, not just that it
        # stayed empty by coincidence.
        self.assertIsNone(pub.conn.pending_rows)
        self.assertEqual(pub.conn.committed_rows, {})


class Test1SourceUnchangedSkipsExecution(unittest.TestCase):
    def test_unchanged_sources_skip_pipeline_execution(self):
        config = tc.SourceChangeCheckConfig(enabled=True)
        pipelines = {'p': make_pipeline()}
        ran = []
        pipelines['p'].run = classmethod(lambda cls, ctx: (ran.append(1), pd.DataFrame({'a': [1]}))[1])

        # First run: nothing stored yet, so sources are "changed" (no prior state) -- runs and persists.
        ctx1, _ = make_context()
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx1, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=FakeDbPublisher,
        )
        self.assertEqual(len(ran), 1)

        # Second run: same signature as what was just persisted -- must be skipped.
        # Reuses the same FakeSourceStateConn's rows by publishing through
        # a fresh publisher whose conn was seeded identically, since
        # run_pipelines doesn't expose the publisher instance directly.
        shared_conn = FakeSourceStateConn()

        class SeededPublisher(FakeDbPublisher):
            def ensure_connection(self):
                return shared_conn

        ctx1b, _ = make_context()
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx1b, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=SeededPublisher,
        )
        ran.clear()

        ctx2, _ = make_context()  # same default signature -- 'sig-v1'
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx2, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=SeededPublisher,
        )
        self.assertEqual(len(ran), 0, 'pipeline ran despite unchanged sources')
        self.assertTrue(result.skipped)
        self.assertEqual(result.skip_reason, 'sources_unchanged')

    def test_unchanged_sources_with_force_run_still_executes(self):
        shared_conn = FakeSourceStateConn()

        class SeededPublisher(FakeDbPublisher):
            def ensure_connection(self):
                return shared_conn

        config = tc.SourceChangeCheckConfig(enabled=True)
        ran = []
        pipeline = make_pipeline()
        pipeline.run = classmethod(lambda cls, ctx: (ran.append(1), pd.DataFrame({'a': [1]}))[1])
        pipelines = {'p': pipeline}

        ctx1, _ = make_context()
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx1, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=SeededPublisher,
        )
        ran.clear()

        ctx2, _ = make_context()  # unchanged signature
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx2, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            force_run=True, publisher_factory=SeededPublisher,
        )
        self.assertEqual(len(ran), 1, 'force_run=True did not run despite unchanged sources')
        self.assertFalse(result.skipped)

    def test_changed_sources_execute(self):
        shared_conn = FakeSourceStateConn()

        class SeededPublisher(FakeDbPublisher):
            def ensure_connection(self):
                return shared_conn

        config = tc.SourceChangeCheckConfig(enabled=True)
        ran = []
        pipeline = make_pipeline()
        pipeline.run = classmethod(lambda cls, ctx: (ran.append(1), pd.DataFrame({'a': [1]}))[1])
        pipelines = {'p': pipeline}

        ctx1, _ = make_context(resource_signature='sig-v1')
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx1, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=SeededPublisher,
        )
        ran.clear()

        ctx2, _ = make_context(resource_signature='sig-v2')  # genuinely changed
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx2, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=SeededPublisher,
        )
        self.assertEqual(len(ran), 1, 'changed sources did not trigger a run')
        self.assertFalse(result.skipped)
        self.assertTrue(result.source_changed)


class Test2SourceCheckWithNoTrackedSources(unittest.TestCase):
    def test_enabled_with_no_tracked_sources_fails_clearly(self):
        config = tc.SourceChangeCheckConfig(enabled=True)
        ctx = tc.task_context(task_name='t', loaders={}, tracked_sources=[])
        pipelines = {'p': make_pipeline()}

        with self.assertRaises(tc.SourceCheckError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, source_change_check=config,
                publisher_factory=FakeDbPublisher,
            )


class Test3TransactionAtomicity(unittest.TestCase):
    def test_two_db_output_pipelines_succeed_one_commit(self):
        pipelines = {
            'p1': make_pipeline(db_table='t1'),
            'p2': make_pipeline(db_table='t2'),
        }
        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        ctx = tc.task_context(task_name='t', loaders={})
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p1', 'p2'],
            output_excel=False, output_db=True, creds={}, publisher_factory=TrackingPublisher,
        )
        self.assertEqual(len(published), 1)
        pub = published[0]
        self.assertEqual(pub.commit_calls, 1)
        self.assertEqual(pub.rollback_calls, 0)
        self.assertEqual(len(pub.committed_tables), 2)

    def test_second_pipeline_failure_rolls_back_first_publish_too(self):
        pipelines = {
            'p1': make_pipeline(db_table='t1'),
            'p2': make_pipeline(db_table='t2', raises=True),
        }
        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaises(tc.PipelineError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p1', 'p2'],
                output_excel=False, output_db=True, creds={}, publisher_factory=TrackingPublisher,
            )
        pub = published[0]
        self.assertEqual(pub.commit_calls, 0, 'commit() was called despite the second pipeline failing')
        self.assertEqual(pub.rollback_calls, 1)
        self.assertEqual(pub.committed_tables, [], 'first pipeline\'s publish survived the rollback')

    def test_excel_only_task_creates_no_db_publisher(self):
        pipelines = {'p': make_pipeline()}  # no db_table
        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        ctx = tc.task_context(task_name='t', loaders={})
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, publisher_factory=TrackingPublisher,
        )
        self.assertEqual(len(published), 0, 'a DbPublisher was created despite no db-output pipeline')

    def test_source_check_only_task_creates_one_publisher(self):
        # No pipeline has a DB output table, but output_db=True activates
        # the technical source-state mechanism -- must still create
        # exactly one publisher (to read/write the source-state table),
        # not skip creating one entirely.
        config = tc.SourceChangeCheckConfig(enabled=True)
        pipelines = {'p': make_pipeline()}
        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        ctx, _ = make_context()
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_factory=TrackingPublisher,
        )
        self.assertEqual(len(published), 1)

    def test_publisher_and_context_closed_on_success(self):
        pipelines = {'p': make_pipeline(db_table='t1')}
        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        resource = FakeFileResource('sig')
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')  # force it into the cache so close() has something to close
        tc.run_pipelines(
            task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, publisher_factory=TrackingPublisher,
        )
        self.assertEqual(published[0].close_calls, 1)
        self.assertTrue(resource.closed)

    def test_publisher_and_context_closed_on_failure(self):
        pipelines = {'p': make_pipeline(db_table='t1', raises=True)}
        published = []

        class TrackingPublisher(FakeDbPublisher):
            def __init__(self, **kw):
                super().__init__(**kw)
                published.append(self)

        resource = FakeFileResource('sig')
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')
        with self.assertRaises(tc.PipelineError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, publisher_factory=TrackingPublisher,
            )
        self.assertEqual(published[0].close_calls, 1, 'publisher.close() was not called on failure')
        self.assertTrue(resource.closed, 'ctx.close() did not close the resource on failure')

    def test_publisher_close_failure_does_not_mask_the_real_pipeline_failure(self):
        # Found by external review, confirmed directly before fixing: a
        # pipeline's real ValueError was being completely replaced by an
        # unrelated "publisher close failed" RuntimeError, with no trace
        # of the actual cause anywhere in what propagated.
        class CloseFailingPublisher(FakeDbPublisher):
            def close(self):
                raise RuntimeError('simulated publisher close failure')

        pipelines = {'p': make_pipeline(db_table='t1', raises=True)}
        resource = FakeFileResource('sig')
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        with self.assertRaises(tc.PipelineError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines=pipelines, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, publisher_factory=CloseFailingPublisher,
            )

        self.assertIn('failed during pipeline execution', str(cm.exception))
        self.assertTrue(resource.closed, 'ctx.close() did not run despite publisher.close() also failing')


class Test6RunnerClearsThePendingSourceStateReadBeforePublishing(unittest.TestCase):
    """run_pipelines() must call publisher.discard_pending_read() between
    the source-state read and the pipeline loop. Nothing tested that
    ordering, in this file or any other -- confirmed directly by deleting
    the call from runner.py and watching all 217 tests pass.

    Why it matters: SourceStateStore's ensure_table()/read_state() go
    through publisher.ensure_connection(), never through
    _ensure_transaction(), so a source-check-enabled run arrives at the
    pipeline loop with an implicit transaction already autobegun on the
    connection. The first publish() then calls _ensure_transaction() ->
    conn.begin(), which genuine SQLAlchemy 2.0.43 rejects outright
    (InvalidRequestError, confirmed directly against a real SQLite
    engine). Without the discard, every source-check-enabled run that also
    publishes a table fails on its first publish, in production,
    immediately -- while the test suite stayed green.

    This lives here rather than in tests/test_db_publish.py deliberately.
    That file now covers DbPublisher's own discard/begin mechanics
    (Test9), and those passed even with the runner's call deleted, because
    the mechanism working in isolation says nothing about whether the
    runner invokes it at the right moment -- the same distinction already
    recorded for stabilize() in the README, and the same mistake made
    again here before being caught.
    """

    def test_a_publish_after_the_source_state_read_succeeds(self):
        # The end-to-end property. Fails with InvalidRequestError-shaped
        # RuntimeError if runner.py stops discarding the pending read.
        publishers = []

        def factory(**kwargs):
            publisher = FakeDbPublisher(**kwargs)
            publishers.append(publisher)
            return publisher

        result = tc.run_pipelines(
            task_name='discard_ordering',
            build_context=lambda: make_context()[0],
            pipelines={'p': make_pipeline(db_table='out_tbl')},
            run_sequence=['p'],
            output_excel=False,
            output_db=True,
            creds={'user': 'x', 'host': 'x', 'dbname': 'x'},
            source_change_check=tc.SourceChangeCheckConfig(enabled=True),
            publisher_factory=factory,
        )

        self.assertFalse(result.skipped)
        self.assertTrue(result.db_committed)
        self.assertEqual(len(publishers), 1)
        self.assertEqual(publishers[0].discard_calls, 1)
        self.assertEqual([p.table_name for p in publishers[0].written_tables], ['out_tbl'])

    def test_the_discard_happens_before_the_first_publish_not_after(self):
        # Ordering specifically, not merely "was it called at some point":
        # a discard issued after the loop would leave publish() broken and
        # additionally throw away the staged source-state write.
        order = []

        class OrderRecordingPublisher(FakeDbPublisher):
            def discard_pending_read(self):
                order.append('discard')
                super().discard_pending_read()

            def publish(self, payload):
                order.append('publish')
                super().publish(payload)

        tc.run_pipelines(
            task_name='discard_ordering',
            build_context=lambda: make_context()[0],
            pipelines={'p': make_pipeline(db_table='out_tbl')},
            run_sequence=['p'],
            output_excel=False,
            output_db=True,
            creds={'user': 'x', 'host': 'x', 'dbname': 'x'},
            source_change_check=tc.SourceChangeCheckConfig(enabled=True),
            publisher_factory=OrderRecordingPublisher,
        )

        self.assertEqual(order, ['discard', 'publish'])



if __name__ == '__main__':
    unittest.main()
