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

import logging
import unittest

import pandas as pd

import task_core as tc
from task_core.source_state import SourceStateStore
from task_core.types import SourceCheckError


class _FakeDialect:
    name = 'sqlite'


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
        # Real SQLAlchemy connections carry a dialect, and SourceStateStore
        # now asks for it to decide whether to read the server's real
        # max_identifier_length. Modelled as a non-PostgreSQL dialect so
        # that check takes its documented fallback rather than trying to
        # execute SHOW against this fake.
        self.dialect = _FakeDialect()

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

    def in_transaction(self):
        # Real SQLAlchemy Connections have this, and SourceStateStore now
        # calls it to close its own read phase. The earlier version of this
        # fake lacked it and the resulting AttributeError was swallowed by
        # the store's catch-all -- which is how a failed read-phase commit
        # could report a successful comparison.
        return self.implicit_transaction_open

    def commit(self):
        if self.pending_rows is not None:
            self.committed_rows = self.pending_rows
        self.pending_rows = None
        self.implicit_transaction_open = False
        # Real SQLAlchemy connections carry a dialect, and SourceStateStore
        # now asks for it to decide whether to read the server's real
        # max_identifier_length. Modelled as a non-PostgreSQL dialect so
        # that check takes its documented fallback rather than trying to
        # execute SHOW against this fake.
        self.dialect = _FakeDialect()

    def rollback(self):
        self.pending_rows = None
        self.implicit_transaction_open = False
        # Real SQLAlchemy connections carry a dialect, and SourceStateStore
        # now asks for it to decide whether to read the server's real
        # max_identifier_length. Modelled as a non-PostgreSQL dialect so
        # that check takes its documented fallback rather than trying to
        # execute SHOW against this fake.
        self.dialect = _FakeDialect()


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

    def __init__(self, *, creds, schema, logger=None, fail_commit=False, **kwargs):
        self.publication_plan = kwargs.get('publication_plan')
        self.identifier_policy = kwargs.get('identifier_policy')
        self.task_name = kwargs.get('task_name')
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

    def begin_run(self):

        return True
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
        # Under the staged model a pending read is COMMITTED rather than
        # rejected: the source-state read is its own bounded phase, and
        # _ensure_transaction() closes it out before opening an explicit
        # transaction. The earlier version of this fake raised here,
        # modelling the contract discard_pending_read() used to enforce --
        # a lifecycle state the architecture no longer has.
        if self.conn.implicit_transaction_open:
            self.conn.commit()
        self._written_tables.append(payload)
        self._table_rows[payload.table_name] = len(payload.rows)

    def commit(self):
        self.commit_calls += 1
        # The publication plan is part of what commit() MEANS under the
        # staged model: the runner queues the source-state write there so
        # it lands in the same transaction as the swaps. A fake that
        # ignored the plan would make "did the queued work actually run"
        # invisible -- and that is an ordering property of the runner,
        # which is this file's scope.
        if self.publication_plan is not None:
            self.publication_plan.run(self.log or logging.getLogger(__name__))
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
                publisher_config=tc.PublisherConfig(publisher_factory=FailingPublisher),
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
                publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
            publisher_config=tc.PublisherConfig(publisher_factory=FakeDbPublisher),
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
            publisher_config=tc.PublisherConfig(publisher_factory=SeededPublisher),
        )
        ran.clear()

        ctx2, _ = make_context()  # same default signature -- 'sig-v1'
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx2, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_config=tc.PublisherConfig(publisher_factory=SeededPublisher),
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
            publisher_config=tc.PublisherConfig(publisher_factory=SeededPublisher),
        )
        ran.clear()

        ctx2, _ = make_context()  # unchanged signature
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx2, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            force_run=True, publisher_config=tc.PublisherConfig(publisher_factory=SeededPublisher),
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
            publisher_config=tc.PublisherConfig(publisher_factory=SeededPublisher),
        )
        ran.clear()

        ctx2, _ = make_context(resource_signature='sig-v2')  # genuinely changed
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx2, pipelines=pipelines, run_sequence=['p'],
            output_excel=False, output_db=True, creds={}, source_change_check=config,
            publisher_config=tc.PublisherConfig(publisher_factory=SeededPublisher),
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
                publisher_config=tc.PublisherConfig(publisher_factory=FakeDbPublisher),
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
            output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
                output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
            output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
            publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
            output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
                output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=TrackingPublisher),
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
                output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=CloseFailingPublisher),
            )

        self.assertIn('failed during pipeline execution', str(cm.exception))
        self.assertTrue(resource.closed, 'ctx.close() did not run despite publisher.close() also failing')


class Test7SourceStateTableIsValidatedBeforeItIsUsed(unittest.TestCase):
    """The source-state table is a real PostgreSQL table this run creates
    and writes, and it was outside both tiers of identifier validation at
    runtime.

    Preflight covers its declared names, but against a configured default
    with no connection available. NAMEDATALEN is compile-time
    configurable, so on a server with a lower limit that default can
    accept a name PostgreSQL then silently truncates. And the resolution
    of the server's real limit used to happen at the first publish() --
    which a source-check-only run never reaches, so such a run never
    verified the limit at all.

    Checked in SourceStateStore rather than through a new publisher
    method: publisher_factory is an advertised extension seam and has
    already been expanded once by accident. The store already receives the
    connection it needs.
    """

    class _Dialect:
        name = 'postgresql'

    class _Conn:
        def __init__(self, limit=63, columns=None, fail_show=False):
            self.dialect = Test7SourceStateTableIsValidatedBeforeItIsUsed._Dialect()
            self._limit = limit
            self._columns = columns
            self._fail_show = fail_show
            self.statements = []

        def execute(self, statement, params=None):
            text = str(statement).lower()
            self.statements.append(text.strip().split()[0] + (' ' + text.strip().split()[1] if len(text.strip().split()) > 1 else ''))

            if 'max_identifier_length' in text:
                if self._fail_show:
                    raise RuntimeError('server went away')
                return _Scalar(self._limit)
            if 'information_schema.columns' in text:
                return [(name,) for name in (self._columns or [])]
            return None

    def test_the_server_limit_is_read_before_any_source_state_ddl(self):
        conn = self._Conn()
        store = SourceStateStore(conn, schema='bsr', table='task_scaffold_meta')
        # Construction alone must have asked, before ensure_table() runs.
        self.assertTrue(any('show max_identifier_length' in s for s in conn.statements),
                        f'server limit never read; statements were {conn.statements}')
        self.assertFalse(any(s.startswith('create') for s in conn.statements),
                         'DDL ran before the limit was verified')

    def test_a_name_over_the_real_server_limit_is_rejected(self):
        # Accepted by preflight's configured default of 63, rejected here
        # against a server that reports 32.
        conn = self._Conn(limit=32)
        with self.assertRaises(SourceCheckError):
            SourceStateStore(conn, schema='bsr', table='a' * 40)

    def test_a_failure_reading_the_limit_is_not_swallowed(self):
        conn = self._Conn(fail_show=True)
        with self.assertRaises(SourceCheckError):
            SourceStateStore(conn, schema='bsr', table='task_scaffold_meta')

    def test_an_existing_table_missing_columns_fails_at_startup(self):
        """ensure_table() uses `create table if not exists`, so a table
        left by an older version with a different shape was accepted
        silently and then failed at the first upsert_state() -- mid-run,
        after every pipeline had already executed. A startup error naming
        the missing columns is a far cheaper failure.
        """
        conn = self._Conn(columns=['task_name', 'source_key', 'source_signature'])
        store = SourceStateStore(conn, schema='bsr', table='task_scaffold_meta')

        with self.assertRaises(SourceCheckError) as caught:
            store.ensure_table()

        message = str(caught.exception)
        self.assertIn('older version', message)
        self.assertIn('source_kind', message)

    def test_a_complete_existing_table_is_accepted(self):
        complete = ['task_name', 'source_key', 'source_kind', 'root_path', 'include_mask',
                    'recursive', 'file_count', 'total_size_bytes', 'max_modified_at_utc',
                    'source_signature', 'source_snapshot', 'processed_at_utc']
        conn = self._Conn(columns=complete)
        store = SourceStateStore(conn, schema='bsr', table='task_scaffold_meta')
        store.ensure_table()

    def test_an_inspection_failure_on_postgres_is_not_swallowed(self):
        """The original catch-all covered backends without
        information_schema, but also swallowed permission failures,
        connection failures, and any future incompatibility in the query --
        and in each case the fail-early guarantee silently disappeared
        while the run went on to fail later at the upsert anyway.
        """
        class _FailingInspection(Test7SourceStateTableIsValidatedBeforeItIsUsed._Conn):
            def execute(self, statement, params=None):
                text = str(statement).lower()
                if 'information_schema.columns' in text:
                    raise RuntimeError('permission denied for schema information_schema')
                return super().execute(statement, params)

        store = SourceStateStore(_FailingInspection(), schema='bsr', table='task_scaffold_meta')
        with self.assertRaises(SourceCheckError) as caught:
            store.ensure_table()
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)

    def test_a_non_postgres_backend_skips_inspection_without_failing(self):
        class _Sqlite(Test7SourceStateTableIsValidatedBeforeItIsUsed._Conn):
            def __init__(self):
                super().__init__()
                self.dialect = type('D', (), {'name': 'sqlite'})()

        store = SourceStateStore(_Sqlite(), schema='bsr', table='task_scaffold_meta')
        store.ensure_table()

    def test_an_uninspectable_table_is_refused(self):
        """An empty information_schema result here is anomalous, not
        benign.

        This test previously asserted the opposite, on the same false
        premise as the comment it was written from: that the table might
        not exist yet. It cannot -- this runs immediately after
        `create table if not exists` on the same connection, so PostgreSQL
        makes the new table's columns visible to this very query. An empty
        result means the check cannot see the table it is about to write
        to, which is precisely what it exists to refuse: an
        identifier-case mismatch against information_schema's stored
        values, or a search_path oddity.
        """
        conn = self._Conn(columns=[])
        store = SourceStateStore(conn, schema='bsr', table='task_scaffold_meta')

        with self.assertRaises(SourceCheckError) as caught:
            store.ensure_table()
        self.assertIn('cannot be inspected', str(caught.exception))


class _Scalar:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value



class Test8RunnerSkipsWhenAnotherRunHoldsTheLock(unittest.TestCase):
    """run_pipelines() claims the task before doing anything expensive,
    and treats losing that race as a SKIP rather than an error.

    A cron overlap is expected operation, not a failure: it should compose
    with the existing sources-unchanged skip instead of paging someone
    every time a long run overlaps its own schedule. It is logged at
    WARNING all the same, because chronic overlap means the schedule is
    wrong even when each individual skip is correct.

    The ordering is the property under test, not the mechanism -- which is
    why this lives here rather than in tests/test_db_publish.py. Testing
    begin_run() in isolation says nothing about whether the runner calls
    it before fingerprinting, and fingerprinting is the expensive part on
    a remote share.
    """

    def _pipeline(self, ran):
        class pipeline:
            spec = tc.PipelineSpec(db_table='out_tbl')

            @classmethod
            def run(cls, ctx):
                ran.append('ran')
                import petl as etl
                return etl.wrap([('v',), ('x',)])

        return pipeline

    def _run(self, publisher_factory, ran):
        ctx, _resources = make_context()
        return tc.run_pipelines(
            task_name='t',
            build_context=lambda: ctx,
            pipelines={'p': self._pipeline(ran)},
            run_sequence=['p'],
            output_excel=False, output_db=True, creds={},
            source_change_check=tc.SourceChangeCheckConfig(
                enabled=True, schema='bsr', table='task_scaffold_meta',
            ),
            publisher_config=tc.PublisherConfig(publisher_factory=publisher_factory),
        )

    def test_a_lost_race_skips_without_running_any_pipeline(self):
        ran = []

        class Contended(FakeDbPublisher):
            def begin_run(self):
                return False

        result = self._run(Contended, ran)

        self.assertTrue(result.skipped)
        self.assertEqual(
            result.skip_reason,
            'task_already_running',
        )
        self.assertEqual(ran, [], 'a pipeline ran while another run held the lock')

    def test_the_claim_happens_before_fingerprints_are_collected(self):
        # Fingerprinting is the expensive part on a remote share. Claiming
        # after it would mean paying for it and then losing anyway -- and
        # update_source_state() WRITES, so two runs past the read are both
        # intending to write the same rows.
        order = []

        class Recording(FakeDbPublisher):
            def begin_run(self):
                order.append('begin_run')
                return True

        original = tc.task_context.collect_source_fingerprints

        def recording_collect(self):
            order.append('collect_fingerprints')
            return original(self)

        tc.task_context.collect_source_fingerprints = recording_collect
        self.addCleanup(
            setattr, tc.task_context, 'collect_source_fingerprints', original,
        )

        self._run(Recording, [])

        self.assertEqual(order[:2], ['begin_run', 'collect_fingerprints'])

    def test_a_skipped_run_still_closes_the_publisher(self):
        # It holds no lock, but it does hold a connection.
        publishers = []

        class Contended(FakeDbPublisher):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                publishers.append(self)

            def begin_run(self):
                return False

        self._run(Contended, [])

        self.assertEqual(len(publishers), 1)
        self.assertEqual(publishers[0].close_calls, 1, 'the publisher was left open')



if __name__ == '__main__':
    unittest.main()
