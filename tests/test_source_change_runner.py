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
    SourceStateStore's real code to run against -- no real database."""

    def __init__(self):
        self.rows = {}  # (task_name, source_key) -> source_signature
        self.table_created = False

    def execute(self, query, params=None):
        sql = str(query).lower().strip()
        params = params or {}

        if 'create table' in sql:
            self.table_created = True
            return None
        if sql.startswith('select'):
            task_name = params['task_name']
            matching = [(sk, sig) for (tn, sk), sig in self.rows.items() if tn == task_name]
            return _FakeResult(matching)
        if sql.startswith('delete'):
            task_name = params['task_name']
            if 'keep_keys' in params:
                keep = set(params['keep_keys'])
                self.rows = {
                    (tn, sk): sig for (tn, sk), sig in self.rows.items()
                    if not (tn == task_name and sk not in keep)
                }
            else:
                self.rows = {(tn, sk): sig for (tn, sk), sig in self.rows.items() if tn != task_name}
            return None
        if sql.startswith('insert'):
            self.rows[(params['task_name'], params['source_key'])] = params['source_signature']
            return None
        raise AssertionError(f'FakeSourceStateConn: unrecognized query: {sql!r}')


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return [{'source_key': k, 'source_signature': v} for k, v in self._rows]


class FakeDbPublisher:
    """Same shape as db_publish.DbPublisher (ensure_connection/
    discard_pending_read/publish/commit/rollback/close/committed/
    committed_tables/written_tables/table_rows), plus call counters these
    tests need. ensure_connection() returns a real FakeSourceStateConn,
    not object() -- source-change tests need SourceStateStore's real code
    to actually run, not just avoid crashing on the connection handle."""

    def __init__(self, *, creds, schema, logger=None):
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

    def ensure_connection(self):
        return self.conn

    def discard_pending_read(self):
        pass

    def publish(self, payload):
        self._written_tables.append(payload)
        self._table_rows[payload.table_name] = len(payload.rows)

    def commit(self):
        self.commit_calls += 1
        self._committed = True
        self._committed_tables = list(self._written_tables)
        return list(self._committed_tables)

    def rollback(self):
        self.rollback_calls += 1
        self._committed = False
        self._committed_tables = []
        self._written_tables = []
        self._table_rows = {}

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
        # output_db=False -- no table has db_table, but source checking
        # alone must still create exactly one publisher (to read/write the
        # technical source-state table), not skip creating one entirely.
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


if __name__ == '__main__':
    unittest.main()
