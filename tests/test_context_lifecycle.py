"""
Resource lifecycle and task_context guarantees. The first test here is
the most important one in this file, maybe in this whole suite: it
directly protects one of the core architectural reasons the resource
model exists at all (task_context's lazy, cached construction) -- a
resource fingerprinted during source-change checking must be the exact
same object later injected into the pipeline that processes it, not a
second, independently-constructed one (which would mean, for a file
resource, opening the same file twice; for a DB resource, a second
connection).
"""

import unittest

import task_core as tc


class InstrumentedResource:
    """Tracks construction and close() calls, so tests can assert on
    both without depending on any particular resource type's own
    internals."""

    def __init__(self, label, load_log, *, signature='sig'):
        self.label = label
        self._signature = signature
        self.close_calls = 0
        load_log.append(label)

    def source_fingerprint(self, source_key):
        return tc.source_tracking.SourceFingerprint(
            source_key=source_key, source_kind='file_set', root_path='/x',
            include_mask='*.xlsx', recursive=False, file_count=1,
            total_size_bytes=1, max_modified_at_utc=None,
            source_signature=self._signature, source_snapshot=None, store_snapshot=False,
        )

    def close(self):
        self.close_calls += 1


class Test1FingerprintAndExecutionShareOneInstance(unittest.TestCase):
    def test_same_object_fingerprinted_and_injected(self):
        load_log = []
        received = {}

        spec = tc.resource(loader=lambda env: InstrumentedResource('r', load_log), tracker=True)

        class pipeline:
            spec = tc.PipelineSpec()

            @classmethod
            def run(cls, ctx, *, source):
                received['source'] = source
                import petl as etl
                return etl.wrap([('a',), (1,)])

        resources = {'r': spec}
        pipelines = {'p': tc.bind(pipeline, source=spec)}
        run_sequence = ['p']
        env = tc.ResourceEnvironment()

        ctx = tc.build_resource_context('t', resources, pipelines, run_sequence, env)

        # Simulate the exact sequence run_pipelines() itself performs:
        # fingerprint collection first, then pipeline execution.
        fingerprints = ctx.collect_source_fingerprints()
        self.assertEqual(len(fingerprints), 1)

        kwargs = {
            alias: ctx.get_resource(ctx.resource_keys_by_spec_id[id(resource_spec)])
            for alias, resource_spec in pipelines['p'].resources.items()
        }
        pipeline.run(ctx, **kwargs)

        self.assertEqual(load_log, ['r'], 'loader was called more than once')
        self.assertIs(
            received['source'], ctx.get_resource('r'),
            'pipeline received a different object than the one used for fingerprinting',
        )

        ctx.close()
        self.assertEqual(received['source'].close_calls, 1)


class Test2ResourceClosedExactlyOnce(unittest.TestCase):
    def test_close_called_exactly_once_per_loaded_resource(self):
        load_log = []
        resource = InstrumentedResource('r', load_log)
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})

        ctx.get_resource('r')
        ctx.close()

        self.assertEqual(resource.close_calls, 1)


class Test3InactiveResourceNeitherConstructedNorClosed(unittest.TestCase):
    def test_never_requested_resource_is_never_loaded(self):
        load_log = []
        ctx = tc.task_context(
            task_name='t',
            loaders={
                'active': lambda: InstrumentedResource('active', load_log),
                'inactive': lambda: InstrumentedResource('inactive', load_log),
            },
        )

        ctx.get_resource('active')
        ctx.close()

        self.assertEqual(load_log, ['active'], 'a resource that was never requested was still constructed')


class TestAllLoadedResourcesClosedByContext(unittest.TestCase):
    # Named for what this actually tests: task_context.close()'s own
    # guarantee when multiple resources are loaded, exercised directly --
    # not a real pipeline failure through run_pipelines(), which
    # test_source_change_runner.py's test_publisher_and_context_closed_
    # on_failure already covers separately.
    def test_multiple_resources_all_closed_even_on_failure(self):
        load_log = []
        r1 = InstrumentedResource('r1', load_log)
        r2 = InstrumentedResource('r2', load_log)
        ctx = tc.task_context(task_name='t', loaders={'r1': lambda: r1, 'r2': lambda: r2})

        pipelines = {
            'p1': tc.bind(type('p1', (), {
                'spec': tc.PipelineSpec(),
                'run': classmethod(lambda cls, ctx, *, source: __import__('petl').wrap([('a',), (1,)])),
            }), source=tc.resource(loader=lambda env: r1)),
            'p2': tc.bind(type('p2', (), {
                'spec': tc.PipelineSpec(),
                'run': classmethod(lambda cls, ctx, *, source: (_ for _ in ()).throw(RuntimeError('boom'))),
            }), source=tc.resource(loader=lambda env: r2)),
        }
        # Force both into the cache directly -- this test is about
        # task_context.close()'s own guarantee, not the full binding
        # machinery already covered by test_source_change_runner.py.
        ctx.get_resource('r1')
        ctx.get_resource('r2')

        try:
            raise RuntimeError('simulated pipeline failure')
        except RuntimeError:
            pass
        finally:
            ctx.close()

        self.assertEqual(r1.close_calls, 1)
        self.assertEqual(r2.close_calls, 1)


class TestCloseAttemptsEveryResourceEvenIfOneFails(unittest.TestCase):
    """Found by external review, not here first: the original close()
    stopped at the first resource whose own close() raised, silently
    leaking every resource after it. Confirmed directly before fixing --
    a second, perfectly healthy resource was never closed at all."""

    def test_a_failing_close_does_not_prevent_other_resources_closing(self):
        class BadResource:
            def close(self):
                raise RuntimeError('this resource fails to close')

        class GoodResource:
            def __init__(self):
                self.closed = False
            def close(self):
                self.closed = True

        bad = BadResource()
        good = GoodResource()
        ctx = tc.task_context(task_name='t', loaders={'bad': lambda: bad, 'good': lambda: good})
        ctx.get_resource('bad')
        ctx.get_resource('good')

        # Standalone call, no ambient exception -- close() now correctly
        # raises the cleanup failure (see tests/test_cleanup.py for the
        # full reasoning), rather than always logging and never raising.
        # The core invariant this test exists for is unchanged: every
        # resource still gets attempted despite an earlier one failing.
        with self.assertRaises(RuntimeError):
            ctx.close()

        self.assertTrue(good.closed, 'a resource after a failing one was never closed')

    def test_close_is_idempotent(self):
        resource = InstrumentedResource('r', [])
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        ctx.close()
        ctx.close()  # must not re-attempt or raise

        self.assertEqual(resource.close_calls, 1)

    def test_same_object_under_two_loader_keys_closed_once(self):
        shared = InstrumentedResource('shared', [])
        ctx = tc.task_context(task_name='t', loaders={'alias1': lambda: shared, 'alias2': lambda: shared})
        ctx.get_resource('alias1')
        ctx.get_resource('alias2')

        ctx.close()

        self.assertEqual(shared.close_calls, 1, 'the same object under two aliases was closed more than once')


class Test5ResultAndSharedDiagnostics(unittest.TestCase):
    def test_get_result_missing_gives_clear_message(self):
        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaisesRegex(KeyError, 'published result not found'):
            ctx.get_result('nonexistent')

    def test_require_shared_missing_gives_clear_message(self):
        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaisesRegex(KeyError, 'shared runtime artifact not found'):
            ctx.require_shared('nonexistent')

    def test_has_result_and_has_shared_do_not_raise(self):
        ctx = tc.task_context(task_name='t', loaders={})
        self.assertFalse(ctx.has_result('x'))
        self.assertFalse(ctx.has_shared('x'))


class Test6UnknownResourceAccessFailsClearly(unittest.TestCase):
    def test_no_loader_registered_gives_clear_message_not_a_raw_keyerror(self):
        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaisesRegex(KeyError, 'no loader registered for resource'):
            ctx.get_resource('nonexistent')


if __name__ == '__main__':
    unittest.main()
