# -*- coding: utf-8 -*-
"""
Resource-management failure modes: a resource's own close() raising,
publisher.close() raising, two loader keys resolving to the same object,
rollback() itself raising, a closed context still constructing new
resources, and -- the most important of these -- a cleanup failure
while a pipeline exception is already active, versus a cleanup failure
when the task itself succeeded.

This file's design went through two rounds of external review, not
caught here first either time.

Round 1 found: task_context.close() and run_pipelines()'s publisher.close()
step both always logged cleanup failures and never raised, which
correctly avoided masking a real pipeline failure but also meant a
cleanup failure on an otherwise-successful run was silently, permanently
invisible.

Round 1's fix used sys.exc_info() to decide whether something was
already failing. Round 2 found that this was itself unreliable: it
detects any exception currently being handled anywhere up the call
stack, not specifically a failure from *this* run_pipelines() call.
Confirmed directly: a caller of run_pipelines() sitting inside its own,
unrelated except: block (`except ValueError: run_pipelines(...)`) makes
sys.exc_info() non-None for the call's entire duration, even when the
task itself completes with no error at all -- a resource cleanup
failure during that genuinely-successful task incorrectly looked like
it had something ambient to avoid masking, and got logged instead of
raised, silently hiding a real, leaked resource (Test6 below is exactly
this reproduction).

There is no reliable way to infer "does *this task* have a primary
failure" from interpreter state -- only run_pipelines()'s own try/except
genuinely knows. The current design (task_core/cleanup.py,
task_core/runner.py) tracks this explicitly instead: cleanup.py's
attempt_all_cleanup() always attempts every item and always raises
collected failures, with no suppress/log parameters of its own at all --
task_context.close() uses it this way, meaning close() itself always
raises on failure too. run_pipelines() is the one place that actually
tracks primary_error explicitly (set inside its own except: block, never
inferred), and decides what to do with whatever close()/rollback()
raise: log it (with that failure's own, correct traceback, attached via
exc_info=(type(e), e, e.__traceback__) -- not a plain log.exception()
call outside the except: block that actually caught it, which would
read sys.exc_info() at the *logging* call's own point and log whatever's
currently ambient there instead, silently losing the real cleanup
exception from diagnostics; Test7 below is exactly this reproduction)
if a primary_error already exists, or let it raise if nothing else did.

Every test here runs through the real run_pipelines()/task_context.close()
code, not isolated mocks of the cleanup logic itself -- small fake
resources are fine, but the lifecycle code under test is real.
"""

import io
import logging
import unittest

import pandas as pd

import task_core as tc


class RecordingResource:
    def __init__(self, label, log, *, raises=None):
        self.label = label
        self._log = log
        self._raises = raises
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        self._log.append(self.label)
        if self._raises is not None:
            raise self._raises


class FakePublisher:
    @classmethod
    def preflight(cls, specs, *, schema, **kwargs):
        pass

    def __init__(self, *, creds, schema, logger=None, close_error=None, commit_error=None, rollback_error=None, **kwargs):
        self._close_error = close_error
        self._commit_error = commit_error
        self._rollback_error = rollback_error
        self.close_calls = 0
        self.rollback_calls = 0
        self.log = logger or logging.getLogger('fake_publisher')

    def ensure_connection(self):
        return object()

    def begin_run(self):

        return True
        pass

    def publish(self, payload):
        pass

    def commit(self):
        if self._commit_error is not None:
            raise self._commit_error
        return []

    def rollback(self):
        self.rollback_calls += 1
        if self._rollback_error is not None:
            raise self._rollback_error

    def close(self):
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error

    committed = property(lambda self: True)
    committed_tables = property(lambda self: [])
    written_tables = property(lambda self: [])
    table_rows = property(lambda self: {})


class _CapturedLog:
    """Captures a named logger's output into a plain string, so a test
    can inspect not just whether something was logged, but whether the
    specific traceback content attached to it is correct -- the actual
    bug in Point 2 (a wrong-but-present traceback) would pass any test
    that only checked "something got logged"."""

    def __init__(self, logger_name):
        self._logger_name = logger_name
        self._stream = io.StringIO()
        self._handler = logging.StreamHandler(self._stream)
        self._handler.setFormatter(logging.Formatter('%(message)s'))
        self._handler.setLevel(logging.DEBUG)

    def __enter__(self):
        self._logger = logging.getLogger(self._logger_name)
        self._original_handlers = self._logger.handlers
        self._original_propagate = self._logger.propagate
        self._logger.handlers = [self._handler]
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False
        return self

    def __exit__(self, *exc):
        self._logger.handlers = self._original_handlers
        self._logger.propagate = self._original_propagate
        return False

    @property
    def text(self):
        self._handler.flush()
        return self._stream.getvalue()


def make_pandas_pipeline(*, db_table=None, raises=None):
    class pipeline:
        spec = tc.PipelineSpec(db_table=db_table, table_adapter='pandas')

        @classmethod
        def run(cls, ctx):
            if raises is not None:
                raise raises
            return pd.DataFrame({'a': [1]})

    return pipeline


class Test1AllResourcesAttemptedEvenWhenOneCloseFails(unittest.TestCase):
    def test_all_resources_receive_close_attempt_when_one_close_fails(self):
        log = []
        bad = RecordingResource('bad', log, raises=RuntimeError('bad fails'))
        good = RecordingResource('good', log)

        ctx = tc.task_context(task_name='t', loaders={'bad': lambda: bad, 'good': lambda: good})
        ctx.get_resource('bad')
        ctx.get_resource('good')

        with self.assertRaises(RuntimeError):
            ctx.close()  # task itself succeeded (this is standalone) -- cleanup failure must surface

        self.assertEqual(good.close_calls, 1, 'a resource after a failing one was never attempted')
        self.assertIn('good', log)


class Test2SameObjectClosedOnceUnderTwoKeys(unittest.TestCase):
    def test_same_resource_object_is_closed_once_when_cached_under_two_keys(self):
        log = []
        shared = RecordingResource('shared', log)

        ctx = tc.task_context(task_name='t', loaders={'first': lambda: shared, 'second': lambda: shared})
        ctx.get_resource('first')
        ctx.get_resource('second')
        ctx.close()

        self.assertEqual(shared.close_calls, 1)


class Test3ContextClosedWhenPublisherCloseRaises(unittest.TestCase):
    def test_context_is_closed_when_publisher_close_raises(self):
        log = []
        resource = RecordingResource('r', log)
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline(db_table='t1')
        with self.assertRaises(OSError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=True, creds={},
                publisher_config=tc.PublisherConfig(
                    publisher_factory=lambda **kw: FakePublisher(**kw, close_error=OSError('SMB close failed')),
                ),
            )

        self.assertEqual(resource.close_calls, 1, 'ctx.close() did not run when publisher.close() raised')


class Test4PipelineErrorRemainsPrimaryWhenCleanupAlsoFails(unittest.TestCase):
    def test_pipeline_error_remains_primary_when_cleanup_also_fails(self):
        # Identity/message-level assertion, not just "some exception" --
        # specifically requested, since a type-only check could pass even
        # if the wrong RuntimeError (e.g. a masking cleanup failure that
        # happens to also be a RuntimeError) were the one that actually
        # propagated.
        pipeline_error = RuntimeError('pipeline failed')
        cleanup_error = OSError('SMB close failed')

        log = []
        resource = RecordingResource('r', log, raises=cleanup_error)
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline(raises=pipeline_error)
        with self.assertRaises(tc.PipelineError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=False,
            )

        # The real, primary cause -- confirmed by identity, via __cause__
        # (PipelineError is raised with `from e`), not just that *a*
        # PipelineError happened to be raised.
        self.assertIs(cm.exception.__cause__, pipeline_error)
        self.assertEqual(resource.close_calls, 1, 'cleanup was not even attempted')

    def test_publisher_close_failure_also_stays_secondary_to_the_pipeline_error(self):
        pipeline_error = RuntimeError('pipeline failed')
        publisher_close_error = OSError('SMB close failed')

        pipeline = make_pandas_pipeline(db_table='t1', raises=pipeline_error)
        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaises(tc.PipelineError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=True, creds={},
                publisher_config=tc.PublisherConfig(
                    publisher_factory=lambda **kw: FakePublisher(**kw, close_error=publisher_close_error),
                ),
            )

        self.assertIs(cm.exception.__cause__, pipeline_error)


class Test5CleanupErrorSurfacedWhenTaskSucceeds(unittest.TestCase):
    """The gap this whole round exists to close: the previous fix
    correctly stopped a cleanup failure from masking a real pipeline
    failure, by always logging cleanup failures rather than raising them
    -- but that meant a cleanup failure on an otherwise-successful run
    was silently, permanently invisible too. Confirmed this was
    genuinely happening before fixing it, not assumed."""

    def test_cleanup_error_is_raised_when_task_itself_succeeds(self):
        log = []
        resource = RecordingResource('r', log, raises=OSError('SMB close failed'))
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline()  # succeeds
        with self.assertRaises(OSError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=False,
            )

    def test_publisher_close_error_is_raised_when_task_itself_succeeds(self):
        resource = RecordingResource('r', [])
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline(db_table='t1')  # succeeds
        with self.assertRaises(OSError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=True, creds={},
                publisher_config=tc.PublisherConfig(
                    publisher_factory=lambda **kw: FakePublisher(**kw, close_error=OSError('SMB close failed')),
                ),
            )
        # ctx.close() must still have run despite the publisher error.
        self.assertEqual(resource.close_calls, 1)

    def test_multiple_resource_cleanup_failures_both_surface_as_a_group(self):
        log = []
        bad1 = RecordingResource('bad1', log, raises=OSError('bad1 fails'))
        bad2 = RecordingResource('bad2', log, raises=ValueError('bad2 fails'))
        ctx = tc.task_context(task_name='t', loaders={'bad1': lambda: bad1, 'bad2': lambda: bad2})
        ctx.get_resource('bad1')
        ctx.get_resource('bad2')

        pipeline = make_pandas_pipeline()
        with self.assertRaises(ExceptionGroup) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=False,
            )
        self.assertEqual(len(cm.exception.exceptions), 2)
        self.assertEqual(bad1.close_calls, 1)
        self.assertEqual(bad2.close_calls, 1)


class Test6TaskSucceedsInsideUnrelatedOuterExcept(unittest.TestCase):
    """The exact reproduction that found sys.exc_info() was unreliable:
    a caller of run_pipelines() sitting inside its own, unrelated
    except: block makes sys.exc_info() non-None for the call's entire
    duration, even though the task itself has nothing to do with that
    outer exception at all."""

    def test_cleanup_error_still_surfaces_despite_ambient_caller_exception(self):
        resource = RecordingResource('r', [], raises=OSError('cleanup specific'))
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline()  # succeeds

        try:
            raise ValueError('caller-side, unrelated exception')
        except ValueError:
            with self.assertRaises(OSError):
                tc.run_pipelines(
                    task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                    run_sequence=['p'], output_excel=False, output_db=False,
                )


class Test7CleanupLogHasTheCorrectTraceback(unittest.TestCase):
    """The bug here isn't "nothing was logged" -- it's "the wrong thing
    was logged, with a real, present-looking traceback that was actually
    the primary exception's, not the cleanup failure's". A test that
    only checks the log message text, not its traceback content, would
    have passed against the actual bug."""

    def test_logged_traceback_is_the_cleanup_exception_not_the_primary_one(self):
        resource = RecordingResource('r', [], raises=OSError('cleanup specific unique marker'))
        ctx = tc.task_context(task_name='cleanup_traceback_test', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline(raises=RuntimeError('primary pipeline failure unique marker'))

        with _CapturedLog('cleanup_traceback_test') as captured:
            with self.assertRaises(tc.PipelineError):
                tc.run_pipelines(
                    task_name='cleanup_traceback_test', build_context=lambda: ctx,
                    pipelines={'p': pipeline}, run_sequence=['p'],
                    output_excel=False, output_db=False,
                )

        log_text = captured.text
        cleanup_idx = log_text.find('cleanup error during run_pipelines()')
        self.assertNotEqual(cleanup_idx, -1, 'no cleanup-error log entry found at all')
        cleanup_section = log_text[cleanup_idx:]
        self.assertIn(
            'cleanup specific unique marker', cleanup_section,
            'the logged traceback under the cleanup-error entry does not contain the cleanup '
            "exception's own message -- it's showing the wrong traceback",
        )


class Test8RollbackFailureHandling(unittest.TestCase):
    """rollback() has the exact same failure-priority problem close()
    had -- if rollback() itself raises, it must not replace whatever
    real error (pipeline, commit, or otherwise) is already the reason
    this task failed. Also covers the skip-path double-rollback edge
    case: sources-unchanged calls rollback() once on its own skip path;
    if that call itself fails, the outer except: block must not attempt
    a second rollback() on the same publisher."""

    def test_pipeline_failure_plus_rollback_failure_preserves_pipeline_error(self):
        pipeline_error = RuntimeError('pipeline failed')
        pipeline = make_pandas_pipeline(db_table='t1', raises=pipeline_error)

        published = []

        def factory(**kw):
            p = FakePublisher(**kw, rollback_error=OSError('rollback failed'))
            published.append(p)
            return p

        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaises(tc.PipelineError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=factory),
            )

        self.assertIs(cm.exception.__cause__, pipeline_error)
        self.assertEqual(published[0].rollback_calls, 1)

    def test_commit_failure_plus_rollback_failure_preserves_commit_error(self):
        pipeline = make_pandas_pipeline(db_table='t1')  # succeeds -- commit() is what fails

        published = []

        def factory(**kw):
            p = FakePublisher(**kw, commit_error=RuntimeError('commit failed'), rollback_error=OSError('rollback also failed'))
            published.append(p)
            return p

        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaises(RuntimeError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, publisher_config=tc.PublisherConfig(publisher_factory=factory),
            )

        self.assertEqual(str(cm.exception), 'commit failed', 'the rollback failure masked the real commit failure')
        self.assertEqual(published[0].rollback_calls, 1)

    def test_skip_path_rollback_failure_is_attempted_exactly_once(self):
        # Verifies the outcome the review asked for directly: rollback()
        # attempted exactly once on the skip path, with its own failure
        # surfacing since nothing else failed. The mechanism guaranteeing
        # "exactly once" here is primarily try_step() itself (it catches
        # the failure rather than letting it propagate, so the skip
        # path's own return always completes normally and the except:
        # block is never even reached from this scenario) -- see
        # runner.py's own comment on try_rollback()'s rollback_attempted
        # guard for the narrower, defense-in-depth case that specifically
        # protects against.
        class _FakeDialect:
            # SourceStateStore reads conn.dialect to decide whether to ask
            # PostgreSQL for its real max_identifier_length. A non-postgres
            # dialect takes the documented fallback.
            name = 'sqlite'

        class FakeSourceStateConn:
            dialect = _FakeDialect()
            invalidated = False

            def in_transaction(self):
                return False

            def __init__(self):
                self.rows = {('t', 'files'): 'sig-v1'}  # seeded as already-committed, unchanged

            def execute(self, query, params=None):
                sql = str(query).lower().strip()
                if 'create table' in sql:
                    return None
                if sql.startswith('select'):
                    class R:
                        def mappings(self_inner):
                            return [
                                {'source_key': k[1], 'source_signature': v}
                                for k, v in self.rows.items() if k[0] == params['task_name']
                            ]
                    return R()
                return None

            def commit(self):
                pass

            def rollback(self):
                pass

            def in_transaction(self):
                return False

        class SkipPathPublisher(FakePublisher):
            def ensure_connection(self):
                return FakeSourceStateConn()

        published = []

        def factory(**kw):
            p = SkipPathPublisher(**kw, rollback_error=OSError('rollback failed'))
            published.append(p)
            return p

        class UnusedResource:
            def source_fingerprint(self, source_key):
                return tc.source_tracking.SourceFingerprint(
                    source_key=source_key, source_kind='file_set', root_path='/x',
                    include_mask='*.xlsx', recursive=False, file_count=1, total_size_bytes=1,
                    max_modified_at_utc=None, source_signature='sig-v1',  # matches seeded row -- unchanged
                    source_snapshot=None, store_snapshot=False,
                )
            def close(self):
                pass

        pipeline_ran = []

        class pipeline:
            spec = tc.PipelineSpec()
            @classmethod
            def run(cls, ctx):
                pipeline_ran.append(True)  # must never actually run -- sources are unchanged

        ctx = tc.task_context(
            task_name='t', loaders={'files': lambda: UnusedResource()},
            tracked_sources=[tc.TrackedResourceSource('files')],
        )
        config = tc.SourceChangeCheckConfig(enabled=True)

        with self.assertRaises(OSError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline}, run_sequence=['p'],
                output_excel=False, output_db=True, creds={}, source_change_check=config,
                publisher_config=tc.PublisherConfig(publisher_factory=factory),
            )

        self.assertEqual(pipeline_ran, [], 'pipeline ran despite unchanged sources')
        self.assertEqual(published[0].rollback_calls, 1, 'rollback() was attempted more than once on the skip path')


class Test9ClosedContextRejectsGetResource(unittest.TestCase):
    """Without this, close() -> get_resource() -> close() constructs a
    real resource between the two close() calls that the second call,
    now a no-op via the idempotency guard, never attempts to close at
    all -- a genuine, silent leak."""

    def test_get_resource_after_close_is_rejected_not_silently_leaked(self):
        construct_count = [0]

        class R:
            def __init__(self):
                construct_count[0] += 1
            def close(self):
                pass

        ctx = tc.task_context(task_name='t', loaders={'source': lambda: R()})
        ctx.close()

        with self.assertRaises(RuntimeError):
            ctx.get_resource('source')

        self.assertEqual(construct_count[0], 0, 'a resource was constructed despite the context already being closed')

    def test_second_close_call_remains_a_safe_no_op(self):
        ctx = tc.task_context(task_name='t', loaders={})
        ctx.close()
        ctx.close()  # must not raise


class Test10InterruptionExceptionsAreNotMasked(unittest.TestCase):
    """KeyboardInterrupt, SystemExit, and GeneratorExit are BaseException
    subclasses, not Exception ones -- found by external review, confirmed
    directly: the outer except: clause only ever caught Exception, so
    primary_error stayed None during a genuine interruption, and a
    cleanup failure during that interruption incorrectly looked like the
    only failure there was to report, replacing the interruption itself
    in what actually propagated."""

    def test_keyboard_interrupt_is_not_masked_by_a_cleanup_failure(self):
        resource = RecordingResource('r', [], raises=OSError('cleanup specific'))
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        class pipeline:
            spec = tc.PipelineSpec()
            @classmethod
            def run(cls, ctx):
                raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )

    def test_system_exit_is_not_masked_by_a_cleanup_failure(self):
        resource = RecordingResource('r', [], raises=OSError('cleanup specific'))
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        class pipeline:
            spec = tc.PipelineSpec()
            @classmethod
            def run(cls, ctx):
                raise SystemExit(1)

        with self.assertRaises(SystemExit):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )

    def test_regular_exception_handling_is_unaffected(self):
        # Confirms widening the outer except: to BaseException didn't
        # change anything about how ordinary pipeline failures get
        # wrapped into PipelineError -- that wrapping happens in the
        # inner, still-Exception-scoped pipeline-loop try/except, which
        # this change doesn't touch.
        pipeline_error = ValueError('regular failure')
        pipeline = make_pandas_pipeline(raises=pipeline_error)
        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaises(tc.PipelineError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )
        self.assertIs(cm.exception.__cause__, pipeline_error)


class Test11CleanupLogDoesNotDuplicateThePrimaryTraceback(unittest.TestCase):
    """Not a correctness bug -- a diagnostics-noise one, found by
    external review: the cleanup exception is caught while primary_error
    is already the active exception, so Python automatically chains it
    as __context__, and since primary_error is already logged
    separately, every cleanup log entry was repeating the full primary
    traceback too."""

    def test_cleanup_log_contains_only_the_cleanup_traceback_not_the_primary_one(self):
        resource = RecordingResource('r', [], raises=OSError('cleanup specific unique marker'))
        ctx = tc.task_context(task_name='no_dup_test', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline(raises=RuntimeError('primary pipeline failure unique marker'))

        with _CapturedLog('no_dup_test') as captured:
            with self.assertRaises(tc.PipelineError):
                tc.run_pipelines(
                    task_name='no_dup_test', build_context=lambda: ctx,
                    pipelines={'p': pipeline}, run_sequence=['p'],
                    output_excel=False, output_db=False,
                )

        log_text = captured.text
        cleanup_idx = log_text.find('cleanup error during run_pipelines()')
        self.assertNotEqual(cleanup_idx, -1)
        cleanup_section = log_text[cleanup_idx:]

        self.assertIn(
            'cleanup specific unique marker', cleanup_section,
            'the cleanup exception itself must still be fully present',
        )
        self.assertNotIn(
            'primary pipeline failure unique marker', cleanup_section,
            'the cleanup log entry duplicates the primary traceback via __context__ chaining',
        )


class Test12CleanupItselfHandlesBaseExceptionToo(unittest.TestCase):
    """Found by external review, confirmed directly before fixing: a
    KeyboardInterrupt/SystemExit raised *during a resource's own close()*
    -- not by the pipeline, but by cleanup itself -- was not caught by
    either cleanup collection point (cleanup.py's attempt_all_cleanup(),
    runner.py's try_step()), both of which only caught Exception. That
    meant it could still stop subsequent resources from getting a close
    attempt, and still replace an already-propagating primary pipeline
    failure -- the exact class of bug already fixed for the outer
    boundary, just one layer further in."""

    def test_keyboard_interrupt_during_cleanup_does_not_stop_remaining_resources(self):
        close_log = []

        class ResourceA:
            def close(self):
                close_log.append('A')
                raise KeyboardInterrupt()

        class ResourceB:
            def close(self):
                close_log.append('B')

        ctx = tc.task_context(task_name='t', loaders={'a': lambda: ResourceA(), 'b': lambda: ResourceB()})
        ctx.get_resource('a')
        ctx.get_resource('b')

        pipeline_error = RuntimeError('the real pipeline failure')
        pipeline = make_pandas_pipeline(raises=pipeline_error)

        with self.assertRaises(tc.PipelineError) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )

        self.assertIs(cm.exception.__cause__, pipeline_error, 'the KeyboardInterrupt during cleanup replaced the real primary failure')
        self.assertEqual(close_log, ['A', 'B'], 'resource B never got its close attempt')

    def test_keyboard_interrupt_during_cleanup_surfaces_when_task_itself_succeeds(self):
        class BadResource:
            def close(self):
                raise KeyboardInterrupt()

        resource = BadResource()
        ctx = tc.task_context(task_name='t', loaders={'r': lambda: resource})
        ctx.get_resource('r')

        pipeline = make_pandas_pipeline()  # succeeds

        with self.assertRaises(KeyboardInterrupt):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )

    def test_mixed_exception_and_base_exception_cleanup_failures_group_correctly(self):
        class BadA:
            def close(self):
                raise KeyboardInterrupt()

        class BadB:
            def close(self):
                raise OSError('resource B fails')

        ctx = tc.task_context(task_name='t', loaders={'a': lambda: BadA(), 'b': lambda: BadB()})
        ctx.get_resource('a')
        ctx.get_resource('b')

        pipeline = make_pandas_pipeline()  # succeeds

        with self.assertRaises(BaseExceptionGroup) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )
        self.assertEqual(len(cm.exception.exceptions), 2)
        self.assertFalse(
            isinstance(cm.exception, ExceptionGroup),
            'a group containing a genuine BaseException-only member should not downcast to a plain ExceptionGroup',
        )

    def test_all_exception_cleanup_failures_still_downcast_to_plain_exceptiongroup(self):
        # Backward-compatibility check: ordinary, all-Exception cleanup
        # failures (the common case) must still be catchable via
        # `except ExceptionGroup:` specifically, not just BaseExceptionGroup.
        class BadA:
            def close(self):
                raise OSError('a fails')

        class BadB:
            def close(self):
                raise ValueError('b fails')

        ctx = tc.task_context(task_name='t', loaders={'a': lambda: BadA(), 'b': lambda: BadB()})
        ctx.get_resource('a')
        ctx.get_resource('b')

        pipeline = make_pandas_pipeline()

        with self.assertRaises(ExceptionGroup) as cm:
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p'], output_excel=False, output_db=False,
            )
        self.assertEqual(len(cm.exception.exceptions), 2)


class Test13SuppressionAppliesRecursivelyToGroupedFailures(unittest.TestCase):
    """Found by external review, confirmed directly before fixing:
    __suppress_context__ was set only on the outer ExceptionGroup, not on
    the exceptions nested inside it -- so a grouped cleanup failure (two
    or more resources each failing) still showed the complete primary
    traceback duplicated inside one of its own nested exceptions, even
    though the single-exception case was already correctly fixed."""

    def test_no_nested_exception_in_the_group_duplicates_the_primary_traceback(self):
        resource_a = RecordingResource('a', [], raises=OSError('resource A close failed unique marker'))
        resource_b = RecordingResource('b', [], raises=ValueError('resource B close failed unique marker'))
        ctx = tc.task_context(task_name='group_suppress_test', loaders={'a': lambda: resource_a, 'b': lambda: resource_b})
        ctx.get_resource('a')
        ctx.get_resource('b')

        pipeline = make_pandas_pipeline(raises=RuntimeError('primary pipeline failure unique marker'))

        with _CapturedLog('group_suppress_test') as captured:
            with self.assertRaises(tc.PipelineError):
                tc.run_pipelines(
                    task_name='group_suppress_test', build_context=lambda: ctx,
                    pipelines={'p': pipeline}, run_sequence=['p'],
                    output_excel=False, output_db=False,
                )

        log_text = captured.text
        cleanup_idx = log_text.find('cleanup error during run_pipelines()')
        self.assertNotEqual(cleanup_idx, -1)
        cleanup_section = log_text[cleanup_idx:]

        self.assertIn('resource A close failed unique marker', cleanup_section)
        self.assertIn('resource B close failed unique marker', cleanup_section)
        self.assertNotIn(
            'primary pipeline failure unique marker', cleanup_section,
            'a nested exception inside the group still duplicates the primary traceback',
        )


if __name__ == '__main__':
    unittest.main()
