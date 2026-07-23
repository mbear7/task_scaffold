# -*- coding: utf-8 -*-
"""
task_core.binding test suite -- covers the ten cases from the review
process (bind()/validate_bindings()/PipelineBinding/runner.py integration).
task_core only: every pipeline and resource here is a minimal stub built
directly in the test that needs it. hr_task.py/ops_task.py are never
imported -- they exist only as design reference for what shapes of real
usage these tests should cover, not as fixtures.

Uses unittest (standard library) rather than pytest, which isn't
available in this environment (no network, not cached).

Run from the project root with: python3 -m unittest tests.test_binding -v
"""

import dataclasses
import unittest

import pandas as pd

import task_core as tc


def make_resource(tag='r', tracker=False, on_load=None):
    """A ResourceSpec whose loader is a bare, call-counting stub -- no
    filesystem, no petl/pandas, nothing beyond what each test needs to
    observe: whether and how many times the loader actually fired."""
    calls = []

    def _load(env):
        calls.append(env)
        if on_load is not None:
            return on_load()
        return f'<{tag}-resource>'

    spec = tc.ResourceSpec(loader=_load, tracker=tracker)
    return spec, calls


class Test1ValidBinding(unittest.TestCase):
    def test_valid_single_resource_binding_passes(self):
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        resources = {'r': spec}
        pipelines = {'p': tc.bind(pipeline, source=spec)}
        # Should not raise.
        tc.validate_bindings(resources, pipelines)

    def test_valid_multi_resource_binding_passes(self):
        spec_a, _ = make_resource('a')
        spec_b, _ = make_resource('b')

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source, mapping):
                pass

        resources = {'a': spec_a, 'b': spec_b}
        pipelines = {'p': tc.bind(pipeline, source=spec_a, mapping=spec_b)}
        tc.validate_bindings(resources, pipelines)

    def test_resource_less_plain_class_passes(self):
        class pipeline:
            @classmethod
            def run(cls, ctx):
                pass

        # A plain class (no bind() at all) is a valid PIPELINES entry --
        # validate_bindings() must not object to it.
        tc.validate_bindings({}, {'p': pipeline})


class Test2MissingAndExtraAliases(unittest.TestCase):
    def test_missing_alias_is_rejected(self):
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source, mapping):
                pass

        # bind() only supplies 'source'; run() also requires 'mapping'.
        pipelines = {'p': tc.bind(pipeline, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)

    def test_extra_alias_is_rejected(self):
        spec_a, _ = make_resource('a')
        spec_b, _ = make_resource('b')

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        # bind() supplies 'mapping' too, which run() never declares.
        pipelines = {'p': tc.bind(pipeline, source=spec_a, mapping=spec_b)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'a': spec_a, 'b': spec_b}, pipelines)

    def test_checked_even_when_inactive(self):
        # The mismatch is still caught by validate_bindings() itself even
        # though nothing here ever gets scoped through RUN_SEQUENCE --
        # structural validation covers every declared binding, not just
        # active ones.
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, wrong_name):
                pass

        pipelines = {'inactive_pipeline': tc.bind(pipeline, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)


class Test3MissingRun(unittest.TestCase):
    def test_no_run_method_at_all(self):
        spec, _ = make_resource()

        class no_run:
            pass

        pipelines = {'p': tc.bind(no_run, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)

    def test_error_is_deliberate_not_a_raw_attribute_error(self):
        # Specifically: this must not surface as a bare AttributeError --
        # it should be the project's own PipelineContractError vocabulary.
        spec, _ = make_resource()

        class no_run:
            pass

        pipelines = {'p': tc.bind(no_run, source=spec)}
        try:
            tc.validate_bindings({'r': spec}, pipelines)
            self.fail('should have raised')
        except AttributeError:
            self.fail('raised a raw AttributeError instead of PipelineContractError')
        except tc.PipelineContractError:
            pass

    def test_run_that_is_not_callable(self):
        spec, _ = make_resource()

        class run_is_not_callable:
            run = 'not a function'

        pipelines = {'p': tc.bind(run_is_not_callable, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)


class Test4AdditionalRequiredPositionalParameter(unittest.TestCase):
    def test_extra_required_positional_is_rejected_at_validation(self):
        spec, _ = make_resource()

        class broken:
            @classmethod
            def run(cls, ctx, extra_argument, *, source):
                pass

        # The keyword-only set ('source') matches bind() exactly -- this
        # must still be rejected, because the complete signature is
        # invalid for injection (extra_argument has no way to be supplied).
        pipelines = {'p': tc.bind(broken, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)

    def test_missing_ctx_itself_is_rejected(self):
        spec, _ = make_resource()

        class broken:
            @classmethod
            def run(cls, *, source):  # no ctx parameter at all
                pass

        pipelines = {'p': tc.bind(broken, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)

    def test_wrongly_named_first_parameter_is_rejected(self):
        spec, _ = make_resource()

        class broken:
            @classmethod
            def run(cls, context, *, source):  # not named 'ctx'
                pass

        pipelines = {'p': tc.bind(broken, source=spec)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'r': spec}, pipelines)

    def test_var_positional_after_ctx_does_not_falsely_reject(self):
        # def run(cls, ctx, *args, source): is unusual but not actually
        # broken -- calling run(ctx, source=X) works fine, since *args
        # just captures zero extra positionals. Structural validation
        # should not reject a shape that would work at call time.
        spec, _ = make_resource()

        class fine:
            @classmethod
            def run(cls, ctx, *args, source):
                pass

        pipelines = {'p': tc.bind(fine, source=spec)}
        tc.validate_bindings({'r': spec}, pipelines)  # should not raise


class Test5OrphanResource(unittest.TestCase):
    def test_binding_to_an_unregistered_spec_is_rejected(self):
        orphan, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        # orphan is never placed in RESOURCES at all.
        pipelines = {'p': tc.bind(pipeline, source=orphan)}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({}, pipelines)

    def test_binding_to_a_registered_spec_passes(self):
        # Same shape, but the spec IS registered -- confirms the check is
        # actually about registration, not rejecting everything.
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        pipelines = {'p': tc.bind(pipeline, source=spec)}
        tc.validate_bindings({'r': spec}, pipelines)

    def test_binding_to_a_non_resourcespec_value_is_rejected(self):
        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        pipelines = {'p': tc.bind(pipeline, source='not a ResourceSpec at all')}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({}, pipelines)


class Test6DuplicateResourceRegistration(unittest.TestCase):
    def test_same_spec_under_two_keys_is_rejected(self):
        shared, _ = make_resource()
        resources = {'a': shared, 'b': shared}
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings(resources, {})

    def test_two_different_specs_under_two_keys_passes(self):
        spec_a, _ = make_resource('a')
        spec_b, _ = make_resource('b')
        resources = {'a': spec_a, 'b': spec_b}
        tc.validate_bindings(resources, {})  # should not raise

    def test_resources_entry_must_be_a_resourcespec(self):
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'bad': 'not a ResourceSpec'}, {})

    def test_resourcespec_must_have_callable_loader(self):
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'bad': tc.ResourceSpec(loader=123, tracker=False)}, {})

    def test_resourcespec_tracker_must_be_bool(self):
        with self.assertRaises(tc.PipelineContractError):
            tc.validate_bindings({'bad': tc.ResourceSpec(loader=lambda env: None, tracker='yes')}, {})


class Test7ImmutableBindingMapping(unittest.TestCase):
    def test_resources_mapping_cannot_be_mutated(self):
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        binding = tc.bind(pipeline, source=spec)
        with self.assertRaises(TypeError):
            binding.resources['source'] = make_resource('other')[0]

    def test_resources_field_reassignment_still_blocked_by_frozen(self):
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        binding = tc.bind(pipeline, source=spec)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            binding.resources = {}

    def test_original_kwargs_dict_mutation_does_not_leak_in(self):
        # Mutating the dict passed to bind() *after* the call must not
        # affect the binding either -- bind() must copy, not alias.
        spec, _ = make_resource()
        kwargs = {'source': spec}

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        binding = tc.bind(pipeline, **kwargs)
        kwargs['source'] = make_resource('other')[0]
        self.assertIs(binding.resources['source'], spec)


class Test8SharedResourceLoadedOnce(unittest.TestCase):
    def test_two_pipelines_sharing_one_spec_load_it_exactly_once(self):
        spec, calls = make_resource('shared')
        received = {}

        class pipeline_a:
            spec = tc.PipelineSpec(table_adapter='pandas')
            @classmethod
            def run(cls, ctx, *, source):
                received['a'] = source
                return pd.DataFrame({'col': [1]})

        class pipeline_b:
            spec = tc.PipelineSpec(table_adapter='pandas')
            @classmethod
            def run(cls, ctx, *, source):
                received['b'] = source
                return pd.DataFrame({'col': [1]})

        resources = {'shared': spec}
        pipelines = {
            'a': tc.bind(pipeline_a, source=spec),
            'b': tc.bind(pipeline_b, source=spec),
        }
        run_sequence = ['a', 'b']
        env = tc.ResourceEnvironment()

        tc.run_pipelines(
            task_name='test8',
            build_context=lambda: tc.build_resource_context('test8', resources, pipelines, run_sequence, env),
            pipelines=pipelines,
            run_sequence=run_sequence,
            output_excel=False,
            output_db=False,
        )

        self.assertEqual(len(calls), 1, f'loader should fire exactly once, fired {len(calls)} times')
        self.assertIs(received['a'], received['b'], 'both pipelines must receive the identical resolved object')

    def test_two_distinct_specs_each_load_independently(self):
        # Sanity check on the other direction -- two DIFFERENT specs must
        # each load their own resource, not collapse into one.
        spec_a, calls_a = make_resource('a')
        spec_b, calls_b = make_resource('b')

        class pipeline_a:
            spec = tc.PipelineSpec(table_adapter='pandas')
            @classmethod
            def run(cls, ctx, *, source):
                return pd.DataFrame({'col': [1]})

        class pipeline_b:
            spec = tc.PipelineSpec(table_adapter='pandas')
            @classmethod
            def run(cls, ctx, *, source):
                return pd.DataFrame({'col': [1]})

        resources = {'a': spec_a, 'b': spec_b}
        pipelines = {
            'a': tc.bind(pipeline_a, source=spec_a),
            'b': tc.bind(pipeline_b, source=spec_b),
        }
        run_sequence = ['a', 'b']
        env = tc.ResourceEnvironment()

        tc.run_pipelines(
            task_name='test8b',
            build_context=lambda: tc.build_resource_context('test8b', resources, pipelines, run_sequence, env),
            pipelines=pipelines,
            run_sequence=run_sequence,
            output_excel=False,
            output_db=False,
        )

        self.assertEqual(len(calls_a), 1)
        self.assertEqual(len(calls_b), 1)


class Test9InactiveResourceNotConstructed(unittest.TestCase):
    def test_inactive_pipelines_resource_is_never_loaded(self):
        active_spec, active_calls = make_resource('active')
        inactive_spec, inactive_calls = make_resource('inactive')

        class active_pipeline:
            spec = tc.PipelineSpec(table_adapter='pandas')
            @classmethod
            def run(cls, ctx, *, source):
                return pd.DataFrame({'col': [1]})

        class inactive_pipeline:
            spec = tc.PipelineSpec()
            @classmethod
            def run(cls, ctx, *, source):
                self.fail('inactive pipeline should never actually run')

        resources = {'active': active_spec, 'inactive': inactive_spec}
        pipelines = {
            'active': tc.bind(active_pipeline, source=active_spec),
            'inactive': tc.bind(inactive_pipeline, source=inactive_spec),
        }
        # 'inactive' is deliberately excluded from run_sequence.
        run_sequence = ['active']
        env = tc.ResourceEnvironment()

        tc.run_pipelines(
            task_name='test9',
            build_context=lambda: tc.build_resource_context('test9', resources, pipelines, run_sequence, env),
            pipelines=pipelines,
            run_sequence=run_sequence,
            output_excel=False,
            output_db=False,
        )

        self.assertEqual(len(active_calls), 1)
        self.assertEqual(len(inactive_calls), 0, 'inactive pipeline\'s resource must never be constructed')

    def test_inactive_but_malformed_binding_is_still_caught(self):
        # The complement of the above: exclusion from run_sequence must
        # not become a way to hide a broken binding. Structural validation
        # still covers it, even though its resource is never touched.
        spec, calls = make_resource()

        class broken_inactive:
            @classmethod
            def run(cls, ctx, *, wrong_alias):
                pass

        class fine_active:
            spec = tc.PipelineSpec(table_adapter='pandas')
            @classmethod
            def run(cls, ctx):
                return pd.DataFrame({'col': [1]})

        resources = {'r': spec}
        pipelines = {
            'active': fine_active,
            'inactive': tc.bind(broken_inactive, source=spec),
        }
        run_sequence = ['active']  # 'inactive' never runs
        env = tc.ResourceEnvironment()

        with self.assertRaises(tc.PipelineContractError) as ctx_manager:
            tc.run_pipelines(
                task_name='test9b',
                build_context=lambda: tc.build_resource_context('test9b', resources, pipelines, run_sequence, env),
                pipelines=pipelines,
                run_sequence=run_sequence,
                output_excel=False,
                output_db=False,
            )
        # Precise, not just "some PipelineContractError fired somewhere" --
        # confirms this is genuinely the binding-mismatch check, not a
        # different failure (e.g. table validation) that happened to also
        # raise the same exception class and would otherwise mask a wrong
        # assumption about execution order.
        self.assertIn('wrong_alias', str(ctx_manager.exception))
        self.assertEqual(len(calls), 0, 'construction must not have started before validation failed')


class Test10DynamicDbContractOnBoundPipeline(unittest.TestCase):
    def test_get_dynamic_db_contract_is_found_through_the_binding(self):
        # The exact regression this test exists for: a bound pipeline
        # (task_cls in PIPELINES is a PipelineBinding, not the underlying
        # class) must still have its get_dynamic_db_contract() hook found
        # and applied -- not silently fall back to the static db_contract
        # because the wrapper, not the class, got inspected.
        spec, _ = make_resource(on_load=lambda: 'loaded-resource')

        class dynamic_pipeline:
            spec = tc.PipelineSpec(
                db_table='test_table',
                db_contract={'static_col': 'static_target'},
                table_adapter='pandas',
            )
            @classmethod
            def get_dynamic_db_contract(cls, out_tbl):
                return {'dynamic_col': 'dynamic_target'}
            @classmethod
            def run(cls, ctx, *, source):
                return pd.DataFrame({'dynamic_col': [1]})

        resources = {'r': spec}
        pipelines = {'p': tc.bind(dynamic_pipeline, source=spec)}
        run_sequence = ['p']
        env = tc.ResourceEnvironment()

        published = []

        class FakeDbPublisher:
            def __init__(self, *, creds, schema, logger=None):
                self.published = []
                published.append(self)
            def ensure_connection(self):
                return object()
            def discard_pending_read(self):
                pass
            def publish(self, payload):
                self.published.append(payload)
            def commit(self):
                return []
            def rollback(self):
                pass
            def close(self):
                pass
            committed = property(lambda self: True)
            committed_tables = property(lambda self: [])
            written_tables = property(lambda self: self.published)
            table_rows = property(lambda self: {})

        original_publisher_cls = tc.runner.DbPublisher
        tc.run_pipelines(
            task_name='test10',
            build_context=lambda: tc.build_resource_context('test10', resources, pipelines, run_sequence, env),
            pipelines=pipelines,
            run_sequence=run_sequence,
            output_excel=False,
            output_db=True,
            creds={'user': 'x', 'host': 'x', 'dbname': 'x'},
            publisher_factory=FakeDbPublisher,
        )
        # publisher_factory is a plain argument -- confirms it didn't need
        # to touch the real DbPublisher class at all, unlike the
        # monkeypatch this replaced.
        self.assertIs(tc.runner.DbPublisher, original_publisher_cls)

        self.assertEqual(len(published), 1)
        payload = published[0].published[0]
        self.assertIn('dynamic_target', payload.columns, 'dynamic contract column missing -- fell back to static')
        self.assertNotIn('static_target', payload.columns, 'static contract should have been fully replaced, not merged')


class Test4DuplicateRunSequenceRejected(unittest.TestCase):
    """Found by external review, confirmed directly before fixing: a
    duplicated name in run_sequence ran the pipeline twice, with
    pipeline_rows silently retaining only the final run's count -- no
    error anywhere, and real consequences (duplicate Excel export, a DB
    table dropped and recreated twice, duplicated shared-state
    publication)."""

    def test_run_pipelines_rejects_duplicate_run_sequence(self):
        call_count = [0]

        class pipeline:
            spec = tc.PipelineSpec()

            @classmethod
            def run(cls, ctx):
                call_count[0] += 1
                import petl as etl
                return etl.wrap([('a',), (1,)])

        ctx = tc.task_context(task_name='t', loaders={})
        with self.assertRaises(tc.PipelineContractError):
            tc.run_pipelines(
                task_name='t', build_context=lambda: ctx, pipelines={'p': pipeline},
                run_sequence=['p', 'p'], output_excel=False, output_db=False,
            )
        self.assertEqual(call_count[0], 0, 'pipeline ran before validation could reject the duplicate')

    def test_compute_resource_wiring_rejects_duplicate_run_sequence(self):
        # A separate validation path, reachable standalone (a task's own
        # build_context() calls this directly) -- not just protected
        # when reached through run_pipelines().
        spec, _ = make_resource()

        class pipeline:
            @classmethod
            def run(cls, ctx, *, source):
                pass

        env = tc.ResourceEnvironment()
        with self.assertRaises(tc.PipelineContractError):
            tc.compute_resource_wiring(
                {'r': spec}, {'p': tc.bind(pipeline, source=spec)}, ['p', 'p'], env,
            )

    def test_non_duplicate_run_sequence_still_works(self):
        ran = []

        class p1:
            spec = tc.PipelineSpec()

            @classmethod
            def run(cls, ctx):
                ran.append('p1')
                import petl as etl
                return etl.wrap([('a',), (1,)])

        class p2:
            spec = tc.PipelineSpec()

            @classmethod
            def run(cls, ctx):
                ran.append('p2')
                import petl as etl
                return etl.wrap([('a',), (1,)])

        ctx = tc.task_context(task_name='t', loaders={})
        result = tc.run_pipelines(
            task_name='t', build_context=lambda: ctx, pipelines={'p1': p1, 'p2': p2},
            run_sequence=['p1', 'p2'], output_excel=False, output_db=False,
        )
        self.assertEqual(ran, ['p1', 'p2'])
        self.assertEqual(result.pipeline_rows, {'p1': 1, 'p2': 1})


if __name__ == '__main__':
    unittest.main()
