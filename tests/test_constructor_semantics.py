# -*- coding: utf-8 -*-
"""Constructor semantics from ADR 0013.

Configuration choices are named. Natural values may be positional. Stable
result contracts remain unchanged. Internal records choose constructor shape
for clarity and measured performance rather than by blanket convention.
"""

from datetime import datetime, timezone
import inspect
import unittest

import sqlalchemy as sa

import task_core as tc


class Test1AuthorFacingConfigurationsAreKeywordOnly(unittest.TestCase):
    def test_every_configuration_parameter_is_keyword_only(self):
        for cls in (
            tc.PipelineSpec,
            tc.PublisherConfig,
            tc.PublicationLockPolicy,
            tc.CopyLoadPolicy,
            tc.SourceChangeCheckConfig,
            tc.ResourceEnvironment,
            tc.IdentifierPolicy,
        ):
            with self.subTest(cls=cls.__name__):
                parameters = inspect.signature(cls).parameters.values()
                self.assertTrue(parameters)
                self.assertTrue(
                    all(
                        parameter.kind is inspect.Parameter.KEYWORD_ONLY
                        for parameter in parameters
                    )
                )

    def test_positional_configuration_is_rejected(self):
        cases = (
            (tc.PipelineSpec, ('out.xlsx',)),
            (tc.PublisherConfig, (None,)),
            (tc.PublicationLockPolicy, (500,)),
            (tc.CopyLoadPolicy, (None,)),
            (tc.SourceChangeCheckConfig, (True,)),
            (tc.ResourceEnvironment, ('/tmp',)),
            (tc.IdentifierPolicy, (63,)),
        )
        for cls, args in cases:
            with self.subTest(cls=cls.__name__):
                with self.assertRaises(TypeError):
                    cls(*args)

    def test_keyword_configuration_remains_valid(self):
        self.assertEqual(tc.PipelineSpec(db_table='t').db_table, 't')
        self.assertIsNone(tc.PublisherConfig(publisher_factory=None).publisher_factory)
        self.assertEqual(
            tc.PublicationLockPolicy(lock_timeout_ms=500).lock_timeout_ms,
            500,
        )
        self.assertFalse(
            tc.CopyLoadPolicy(encrypt_spools=False).encrypt_spools
        )
        self.assertTrue(
            tc.SourceChangeCheckConfig(enabled=True).enabled
        )
        self.assertEqual(
            tc.ResourceEnvironment(base_path='/tmp').base_path,
            '/tmp',
        )
        self.assertEqual(
            tc.IdentifierPolicy(max_identifier_bytes=63).max_identifier_bytes,
            63,
        )


class Test2MixedValueObjectsKeepOnlyNaturalValuesPositional(unittest.TestCase):
    def test_output_column_identity_is_positional_and_policy_is_named(self):
        signature = inspect.signature(tc.OutputColumn).parameters
        self.assertIs(
            signature['name'].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertIs(
            signature['type'].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertIs(
            signature['nullable'].kind,
            inspect.Parameter.KEYWORD_ONLY,
        )

        column = tc.OutputColumn('id', sa.BigInteger(), nullable=False)
        self.assertFalse(column.nullable)
        with self.assertRaises(TypeError):
            tc.OutputColumn('id', sa.BigInteger(), False)

    def test_resource_loader_is_positional_and_tracker_policy_is_named(self):
        signature = inspect.signature(tc.ResourceSpec).parameters
        self.assertIs(
            signature['loader'].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertIs(
            signature['tracker'].kind,
            inspect.Parameter.KEYWORD_ONLY,
        )

        def loader(env):
            return env

        resource = tc.ResourceSpec(loader, tracker=True)
        self.assertIs(resource.loader, loader)
        self.assertTrue(resource.tracker)
        with self.assertRaises(TypeError):
            tc.ResourceSpec(loader, True)


class Test3StableResultsRemainUnchanged(unittest.TestCase):
    def test_result_fields_are_not_keyword_only(self):
        for cls in (tc.RunResult, tc.DbRunResult):
            with self.subTest(cls=cls.__name__):
                for parameter in inspect.signature(cls).parameters.values():
                    self.assertIs(
                        parameter.kind,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    )


class Test4NaturalRecordsMayRemainPositional(unittest.TestCase):
    def test_source_file_meta_keeps_natural_positional_construction(self):
        value = tc.SourceFileMeta(
            'source.xlsx',
            '/tmp/source.xlsx',
            123,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(value.relative_path, 'source.xlsx')
        self.assertEqual(value.size_bytes, 123)


if __name__ == '__main__':
    unittest.main()
