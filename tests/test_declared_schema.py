# -*- coding: utf-8 -*-

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest import mock

import pandas as pd
import sqlalchemy as sa

import task_core as tc
from task_core.db_publish import (
    DbPayload,
    DbPublishError,
    DbPublisher,
    ResolvedColumn,
    ResolvedSchema,
    _resolve_payload_schema,
)
from task_core.export import _build_db_payload_with_spec, apply_db_updated_at


_CREDS = {'user': 'x', 'host': 'x', 'dbname': 'x'}


class Test1OutputSchemaConfiguration(unittest.TestCase):
    def test_nullable_defaults_true(self):
        column = tc.OutputColumn('value', sa.Text())
        self.assertTrue(column.nullable)

    def test_existing_positional_pipeline_spec_fields_keep_their_041_meaning(self):
        marker = object()
        spec = tc.PipelineSpec(
            'x.xlsx', 'target', ('id',), {'id': 'id'}, {'id': 'BIGINT'},
            marker, True, True, True, 'pandas',
        )
        self.assertIs(spec.db_table_id_pix, marker)
        self.assertTrue(spec.db_updated_at)
        self.assertTrue(spec.publish_result)
        self.assertTrue(spec.debug_display)
        self.assertEqual(spec.table_adapter, 'pandas')
        self.assertIsNone(spec.db_not_null_columns)
        self.assertIsNone(spec.output_schema)

    def test_schema_is_immutable_and_non_empty(self):
        columns = [tc.OutputColumn('id', sa.BigInteger(), nullable=False)]
        spec = tc.PipelineSpec(db_table='target', output_schema=columns)
        columns.append(tc.OutputColumn('extra', sa.Text()))
        self.assertIsInstance(spec.output_schema, tuple)
        self.assertEqual([column.name for column in spec.output_schema], ['id'])
        with self.assertRaises(tc.PipelineContractError):
            tc.PipelineSpec(db_table='target', output_schema=[])

    def test_declared_schema_has_one_source_of_truth(self):
        schema = (tc.OutputColumn('id', sa.BigInteger()),)
        for field, value in (
            ('db_output', ('id',)),
            ('db_type_overrides', {'id': 'BIGINT'}),
            ('db_not_null_columns', ('id',)),
        ):
            with self.subTest(field=field):
                with self.assertRaises(tc.PipelineContractError):
                    tc.PipelineSpec(db_table='target', output_schema=schema, **{field: value})

    def test_inferred_not_null_columns_are_normalized_and_unique(self):
        spec = tc.PipelineSpec(db_table='target', db_not_null_columns=['id'])
        self.assertEqual(spec.db_not_null_columns, ('id',))
        with self.assertRaises(tc.PipelineContractError):
            tc.PipelineSpec(db_not_null_columns=['id', 'id'])

    def test_framework_timestamp_cannot_be_declared_or_overridden(self):
        with self.assertRaises(tc.PipelineContractError):
            tc.PipelineSpec(
                db_updated_at=True,
                output_schema=(tc.OutputColumn('etl_updated_at', sa.DateTime(timezone=True)),),
            )
        with self.assertRaises(tc.PipelineContractError):
            tc.PipelineSpec(
                db_updated_at=True,
                db_type_overrides={'etl_updated_at': 'TEXT'},
            )


class Test2ResolvedSchemaAndValidation(unittest.TestCase):
    def _declared_payload(self, rows, columns=None, schema=None):
        schema = schema or (
            tc.OutputColumn('id', sa.BigInteger(), nullable=False),
            tc.OutputColumn('amount', sa.Numeric(18, 2)),
        )
        return DbPayload(
            table_name='target',
            schema='bsr',
            columns=list(columns or ['amount', 'id']),
            rows=rows,
            output_schema=schema,
        )

    def test_both_paths_produce_one_resolved_schema_type(self):
        inferred = _resolve_payload_schema(
            DbPayload('a', 'bsr', ['id'], [{'id': 1}]), sample_size=5000,
        )
        declared = _resolve_payload_schema(
            DbPayload(
                'b', 'bsr', ['id'], [{'id': 1}],
                output_schema=(tc.OutputColumn('id', sa.BigInteger()),),
            ),
            sample_size=5000,
        )
        self.assertIsInstance(inferred, ResolvedSchema)
        self.assertIsInstance(declared, ResolvedSchema)
        self.assertEqual(inferred.source, 'inferred')
        self.assertEqual(declared.source, 'declared')

    def test_declared_schema_reorders_columns(self):
        payload = self._declared_payload([{'amount': Decimal('1.25'), 'id': 1}])
        resolved = _resolve_payload_schema(payload, sample_size=5000)
        self.assertEqual(payload.columns, ['id', 'amount'])
        self.assertEqual([column.name for column in resolved.columns], ['id', 'amount'])

    def test_missing_and_unexpected_columns_fail(self):
        for columns, expected in ((['id'], 'missing'), (['id', 'amount', 'extra'], 'unexpected')):
            with self.subTest(columns=columns):
                payload = self._declared_payload([], columns=columns)
                with self.assertRaisesRegex(DbPublishError, expected):
                    _resolve_payload_schema(payload, sample_size=5000)

    def test_normalized_missing_markers_violate_not_null(self):
        for value in (None, float('nan'), pd.NA, pd.NaT):
            with self.subTest(value=repr(value)):
                payload = DbPayload(
                    'target', 'bsr', ['id'], [{'id': value}],
                    output_schema=(tc.OutputColumn('id', sa.BigInteger(), nullable=False),),
                )
                with self.assertRaisesRegex(DbPublishError, 'non-nullable'):
                    _resolve_payload_schema(payload, sample_size=5000)

    def test_inferred_not_null_is_enforced_during_preparation(self):
        payload = DbPayload(
            'target', 'bsr', ['id'], [{'id': 1}, {'id': float('nan')}],
            not_null_columns=('id',),
        )
        with self.assertRaisesRegex(DbPublishError, 'non-nullable'):
            _resolve_payload_schema(payload, sample_size=5000)

    def test_decimal_is_supported_but_float_to_numeric_is_not(self):
        good = self._declared_payload([{'id': 1, 'amount': Decimal('12.34')}], columns=['id', 'amount'])
        _resolve_payload_schema(good, sample_size=5000)

        bad = self._declared_payload([{'id': 1, 'amount': 12.34}], columns=['id', 'amount'])
        with self.assertRaisesRegex(DbPublishError, 'float-to-NUMERIC'):
            _resolve_payload_schema(bad, sample_size=5000)

    def test_numeric_scale_never_rounds_silently(self):
        payload = self._declared_payload(
            [{'id': 1, 'amount': Decimal('12.345')}], columns=['id', 'amount'],
        )
        with self.assertRaisesRegex(DbPublishError, 'rounding'):
            _resolve_payload_schema(payload, sample_size=5000)

    def test_datetime_awareness_must_match(self):
        cases = (
            (sa.DateTime(timezone=True), datetime(2026, 1, 1), False),
            (sa.DateTime(timezone=True), datetime(2026, 1, 1, tzinfo=timezone.utc), True),
            (sa.DateTime(timezone=False), datetime(2026, 1, 1), True),
            (sa.DateTime(timezone=False), datetime(2026, 1, 1, tzinfo=timezone.utc), False),
        )
        for type_obj, value, accepted in cases:
            with self.subTest(type=type_obj, value=value):
                payload = DbPayload(
                    'target', 'bsr', ['created_at'], [{'created_at': value}],
                    output_schema=(tc.OutputColumn('created_at', type_obj, nullable=False),),
                )
                if accepted:
                    _resolve_payload_schema(payload, sample_size=5000)
                else:
                    with self.assertRaises(DbPublishError):
                        _resolve_payload_schema(payload, sample_size=5000)

    def test_datetime_does_not_implicitly_become_date(self):
        payload = DbPayload(
            'target', 'bsr', ['day'], [{'day': datetime(2026, 1, 1)}],
            output_schema=(tc.OutputColumn('day', sa.Date()),),
        )
        with self.assertRaisesRegex(DbPublishError, 'datetime-to-DATE'):
            _resolve_payload_schema(payload, sample_size=5000)

    def test_empty_output_is_valid_when_columns_are_known(self):
        payload = self._declared_payload([], columns=['id', 'amount'])
        resolved = _resolve_payload_schema(payload, sample_size=5000)
        self.assertEqual(len(resolved.columns), 2)

    def test_direct_payload_rejects_competing_declared_configuration(self):
        schema = (tc.OutputColumn('id', sa.BigInteger()),)
        for kwargs, expected in (
            ({'type_overrides': {'id': 'BIGINT'}}, 'type_overrides'),
            ({'not_null_columns': ('id',)}, 'not_null_columns'),
        ):
            with self.subTest(expected=expected):
                payload = DbPayload(
                    'target', 'bsr', ['id'], [{'id': 1}],
                    output_schema=schema,
                    **kwargs,
                )
                with self.assertRaisesRegex(DbPublishError, expected):
                    _resolve_payload_schema(payload, sample_size=5000)

    def test_direct_payload_rejects_duplicate_not_null_columns(self):
        payload = DbPayload(
            'target', 'bsr', ['id'], [{'id': 1}],
            not_null_columns=('id', 'id'),
        )
        with self.assertRaisesRegex(DbPublishError, 'duplicate'):
            _resolve_payload_schema(payload, sample_size=5000)

    def test_framework_column_collision_is_rejected_for_direct_payload(self):
        payload = DbPayload(
            'target', 'bsr', ['id', 'etl_updated_at'],
            [{'id': 1, 'etl_updated_at': datetime(2026, 1, 1, tzinfo=timezone.utc)}],
            output_schema=(
                tc.OutputColumn('id', sa.BigInteger()),
                tc.OutputColumn('etl_updated_at', sa.DateTime(timezone=True)),
            ),
            framework_columns=(
                tc.OutputColumn('etl_updated_at', sa.DateTime(timezone=True), nullable=False),
            ),
        )
        with self.assertRaisesRegex(DbPublishError, 'framework'):
            _resolve_payload_schema(payload, sample_size=5000)

    def test_numeric_accepts_trailing_fractional_zeroes_and_large_precision(self):
        payload = DbPayload(
            'target', 'bsr', ['amount'],
            [{'amount': Decimal('123456789012345678901234567890.1200')}],
            output_schema=(tc.OutputColumn('amount', sa.Numeric(40, 2)),),
        )
        _resolve_payload_schema(payload, sample_size=5000)

    def test_unsupported_numeric_shape_is_rejected_at_schema_resolution(self):
        for type_obj in (sa.Numeric(0, 0), sa.Numeric(2, 3), sa.Numeric(4, -1)):
            with self.subTest(type=type_obj):
                payload = DbPayload(
                    'target', 'bsr', ['amount'], [{'amount': Decimal('1')}],
                    output_schema=(tc.OutputColumn('amount', type_obj),),
                )
                with self.assertRaisesRegex(DbPublishError, 'NUMERIC'):
                    _resolve_payload_schema(payload, sample_size=5000)

    def test_deferred_string_like_types_are_rejected(self):
        for type_obj in (sa.Enum('a', 'b', name='status_enum'), sa.CHAR(5)):
            with self.subTest(type=type_obj):
                payload = DbPayload(
                    'target', 'bsr', ['value'], [{'value': 'a'}],
                    output_schema=(tc.OutputColumn('value', type_obj),),
                )
                with self.assertRaisesRegex(DbPublishError, 'not supported'):
                    _resolve_payload_schema(payload, sample_size=5000)

    def test_inferred_not_null_name_must_exist(self):
        payload = DbPayload(
            'target', 'bsr', ['id'], [{'id': 1}],
            not_null_columns=('missing',),
        )
        with self.assertRaisesRegex(DbPublishError, 'not present'):
            _resolve_payload_schema(payload, sample_size=5000)


class Test3TechnicalTimestampAndPayloadConstruction(unittest.TestCase):
    def test_technical_timestamp_is_framework_owned_not_null(self):
        payload = DbPayload('target', 'bsr', ['id'], [{'id': 1}])
        spec = tc.PipelineSpec(db_table='target', db_updated_at=True)
        fixed = datetime(2026, 1, 1, tzinfo=timezone.utc)
        apply_db_updated_at(payload, spec, fixed)
        self.assertEqual(payload.columns, ['id', 'etl_updated_at'])
        self.assertEqual(payload.rows[0]['etl_updated_at'], fixed)
        self.assertEqual(payload.framework_columns[0].name, 'etl_updated_at')
        self.assertFalse(payload.framework_columns[0].nullable)

    def test_dynamic_contract_is_rejected_during_pipeline_validation(self):
        class pipeline:
            spec = tc.PipelineSpec(
                db_table='target',
                output_schema=(tc.OutputColumn('id', sa.BigInteger()),),
            )

            @classmethod
            def get_dynamic_db_contract(cls, tbl):
                return {'id': 'id'}

            @classmethod
            def run(cls, ctx):
                return None

        with self.assertRaisesRegex(tc.PipelineContractError, 'get_dynamic_db_contract'):
            tc.validate_pipeline_class(pipeline)

    def test_dynamic_contract_is_rejected_with_declared_schema(self):
        called = []

        class pipeline:
            spec = tc.PipelineSpec(
                db_table='target',
                output_schema=(tc.OutputColumn('id', sa.BigInteger()),),
            )

            @classmethod
            def get_dynamic_db_contract(cls, tbl):
                called.append(True)
                return {'id': 'id'}

        with self.assertRaisesRegex(tc.PipelineContractError, 'get_dynamic_db_contract'):
            _build_db_payload_with_spec(
                pipeline, pd.DataFrame({'id': [1]}), pipeline.spec, 'bsr',
            )
        self.assertEqual(called, [])


class _Transaction:
    def __init__(self, conn):
        self.conn = conn

    def commit(self):
        self.conn.events.append('commit')
        self.conn.in_tx = False

    def rollback(self):
        self.conn.events.append('rollback')
        self.conn.in_tx = False


class _Connection:
    class Dialect:
        name = 'postgresql'

    dialect = Dialect()
    invalidated = False

    def __init__(self):
        self.events = []
        self.in_tx = False

    def begin(self):
        self.in_tx = True
        self.events.append('begin')
        return _Transaction(self)

    def in_transaction(self):
        return self.in_tx

    def execute(self, statement, params=None):
        self.events.append(str(statement))
        return mock.Mock()


class Test4DeclaredPublicationControlFlow(unittest.TestCase):
    def _publisher(self):
        publisher = DbPublisher(creds=_CREDS, schema='bsr', task_name='declared_test')
        publisher._engine = mock.Mock()
        publisher._conn = _Connection()
        publisher._verify_prepared_artifacts = lambda: publisher._conn.events.append('verify')
        publisher._pending_swaps = [('bsr', 'target', 'target__stg_x', 1)]
        publisher._declared_targets = {('bsr', 'target')}
        publisher._resolved_schemas = {
            ('bsr', 'target'): ResolvedSchema(
                (ResolvedColumn('id', sa.BigInteger(), False),), 'declared'
            )
        }
        return publisher

    def test_source_state_finishes_before_the_first_live_lock(self):
        publisher = self._publisher()
        publisher._prepare_declared_targets_before_lock = lambda: set()
        publisher.publication_plan.add('state', lambda: publisher._conn.events.append('state'))
        publisher._lock_publication_targets = lambda deadline, **kwargs: publisher._conn.events.append('lock')
        publisher._refill_declared_target = lambda **kwargs: publisher._conn.events.append('refill')

        publisher._publish_once(100.0)
        events = publisher._conn.events
        self.assertLess(events.index('state'), events.index('lock'))
        self.assertLess(events.index('lock'), events.index('refill'))
        self.assertLess(events.index('refill'), events.index('commit'))

    def test_first_declared_target_is_not_refilled_or_locked(self):
        publisher = self._publisher()
        key = ('bsr', 'target')
        publisher._prepare_declared_targets_before_lock = lambda: {key}
        lock_args = []
        publisher._lock_publication_targets = lambda deadline, **kwargs: lock_args.append(kwargs)
        publisher._refill_declared_target = lambda **kwargs: self.fail('first target was refilled')

        publisher._publish_once(100.0)
        self.assertEqual(lock_args, [{'exclude_targets': {key}}])

    def test_non_table_declared_target_is_rejected_explicitly(self):
        publisher = self._publisher()
        with mock.patch('task_core.db_publish._find_relation', side_effect=[(10, 'r'), (20, 'v')]):
            with self.assertRaisesRegex(DbPublishError, 'view'):
                publisher._prepare_declared_targets_before_lock()

    def test_schema_mismatch_and_incoming_fk_are_rejected_before_lock(self):
        publisher = self._publisher()
        with mock.patch('task_core.db_publish._relation_columns', side_effect=[(('id', 1),), (('id', 2),)]):
            with self.assertRaisesRegex(DbPublishError, 'does not match'):
                publisher._verify_declared_target_compatibility(
                    schema='bsr', table_name='target', staging_name='stg',
                    target_oid=1, staging_oid=2,
                )

        columns = (('id', 20, -1, True, 0, '', ''),)
        with mock.patch('task_core.db_publish._relation_columns', side_effect=[columns, columns]), \
             mock.patch('task_core.db_publish._external_incoming_foreign_keys', return_value=(('x', 'child', 'fk'),)):
            with self.assertRaisesRegex(DbPublishError, 'incoming'):
                publisher._verify_declared_target_compatibility(
                    schema='bsr', table_name='target', staging_name='stg',
                    target_oid=1, staging_oid=2,
                )

    def test_refill_uses_truncate_insert_comment_and_staging_drop(self):
        publisher = self._publisher()
        publisher._set_comment = lambda *args, **kwargs: publisher._conn.events.append('comment')
        publisher._refill_declared_target(
            schema='bsr', table_name='target', staging_name='target__stg_x', rows=1,
        )
        sql = '\n'.join(publisher._conn.events)
        self.assertIn('truncate table "bsr"."target"', sql.lower())
        self.assertIn('insert into "bsr"."target"', sql.lower())
        self.assertIn('drop table "bsr"."target__stg_x"', sql.lower())
        self.assertLess(publisher._conn.events.index('comment'), len(publisher._conn.events))


if __name__ == '__main__':
    unittest.main()
