from __future__ import annotations

import argparse
import ast
import contextlib
import io
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import generate_output_schema as tool


class _FakeCursor:
    def __init__(self, result_sets):
        self.result_sets = list(result_sets)
        self.current = []
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.executions.append((query, params))
        self.current = self.result_sets.pop(0)

    def fetchone(self):
        return self.current[0] if self.current else None

    def fetchall(self):
        return list(self.current)


class _FakeConnection:
    def __init__(self, result_sets=()):
        self.cursor_instance = _FakeCursor(result_sets)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def _column(
    name,
    formatted_type,
    *,
    position=1,
    type_name=None,
    type_kind='b',
    type_schema='pg_catalog',
    not_null=False,
    identity='',
    generated='',
    has_nondefault_collation=False,
    default_expression=None,
):
    return tool.ColumnMetadata(
        position=position,
        name=name,
        formatted_type=formatted_type,
        type_name=type_name or formatted_type.split('(')[0].replace(' ', '_'),
        type_kind=type_kind,
        type_schema=type_schema,
        not_null=not_null,
        identity=identity,
        generated=generated,
        has_nondefault_collation=has_nondefault_collation,
        default_expression=default_expression,
    )


def _inspection(*columns):
    return tool.TableInspection(
        schema='bsr',
        table='customer_summary',
        relation_kind='r',
        columns=tuple(columns),
    )


class CredentialTests(unittest.TestCase):
    def _args(self, **overrides):
        values = {
            'host': None,
            'port': None,
            'dbname': None,
            'user': None,
            'password': None,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_command_line_overrides_inline_and_pgcreds(self):
        with (
            mock.patch.object(tool, '_load_pgcreds', return_value={
                'host': 'pg-host',
                'port': 5432,
                'dbname': 'pg-db',
                'user': 'pg-user',
                'password': 'pg-password',
                'sslmode': 'require',
            }),
            mock.patch.object(tool, 'DB_HOST', 'inline-host'),
            mock.patch.object(tool, 'DB_NAME', 'inline-db'),
            mock.patch.object(tool, 'DB_USER', None),
        ):
            creds = tool.resolve_credentials(self._args(
                host='cli-host',
                user='cli-user',
                password='cli-password',
            ))

        self.assertEqual(creds['host'], 'cli-host')
        self.assertEqual(creds['dbname'], 'inline-db')
        self.assertEqual(creds['user'], 'cli-user')
        self.assertEqual(creds['password'], 'cli-password')
        self.assertEqual(creds['port'], 5432)
        self.assertEqual(creds['sslmode'], 'require')

    def test_missing_credentials_are_reported_together(self):
        with (
            mock.patch.object(tool, '_load_pgcreds', return_value={}),
            mock.patch.object(tool, 'DB_HOST', None),
            mock.patch.object(tool, 'DB_NAME', None),
            mock.patch.object(tool, 'DB_USER', None),
        ):
            with self.assertRaisesRegex(
                tool.SchemaGenerationError,
                'missing host, dbname, user',
            ):
                tool.resolve_credentials(self._args())

    def test_invalid_pgcreds_shape_fails_clearly(self):
        fake_module = type(sys)('pgcreds')
        fake_module.pgcreds = 'not-a-mapping'
        with mock.patch.dict(sys.modules, {'pgcreds': fake_module}):
            with self.assertRaisesRegex(
                tool.SchemaGenerationError,
                'must be a mapping',
            ):
                tool._load_pgcreds()


class CatalogInspectionTests(unittest.TestCase):
    def test_regular_table_is_read_in_physical_column_order(self):
        rows = [
            (1, 'id', 'bigint', 'int8', 'b', 'pg_catalog', True, '', '', False, None),
            (2, 'name', 'text', 'text', 'b', 'pg_catalog', False, '', '', False, None),
        ]
        connection = _FakeConnection([[(91, 'bsr', 'customer_summary', 'r')], rows])

        result = tool.inspect_table(
            connection,
            schema='bsr',
            table='customer_summary',
        )

        self.assertEqual([column.name for column in result.columns], ['id', 'name'])
        self.assertEqual(
            connection.cursor_instance.executions[0][1],
            ('bsr', 'customer_summary'),
        )
        self.assertEqual(connection.cursor_instance.executions[1][1], (91,))
        self.assertNotIn(
            'to_regclass',
            connection.cursor_instance.executions[0][0],
        )

    def test_omitted_schema_resolves_through_active_search_path(self):
        rows = [
            (1, 'id', 'bigint', 'int8', 'b', 'pg_catalog', True, '', '', False, None),
        ]
        connection = _FakeConnection([
            [(101, 'bsr', 'customer_summary', 'r')],
            rows,
        ])

        result = tool.inspect_table(
            connection,
            schema=None,
            table='customer_summary',
        )

        self.assertEqual(result.schema, 'bsr')
        self.assertEqual(result.table, 'customer_summary')
        query, params = connection.cursor_instance.executions[0]
        self.assertIn('to_regclass', query)
        self.assertEqual(params, ('customer_summary',))
        self.assertEqual(connection.cursor_instance.executions[1][1], (101,))

    def test_missing_relation_reports_search_path_resolution(self):
        connection = _FakeConnection([[]])
        with self.assertRaisesRegex(
            tool.SchemaGenerationError,
            'not visible through the active search_path',
        ):
            tool.inspect_table(connection, schema=None, table='missing')

    def test_partitioned_table_is_supported(self):
        row = (1, 'id', 'bigint', 'int8', 'b', 'pg_catalog', True, '', '', False, None)
        connection = _FakeConnection([[(92, 'bsr', 'p', 'p')], [row]])
        result = tool.inspect_table(connection, schema='bsr', table='p')
        self.assertEqual(result.relation_kind, 'p')

    def test_missing_relation_fails(self):
        connection = _FakeConnection([[]])
        with self.assertRaisesRegex(tool.SchemaGenerationError, 'does not exist'):
            tool.inspect_table(connection, schema='bsr', table='missing')

    def test_view_is_rejected(self):
        connection = _FakeConnection([[(93, 'bsr', 'v', 'v')]])
        with self.assertRaisesRegex(tool.SchemaGenerationError, 'is a view'):
            tool.inspect_table(connection, schema='bsr', table='v')

    def test_zero_column_table_is_rejected(self):
        connection = _FakeConnection([[(94, 'bsr', 'empty', 'r')], []])
        with self.assertRaisesRegex(tool.SchemaGenerationError, 'no user columns'):
            tool.inspect_table(connection, schema='bsr', table='empty')


class TypeConversionTests(unittest.TestCase):
    def test_supported_postgresql_families_render_exact_sqlalchemy_types(self):
        formatted_types = {
            'boolean': 'sa.Boolean()',
            'smallint': 'sa.SmallInteger()',
            'integer': 'sa.Integer()',
            'bigint': 'sa.BigInteger()',
            'real': 'sa.REAL()',
            'double precision': 'sa.Double()',
            'numeric': 'sa.Numeric()',
            'numeric(18)': 'sa.Numeric(18)',
            'numeric(18,2)': 'sa.Numeric(18, 2)',
            'text': 'sa.Text()',
            'character varying': 'sa.String()',
            'character varying(120)': 'sa.String(120)',
            'bytea': 'sa.LargeBinary()',
            'date': 'sa.Date()',
            'timestamp without time zone': 'sa.DateTime()',
            'timestamp(6) without time zone': 'sa.DateTime()',
            'timestamp with time zone': 'sa.DateTime(timezone=True)',
        }

        for formatted_type, expected in formatted_types.items():
            with self.subTest(formatted_type=formatted_type):
                self.assertEqual(
                    tool._render_type(_column('value', formatted_type)),
                    expected,
                )


    def test_generated_type_expressions_compile_to_expected_postgresql_types(self):
        import sqlalchemy as sa
        from sqlalchemy.dialects import postgresql

        expected = {
            'sa.Boolean()': 'BOOLEAN',
            'sa.SmallInteger()': 'SMALLINT',
            'sa.Integer()': 'INTEGER',
            'sa.BigInteger()': 'BIGINT',
            'sa.REAL()': 'REAL',
            'sa.Double()': 'DOUBLE PRECISION',
            'sa.Numeric(18, 2)': 'NUMERIC(18, 2)',
            'sa.Text()': 'TEXT',
            'sa.String(120)': 'VARCHAR(120)',
            'sa.LargeBinary()': 'BYTEA',
            'sa.Date()': 'DATE',
            'sa.DateTime()': 'TIMESTAMP WITHOUT TIME ZONE',
            'sa.DateTime(timezone=True)': 'TIMESTAMP WITH TIME ZONE',
        }
        dialect = postgresql.dialect()

        for expression, sql in expected.items():
            with self.subTest(expression=expression):
                type_obj = eval(expression, {'sa': sa})
                rendered = type_obj.compile(dialect=dialect)
                self.assertEqual(rendered, sql)

    def test_nondefault_timestamp_precision_is_rejected(self):
        with self.assertRaisesRegex(
            tool.SchemaGenerationError,
            'cannot preserve non-default timestamp precision exactly',
        ):
            tool._render_type(
                _column('created_at', 'timestamp(3) without time zone')
            )

    def test_unsupported_columns_are_reported_together(self):
        inspection = _inspection(
            _column('payload', 'jsonb'),
            _column('status', 'bsr.status', type_kind='e', type_schema='bsr'),
            _column('amounts', 'numeric[]'),
        )

        with self.assertRaises(tool.SchemaGenerationError) as caught:
            tool.convert_columns(inspection)

        message = str(caught.exception)
        self.assertIn('- payload: jsonb:', message)
        self.assertIn('- status: bsr.status:', message)
        self.assertIn('- amounts: numeric[]:', message)
        self.assertIn('No code was emitted', message)

    def test_domain_identity_generated_and_collation_fail_together(self):
        inspection = _inspection(
            _column('domain_value', 'bsr.money', type_kind='d', type_schema='bsr'),
            _column('identity_id', 'bigint', identity='a'),
            _column('computed', 'integer', generated='s'),
            _column('label', 'text', has_nondefault_collation=True),
        )

        with self.assertRaises(tool.SchemaGenerationError) as caught:
            tool.convert_columns(inspection)

        message = str(caught.exception)
        self.assertIn('domain type', message)
        self.assertIn('identity column', message)
        self.assertIn('generated column', message)
        self.assertIn('non-default text collation', message)

    def test_negative_numeric_scale_is_rejected(self):
        with self.assertRaisesRegex(
            tool.SchemaGenerationError,
            'negative NUMERIC scale',
        ):
            tool._render_type(_column('amount', 'numeric(10,-2)'))

    def test_default_expression_generates_warning_but_not_wrong_type(self):
        inspection = _inspection(
            _column(
                'id',
                'bigint',
                not_null=True,
                default_expression="nextval('customer_id_seq'::regclass)",
            )
        )

        columns, warnings = tool.convert_columns(inspection)

        self.assertEqual(columns[0].sqlalchemy_expression, 'sa.BigInteger()')
        self.assertEqual(len(warnings), 1)
        self.assertIn('not represented by OutputColumn', warnings[0])


class SelectionAndRenderingTests(unittest.TestCase):
    def test_excluded_framework_column_is_omitted(self):
        inspection = _inspection(
            _column('id', 'bigint', position=1),
            _column('etl_updated_at', 'timestamp with time zone', position=2),
        )

        columns, warnings = tool.convert_columns(
            inspection,
            exclude_columns=('etl_updated_at',),
        )

        self.assertEqual([column.name for column in columns], ['id'])
        self.assertEqual(warnings, ())

    def test_unknown_excluded_column_is_rejected(self):
        inspection = _inspection(_column('id', 'bigint'))
        with self.assertRaisesRegex(
            tool.SchemaGenerationError,
            'excluded column.*do not exist',
        ):
            tool.convert_columns(inspection, exclude_columns=('missing',))

    def test_invalid_column_names_are_rejected_before_output(self):
        """Columns take the wider rule; they are not exempt from a rule.

        Since 0.7.5 a column may carry dots, so the message names the column
        contract rather than the portable-identifier one. Upper case and
        spaces stay rejected either way.
        """
        for name in ('Customer ID', 'lev..1', '.lev', 'lev-1'):
            with self.subTest(column=name):
                inspection = _inspection(_column(name, 'bigint'))
                with self.assertRaisesRegex(
                    tool.SchemaGenerationError,
                    'not a valid task_core published column name',
                ):
                    tool.convert_columns(inspection)

    def test_dotted_column_names_are_accepted(self):
        """`lev.1` is ordinary analytical vocabulary, not garbage.

        The generator reads a real catalog, so rejecting it here would make
        the tool unable to describe tables task_core can now publish.
        """
        inspection = _inspection(
            _column('lev.1', 'bigint', not_null=True),
            _column('metric.plan_2026', 'text'),
        )
        columns, _ = tool.convert_columns(inspection)
        self.assertEqual(
            [c.name for c in columns], ['lev.1', 'metric.plan_2026'],
        )

    def test_the_generator_column_rule_matches_task_core(self):
        """The tool is standalone and cannot import task_core, so the two
        copies of the pattern are kept in step here rather than by sharing a
        constant."""
        from task_core.types import PORTABLE_IDENTIFIER_RE, PUBLISHED_COLUMN_RE
        self.assertEqual(
            tool._PUBLISHED_COLUMN_RE.pattern, PUBLISHED_COLUMN_RE.pattern,
        )
        self.assertEqual(
            tool._PORTABLE_IDENTIFIER_RE.pattern, PORTABLE_IDENTIFIER_RE.pattern,
        )

    def test_pipeline_argument_is_paste_ready_inside_pipeline_spec(self):
        inspection = _inspection(
            _column('id', 'bigint', not_null=True),
            _column('name', 'text'),
        )
        columns, _ = tool.convert_columns(inspection)

        code = tool.render_output_schema(
            inspection,
            columns,
            style='pipeline-argument',
            output_name='OUTPUT_SCHEMA',
        )

        self.assertIn('        output_schema=(', code)
        self.assertIn(
            "            OutputColumn('id', sa.BigInteger(), nullable=False),",
            code,
        )
        wrapped = 'class Example:\n    spec = PipelineSpec(\n' + code + '    )\n'
        ast.parse(wrapped)

    def test_nullable_true_uses_outputcolumn_default(self):
        inspection = _inspection(_column('name', 'text'))
        columns, _ = tool.convert_columns(inspection)

        code = tool.render_output_schema(
            inspection,
            columns,
            style='pipeline-argument',
            output_name='OUTPUT_SCHEMA',
        )

        self.assertIn("OutputColumn('name', sa.Text()),", code)
        self.assertNotIn('nullable=True', code)

    def test_class_constant_is_paste_ready_inside_task_class(self):
        inspection = _inspection(_column('id', 'bigint', not_null=True))
        columns, _ = tool.convert_columns(inspection)

        code = tool.render_output_schema(
            inspection,
            columns,
            style='class-constant',
            output_name='OUTPUT_SCHEMA',
        )

        self.assertIn('    OUTPUT_SCHEMA = (', code)
        ast.parse('class Example:\n' + code)


    def test_large_existing_table_generates_complete_valid_class_code(self):
        inspection = _inspection(*(
            _column(f'column_{index:03d}', 'bigint', position=index)
            for index in range(1, 151)
        ))
        columns, warnings = tool.convert_columns(inspection)

        code = tool.render_output_schema(
            inspection,
            columns,
            style='pipeline-argument',
            output_name='OUTPUT_SCHEMA',
        )

        self.assertEqual(code.count('OutputColumn('), 150)
        self.assertNotIn('nullable=True', code)
        self.assertEqual(warnings, ())
        ast.parse('class Example:\n    spec = PipelineSpec(\n' + code + '    )\n')

    def test_long_column_name_uses_multiline_black_compatible_shape(self):
        name = 'this_is_a_very_long_but_still_portable_column_name_for_a_report'
        inspection = _inspection(_column(name, 'timestamp with time zone'))
        columns, _ = tool.convert_columns(inspection)

        code = tool.render_output_schema(
            inspection,
            columns,
            style='pipeline-argument',
            output_name='OUTPUT_SCHEMA',
        )

        self.assertIn('            OutputColumn(\n', code)
        self.assertIn(f"                {name!r},\n", code)

    def test_invalid_output_name_is_rejected(self):
        inspection = _inspection(_column('id', 'bigint'))
        columns, _ = tool.convert_columns(inspection)
        with self.assertRaisesRegex(
            tool.SchemaGenerationError,
            'not a valid Python identifier',
        ):
            tool.render_output_schema(
                inspection,
                columns,
                style='class-constant',
                output_name='not-valid',
            )


class ExecutionTests(unittest.TestCase):

    def test_connect_closes_partial_connection_when_readonly_setup_fails(self):
        connection = mock.Mock()
        connection.set_session.side_effect = RuntimeError('readonly failed')
        fake_psycopg2 = type(sys)('psycopg2')
        fake_psycopg2.connect = mock.Mock(return_value=connection)

        with mock.patch.dict(sys.modules, {'psycopg2': fake_psycopg2}):
            with self.assertRaisesRegex(
                tool.SchemaGenerationError,
                'readonly failed',
            ):
                tool._connect({'host': 'x', 'dbname': 'x', 'user': 'x'})

        connection.set_session.assert_called_once_with(
            readonly=True,
            autocommit=True,
        )
        connection.close.assert_called_once_with()

    def test_main_reports_output_write_failure(self):
        code = '        output_schema=(\n        ),\n'
        parser = tool.build_parser()
        args = parser.parse_args([
            '--table', 'customer_summary',
            '--schema', 'bsr',
            '--output', 'missing-parent/schema.py',
        ])

        with (
            mock.patch.object(tool, 'build_parser') as build_parser,
            mock.patch.object(tool, 'generate', return_value=(code, ())),
            contextlib.redirect_stderr(io.StringIO()) as stderr,
        ):
            build_parser.return_value.parse_args.return_value = args
            result = tool.main([])

        self.assertEqual(result, 2)
        self.assertIn('cannot write', stderr.getvalue())


    def test_standalone_cli_uses_pgcreds_with_command_line_override(self):
        fake_pgcreds = """
pgcreds = {
    'host': 'pg-host',
    'port': 5432,
    'dbname': 'analytics',
    'user': 'reporter',
    'sslmode': 'require',
    'options': '-c search_path=bsr,public',
}
"""
        fake_psycopg2 = """
class Cursor:
    def __init__(self):
        self.step = 0
        self.current = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.step += 1
        if self.step == 1:
            assert 'to_regclass' in query
            assert params == ('customer_summary',)
            self.current = [(123, 'bsr', 'customer_summary', 'r')]
        else:
            assert params == (123,)
            self.current = [
                (1, 'id', 'bigint', 'int8', 'b', 'pg_catalog', True, '', '', False, None),
                (2, 'name', 'text', 'text', 'b', 'pg_catalog', False, '', '', False, None),
            ]

    def fetchone(self):
        return self.current[0] if self.current else None

    def fetchall(self):
        return list(self.current)


class Connection:
    def __init__(self):
        self.cursor_value = Cursor()

    def set_session(self, *, readonly, autocommit):
        assert readonly is True
        assert autocommit is True

    def cursor(self):
        return self.cursor_value

    def close(self):
        pass


def connect(**kwargs):
    assert kwargs['host'] == 'cli-host'
    assert kwargs['dbname'] == 'analytics'
    assert kwargs['user'] == 'reporter'
    assert kwargs['sslmode'] == 'require'
    assert kwargs['options'] == '-c search_path=bsr,public'
    return Connection()
"""

        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            tools_dir = temp.joinpath('tools')
            tools_dir.mkdir()
            script_path = tools_dir.joinpath('generate_output_schema.py')
            script_path.write_text(
                Path('tools/generate_output_schema.py').read_text(encoding='utf-8'),
                encoding='utf-8',
            )
            temp.joinpath('pgcreds.py').write_text(fake_pgcreds, encoding='utf-8')
            temp.joinpath('psycopg2.py').write_text(fake_psycopg2, encoding='utf-8')
            result = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    '--table', 'customer_summary',
                    '--host', 'cli-host',
                ],
                capture_output=True,
                text=True,
                check=False,
                cwd=temp,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('        # Generated from bsr.customer_summary.', result.stdout)
        self.assertIn('        output_schema=(', result.stdout)
        self.assertIn(
            "OutputColumn('id', sa.BigInteger(), nullable=False)",
            result.stdout,
        )
        self.assertIn("OutputColumn('name', sa.Text())", result.stdout)
        self.assertNotIn('nullable=True', result.stdout)
        self.assertEqual(result.stderr, '')

    def test_help_is_available_from_standalone_script(self):
        result = subprocess.run(
            [sys.executable, 'tools/generate_output_schema.py', '--help'],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--table', result.stdout)
        self.assertIn('--exclude-column', result.stdout)
        self.assertIn('active search_path', result.stdout)
        self.assertIn('Without command-line arguments', result.stdout)

    def test_main_writes_utf8_output_file_and_warning(self):
        parser = tool.build_parser()
        args = parser.parse_args([
            '--table', 'customer_summary',
            '--schema', 'bsr',
        ])
        inspection = _inspection(
            _column('name', 'text', default_expression="'unknown'::text")
        )
        connection = _FakeConnection()

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp, 'schema.py')
            with (
                mock.patch.object(tool, '_load_pgcreds', return_value={
                    'host': 'x',
                    'dbname': 'x',
                    'user': 'x',
                }),
                mock.patch.object(tool, '_connect', return_value=connection),
                mock.patch.object(tool, 'inspect_table', return_value=inspection),
                contextlib.redirect_stderr(io.StringIO()) as stderr,
            ):
                args.output = str(output_path)
                code, warnings = tool.generate(args)
                output_path.write_text(code, encoding='utf-8')
                for warning in warnings:
                    print(f'warning: {warning}', file=sys.stderr)

            self.assertTrue(connection.closed)
            self.assertIn('output_schema=(', output_path.read_text(encoding='utf-8'))
            self.assertIn('warning:', stderr.getvalue())

    def test_generate_closes_connection_when_introspection_fails(self):
        parser = tool.build_parser()
        args = parser.parse_args([
            '--table', 'customer_summary',
            '--schema', 'bsr',
        ])
        connection = _FakeConnection()

        with (
            mock.patch.object(tool, '_load_pgcreds', return_value={
                'host': 'x',
                'dbname': 'x',
                'user': 'x',
            }),
            mock.patch.object(tool, '_connect', return_value=connection),
            mock.patch.object(
                tool,
                'inspect_table',
                side_effect=tool.SchemaGenerationError('broken catalog'),
            ),
        ):
            with self.assertRaisesRegex(tool.SchemaGenerationError, 'broken catalog'):
                tool.generate(args)

        self.assertTrue(connection.closed)

    def test_no_argument_mode_uses_inline_table_and_schema(self):
        parser = tool.build_parser()
        args = parser.parse_args([])
        with (
            mock.patch.object(tool, 'TABLE_NAME', 'inline_table'),
            mock.patch.object(tool, 'SCHEMA_NAME', 'inline_schema'),
        ):
            args = tool._effective_args(args)
        self.assertEqual(args.table, 'inline_table')
        self.assertEqual(args.schema, 'inline_schema')

    def test_no_argument_mode_uses_search_path_when_schema_is_none(self):
        parser = tool.build_parser()
        args = parser.parse_args([])
        with (
            mock.patch.object(tool, 'TABLE_NAME', 'inline_table'),
            mock.patch.object(tool, 'SCHEMA_NAME', None),
        ):
            args = tool._effective_args(args)
            tool._validate_request(args)
        self.assertEqual(args.table, 'inline_table')
        self.assertIsNone(args.schema)

    def test_missing_table_explains_both_entry_modes(self):
        parser = tool.build_parser()
        args = parser.parse_args([])
        with mock.patch.object(tool, 'TABLE_NAME', ''):
            args = tool._effective_args(args)
            with self.assertRaisesRegex(
                tool.SchemaGenerationError,
                'pass --table or edit TABLE_NAME',
            ):
                tool._validate_request(args)

    def test_psycopg2_missing_message_is_actionable(self):
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == 'psycopg2':
                raise ImportError('missing')
            return real_import(name, *args, **kwargs)

        with mock.patch('builtins.__import__', side_effect=fake_import):
            with self.assertRaisesRegex(
                tool.SchemaGenerationError,
                'Install repository requirements',
            ):
                tool._connect({'host': 'x', 'dbname': 'x', 'user': 'x'})


class DocumentationTests(unittest.TestCase):
    def test_task_authoring_documents_both_execution_modes_and_fail_policy(self):
        text = Path('docs/task-authoring.md').read_text(encoding='utf-8')
        required = (
            'tools/generate_output_schema.py',
            '--style class-constant',
            '`SCHEMA_NAME = None` to use the active PostgreSQL `search_path`',
            'active PostgreSQL `search_path`',
            'pgcreds.pgcreds',
            'no partial code is emitted',
            '--exclude-column etl_updated_at',
        )
        for phrase in required:
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, text)

    def test_readme_links_to_schema_generator_guidance(self):
        text = Path('README.md').read_text(encoding='utf-8')
        self.assertIn('python tools/generate_output_schema.py', text)
        self.assertIn(
            'generate-a-declaration-from-an-existing-table',
            text,
        )

    def test_release_version_and_architecture_claim_are_current(self):
        facade = Path('task_core/__init__.py').read_text(encoding='utf-8')
        architecture = Path('docs/architecture.md').read_text(encoding='utf-8')
        match = re.search(r"^__version__ = '([^']+)'$", facade, re.M)
        self.assertIsNotNone(match)
        self.assertIn(f'as of {match.group(1)}', architecture)


if __name__ == '__main__':
    unittest.main()
