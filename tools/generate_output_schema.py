# -*- coding: utf-8 -*-
"""Generate task_core ``output_schema`` code from an existing PostgreSQL table.

Standalone command-line use through the active PostgreSQL ``search_path``::

    python tools/generate_output_schema.py --table customer_summary

Pass ``--schema`` to bypass ``search_path`` and inspect one explicit schema::

    python tools/generate_output_schema.py --schema bsr --table customer_summary

Complete command-line example using every supported option::

    python tools/generate_output_schema.py --host db.example.com --port 5432 --dbname analytics --user schema_reader --password secret-password --schema reporting --table customer_snapshot --style class-constant --name CUSTOMER_SNAPSHOT_SCHEMA --exclude-column etl_updated_at --output customer_snapshot_schema.py

Repeat ``--exclude-column`` to omit multiple framework-owned columns. Command-line
connection values override only the corresponding inline ``DB_*`` or ``pgcreds``
keys. Supplying ``--password`` may expose it in shell history; prefer ``pgcreds.py``
or ``.pgpass`` when possible.

Notebook / editor use without command-line arguments:

1. Edit ``TABLE_NAME`` in the configuration block below.
2. Leave ``SCHEMA_NAME`` as ``None`` to use the connection's active
   ``search_path``, or set it to one explicit schema.
3. Ensure ``pgcreds.py`` is importable.
4. Run this file. The generated Python code is printed to stdout.

Credential precedence is command line, inline ``DB_*`` values, then the
``pgcreds`` mapping when it is importable. The script performs read-only
catalog introspection and never changes the database.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))


# ---------------------------------------------------------------------------
# Notebook / no-command-line configuration.
# Edit TABLE_NAME, and optionally SCHEMA_NAME, then run this file.
# Leave SCHEMA_NAME as None to resolve the table through PostgreSQL search_path.
# ---------------------------------------------------------------------------

TABLE_NAME = ''
SCHEMA_NAME: str | None = None
OUTPUT_STYLE = 'pipeline-argument'
OUTPUT_NAME = 'OUTPUT_SCHEMA'
EXCLUDE_COLUMNS: tuple[str, ...] = ()
OUTPUT_FILE: str | None = None

# Usually leave these as None and let pgcreds.py supply the connection.
DB_HOST: str | None = None
DB_PORT: int | None = None
DB_NAME: str | None = None
DB_USER: str | None = None
DB_PASSWORD: str | None = None


_PORTABLE_IDENTIFIER_RE = re.compile(r'^[a-z_][a-z0-9_]*$')
# Columns take the wider contract: a dot may separate parts. Relations do
# not. Mirrors task_core.types -- this script is standalone by design and
# must not import task_core, so the two patterns are kept in step by
# tools/tests, not by sharing a constant. See decisions/0014.
_PUBLISHED_COLUMN_RE = re.compile(r'^[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*$')
_PYTHON_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_NUMERIC_RE = re.compile(r'^numeric(?:\(([-+]?\d+)(?:,([-+]?\d+))?\))?$')
_VARCHAR_RE = re.compile(r'^character varying(?:\((\d+)\))?$')
_TIMESTAMP_RE = re.compile(
    r'^timestamp(?:\((\d+)\))? (without|with) time zone$'
)

_RELATION_KINDS = {
    'r': 'ordinary table',
    'p': 'partitioned table',
    'v': 'view',
    'm': 'materialized view',
    'f': 'foreign table',
}

_RELATION_QUERY_BY_SCHEMA = '''
SELECT c.oid, n.nspname, c.relname, c.relkind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = c.relnamespace
WHERE n.nspname = %s
  AND c.relname = %s
'''

_RELATION_QUERY_BY_SEARCH_PATH = '''
SELECT c.oid, n.nspname, c.relname, c.relkind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = c.relnamespace
WHERE c.oid = pg_catalog.to_regclass(%s)
'''

_COLUMN_QUERY = '''
SELECT
    a.attnum,
    a.attname,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
    t.typname,
    t.typtype,
    tn.nspname AS type_schema,
    a.attnotnull,
    a.attidentity,
    a.attgenerated,
    (a.attcollation <> t.typcollation) AS has_nondefault_collation,
    CASE
        WHEN ad.oid IS NULL THEN NULL
        ELSE pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
    END AS default_expression
FROM pg_catalog.pg_attribute AS a
JOIN pg_catalog.pg_type AS t
  ON t.oid = a.atttypid
JOIN pg_catalog.pg_namespace AS tn
  ON tn.oid = t.typnamespace
LEFT JOIN pg_catalog.pg_attrdef AS ad
  ON ad.adrelid = a.attrelid
 AND ad.adnum = a.attnum
WHERE a.attrelid = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
'''


class SchemaGenerationError(RuntimeError):
    """The existing table cannot be represented by task_core output_schema."""


@dataclass(frozen=True)
class ColumnMetadata:
    position: int
    name: str
    formatted_type: str
    type_name: str
    type_kind: str
    type_schema: str
    not_null: bool
    identity: str
    generated: str
    has_nondefault_collation: bool
    default_expression: str | None


@dataclass(frozen=True)
class GeneratedColumn:
    name: str
    sqlalchemy_expression: str
    nullable: bool


@dataclass(frozen=True)
class TableInspection:
    schema: str
    table: str
    relation_kind: str
    columns: tuple[ColumnMetadata, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Generate paste-ready task_core output_schema Python code from '
            'an existing PostgreSQL table. The database is inspected read-only.'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Examples:
  python tools/generate_output_schema.py --schema bsr --table customer_summary
  python tools/generate_output_schema.py --schema bsr --table customer_summary \\
      --host localhost --port 5432 --dbname analytics --user analyst
  python tools/generate_output_schema.py --schema bsr --table customer_summary \\
      --exclude-column etl_updated_at --style class-constant

Without command-line arguments, edit TABLE_NAME at the top of this file.
Leave SCHEMA_NAME as None to use the active PostgreSQL search_path, or set it
to one explicit schema. Command-line connection values override inline DB_*
values and pgcreds.py.''',
    )
    parser.add_argument('--table', help='existing PostgreSQL table name')
    parser.add_argument(
        '--schema',
        help=(
            'existing PostgreSQL schema name; omit to resolve the table '
            'through the active search_path'
        ),
    )
    parser.add_argument('--host', help='PostgreSQL host; overrides pgcreds')
    parser.add_argument('--port', type=int, help='PostgreSQL port; overrides pgcreds')
    parser.add_argument(
        '--dbname', '--database', dest='dbname',
        help='PostgreSQL database name; overrides pgcreds',
    )
    parser.add_argument('--user', help='PostgreSQL user; overrides pgcreds')
    parser.add_argument(
        '--password',
        help='PostgreSQL password; prefer pgcreds/.pgpass when possible',
    )
    parser.add_argument(
        '--style',
        choices=('pipeline-argument', 'class-constant'),
        help=(
            "'pipeline-argument' emits an eight-space output_schema argument; "
            "'class-constant' emits a four-space named tuple"
        ),
    )
    parser.add_argument(
        '--name',
        help='class-constant name; default is OUTPUT_NAME from this file',
    )
    parser.add_argument(
        '--exclude-column',
        action='append',
        dest='exclude_columns',
        help='omit one column; repeat for multiple framework-owned columns',
    )
    parser.add_argument(
        '--output',
        help='write generated code to this UTF-8 file instead of stdout',
    )
    return parser


def _load_pgcreds() -> dict[str, Any]:
    try:
        from pgcreds import pgcreds
    except ModuleNotFoundError as exc:
        if exc.name == 'pgcreds':
            return {}
        raise

    if not isinstance(pgcreds, Mapping):
        raise SchemaGenerationError(
            f'pgcreds.pgcreds must be a mapping, got {type(pgcreds).__name__}'
        )
    return dict(pgcreds)


def resolve_credentials(args: argparse.Namespace) -> dict[str, Any]:
    creds = _load_pgcreds()

    inline_values = {
        'host': DB_HOST,
        'port': DB_PORT,
        'dbname': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD,
    }
    command_values = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password,
    }

    for values in (inline_values, command_values):
        for key, value in values.items():
            if value is not None:
                creds[key] = value

    missing = [key for key in ('host', 'dbname', 'user') if not creds.get(key)]
    if missing:
        joined = ', '.join(missing)
        raise SchemaGenerationError(
            f'PostgreSQL credentials are incomplete; missing {joined}. '
            'Provide pgcreds.py, edit DB_* values, or pass command-line overrides.'
        )
    return creds


def _connect(creds: Mapping[str, Any]):
    try:
        import psycopg2
    except ImportError as exc:
        raise SchemaGenerationError(
            'psycopg2 is required to inspect PostgreSQL. Install repository '
            'requirements before running this tool.'
        ) from exc

    connection = None
    try:
        connection = psycopg2.connect(**dict(creds))
        connection.set_session(readonly=True, autocommit=True)
        return connection
    except Exception as exc:
        if connection is not None:
            connection.close()
        raise SchemaGenerationError(f'PostgreSQL connection failed: {exc}') from exc


def inspect_table(
    connection,
    *,
    schema: str | None,
    table: str,
) -> TableInspection:
    with connection.cursor() as cursor:
        if schema is None:
            cursor.execute(_RELATION_QUERY_BY_SEARCH_PATH, (table,))
        else:
            cursor.execute(_RELATION_QUERY_BY_SCHEMA, (schema, table))

        relation_row = cursor.fetchone()
        if relation_row is None:
            if schema is None:
                raise SchemaGenerationError(
                    f'PostgreSQL relation {table!r} does not exist or is not '
                    'visible through the active search_path'
                )
            raise SchemaGenerationError(
                f'PostgreSQL relation {schema}.{table} does not exist'
            )

        relation_oid, resolved_schema, resolved_table, relation_kind = relation_row
        qualified_name = f'{resolved_schema}.{resolved_table}'
        if relation_kind not in ('r', 'p'):
            description = _RELATION_KINDS.get(
                relation_kind,
                f'unsupported relation kind {relation_kind!r}',
            )
            raise SchemaGenerationError(
                f'{qualified_name} is a {description}, not a publishable table'
            )

        cursor.execute(_COLUMN_QUERY, (relation_oid,))
        rows = cursor.fetchall()

    if not rows:
        raise SchemaGenerationError(
            f'PostgreSQL table {qualified_name} has no user columns'
        )

    columns = tuple(
        ColumnMetadata(
            position=row[0],
            name=row[1],
            formatted_type=row[2],
            type_name=row[3],
            type_kind=row[4],
            type_schema=row[5],
            not_null=row[6],
            identity=row[7],
            generated=row[8],
            has_nondefault_collation=row[9],
            default_expression=row[10],
        )
        for row in rows
    )
    return TableInspection(
        schema=resolved_schema,
        table=resolved_table,
        relation_kind=relation_kind,
        columns=columns,
    )


def _identifier_problem(value: str, *, label: str, column: bool = False) -> str | None:
    if len(value.encode('utf-8')) > 63:
        return f'{label} exceeds PostgreSQL/task_core 63-byte identifier limit'
    pattern = _PUBLISHED_COLUMN_RE if column else _PORTABLE_IDENTIFIER_RE
    if not pattern.fullmatch(value):
        if column:
            return (
                f'{label} is not a valid task_core published column name; '
                f'expected {pattern.pattern}'
            )
        return (
            f'{label} is not a portable task_core identifier; expected '
            "^[a-z_][a-z0-9_]*$"
        )
    return None


def _render_type(column: ColumnMetadata) -> str:
    formatted = column.formatted_type

    if column.type_kind == 'd':
        raise SchemaGenerationError(
            f'domain type {column.type_schema}.{column.type_name} is not '
            'representable by task_core output_schema'
        )
    if column.type_kind == 'e':
        raise SchemaGenerationError(
            f'enum type {column.type_schema}.{column.type_name} is not '
            'supported by task_core output_schema'
        )
    if column.has_nondefault_collation:
        raise SchemaGenerationError(
            'non-default text collation is not representable by task_core '
            'output_schema'
        )
    if column.identity:
        mode = 'always' if column.identity == 'a' else 'by default'
        raise SchemaGenerationError(
            f'identity column ({mode}) is not representable by OutputColumn'
        )
    if column.generated:
        raise SchemaGenerationError(
            'generated column expression is not representable by OutputColumn'
        )

    simple = {
        'boolean': 'sa.Boolean()',
        'smallint': 'sa.SmallInteger()',
        'integer': 'sa.Integer()',
        'bigint': 'sa.BigInteger()',
        'real': 'sa.REAL()',
        'double precision': 'sa.Double()',
        'text': 'sa.Text()',
        'bytea': 'sa.LargeBinary()',
        'date': 'sa.Date()',
    }
    if formatted in simple:
        return simple[formatted]

    numeric_match = _NUMERIC_RE.fullmatch(formatted)
    if numeric_match:
        precision_text, scale_text = numeric_match.groups()
        if precision_text is None:
            return 'sa.Numeric()'
        precision = int(precision_text)
        if scale_text is None:
            return f'sa.Numeric({precision})'
        scale = int(scale_text)
        if scale < 0:
            raise SchemaGenerationError(
                f'negative NUMERIC scale {scale} is outside the supported '
                'task_core declared-schema subset'
            )
        if scale > precision:
            raise SchemaGenerationError(
                f'NUMERIC scale {scale} exceeds precision {precision}'
            )
        return f'sa.Numeric({precision}, {scale})'

    varchar_match = _VARCHAR_RE.fullmatch(formatted)
    if varchar_match:
        length_text = varchar_match.group(1)
        if length_text is None:
            return 'sa.String()'
        return f'sa.String({int(length_text)})'

    timestamp_match = _TIMESTAMP_RE.fullmatch(formatted)
    if timestamp_match:
        precision_text, awareness = timestamp_match.groups()
        if precision_text is not None and int(precision_text) != 6:
            raise SchemaGenerationError(
                f'{formatted} has explicit precision {precision_text}; '
                'task_core OutputColumn cannot preserve non-default timestamp '
                'precision exactly'
            )
        if awareness == 'with':
            return 'sa.DateTime(timezone=True)'
        return 'sa.DateTime()'

    raise SchemaGenerationError(
        f'PostgreSQL type {formatted!r} is not supported by task_core '
        'output_schema; migrate the table or declare the contract manually'
    )


def convert_columns(
    inspection: TableInspection,
    *,
    exclude_columns: Sequence[str] = (),
) -> tuple[tuple[GeneratedColumn, ...], tuple[str, ...]]:
    excluded = set(exclude_columns)
    existing_names = {column.name for column in inspection.columns}
    unknown_exclusions = sorted(excluded - existing_names)
    if unknown_exclusions:
        raise SchemaGenerationError(
            f'excluded column(s) do not exist in {inspection.schema}.{inspection.table}: '
            f'{unknown_exclusions}'
        )

    problems: list[str] = []
    warnings: list[str] = []
    generated: list[GeneratedColumn] = []

    for column in inspection.columns:
        if column.name in excluded:
            continue

        identifier_problem = _identifier_problem(
            column.name,
            label=f'column {column.name!r}',
            column=True,
        )
        if identifier_problem is not None:
            problems.append(f'- {column.name}: {identifier_problem}')
            continue

        try:
            expression = _render_type(column)
        except SchemaGenerationError as exc:
            problems.append(
                f'- {column.name}: {column.formatted_type}: {exc}'
            )
            continue

        generated.append(
            GeneratedColumn(
                name=column.name,
                sqlalchemy_expression=expression,
                nullable=not column.not_null,
            )
        )

        if column.default_expression is not None:
            warnings.append(
                f'{column.name}: default {column.default_expression!r} is not '
                'represented by OutputColumn; refill preserves the existing '
                'default, while replace recreates the table without it'
            )

    if problems:
        details = '\n'.join(problems)
        raise SchemaGenerationError(
            f'{inspection.schema}.{inspection.table} cannot be represented '
            f'exactly as task_core output_schema:\n{details}\n'
            'No code was emitted. Migrate the destination manually, exclude a '
            'framework-owned column where appropriate, or declare the schema manually.'
        )

    if not generated:
        raise SchemaGenerationError(
            'all table columns were excluded; output_schema must contain at '
            'least one user-owned column'
        )

    return tuple(generated), tuple(warnings)


def _render_column_line(column: GeneratedColumn, *, indent: int) -> list[str]:
    prefix = ' ' * indent
    nullable_argument = '' if column.nullable else ', nullable=False'
    one_line = (
        f'{prefix}OutputColumn({column.name!r}, '
        f'{column.sqlalchemy_expression}{nullable_argument}),'
    )
    if len(one_line) <= 88:
        return [one_line]

    lines = [
        f'{prefix}OutputColumn(',
        f'{prefix}    {column.name!r},',
        f'{prefix}    {column.sqlalchemy_expression},',
    ]
    if not column.nullable:
        lines.append(f'{prefix}    nullable=False,')
    lines.append(f'{prefix}),')
    return lines


def render_output_schema(
    inspection: TableInspection,
    columns: Sequence[GeneratedColumn],
    *,
    style: str,
    output_name: str,
) -> str:
    if style not in ('pipeline-argument', 'class-constant'):
        raise SchemaGenerationError(f'unsupported output style {style!r}')
    if (
        style == 'class-constant'
        and not _PYTHON_IDENTIFIER_RE.fullmatch(output_name)
    ):
        raise SchemaGenerationError(
            f'output name {output_name!r} is not a valid Python identifier'
        )

    if style == 'pipeline-argument':
        base_indent = 8
        opening = 'output_schema=('
        closing = '),'
    else:
        base_indent = 4
        opening = f'{output_name} = ('
        closing = ')'

    lines = [
        f"{' ' * base_indent}# Generated from "
        f'{inspection.schema}.{inspection.table}.',
        f"{' ' * base_indent}{opening}",
    ]
    for column in columns:
        lines.extend(_render_column_line(column, indent=base_indent + 4))
    lines.append(f"{' ' * base_indent}{closing}")
    return '\n'.join(lines) + '\n'


def _running_in_notebook() -> bool:
    return 'ipykernel' in sys.modules


def _effective_args(args: argparse.Namespace) -> argparse.Namespace:
    args.table = args.table if args.table is not None else TABLE_NAME
    args.schema = args.schema if args.schema is not None else SCHEMA_NAME
    args.style = args.style if args.style is not None else OUTPUT_STYLE
    args.name = args.name if args.name is not None else OUTPUT_NAME
    if args.exclude_columns is None:
        args.exclude_columns = list(EXCLUDE_COLUMNS)
    args.output = args.output if args.output is not None else OUTPUT_FILE
    return args


def _validate_request(args: argparse.Namespace):
    if not args.table:
        raise SchemaGenerationError(
            'table name is required; pass --table or edit TABLE_NAME at the '
            'top of this file'
        )
    if args.style not in ('pipeline-argument', 'class-constant'):
        raise SchemaGenerationError(
            f'OUTPUT_STYLE must be pipeline-argument or class-constant, '
            f'got {args.style!r}'
        )
    if (
        args.style == 'class-constant'
        and not _PYTHON_IDENTIFIER_RE.fullmatch(args.name)
    ):
        raise SchemaGenerationError(
            f'output name {args.name!r} is not a valid Python identifier'
        )

    identifier_checks = [
        _identifier_problem(args.table, label=f'table {args.table!r}'),
    ]
    if args.schema is not None:
        identifier_checks.insert(
            0,
            _identifier_problem(args.schema, label=f'schema {args.schema!r}'),
        )
    problems = [
        problem for problem in identifier_checks if problem is not None
    ]
    if problems:
        raise SchemaGenerationError('; '.join(problems))


def generate(args: argparse.Namespace) -> tuple[str, tuple[str, ...]]:
    args = _effective_args(args)
    _validate_request(args)
    creds = resolve_credentials(args)
    connection = _connect(creds)
    try:
        inspection = inspect_table(
            connection,
            schema=args.schema,
            table=args.table,
        )
    finally:
        connection.close()

    columns, warnings = convert_columns(
        inspection,
        exclude_columns=args.exclude_columns,
    )
    code = render_output_schema(
        inspection,
        columns,
        style=args.style,
        output_name=args.name,
    )
    return code, warnings


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        code, warnings = generate(args)
    except SchemaGenerationError as exc:
        print(f'error: {exc}', file=sys.stderr)
        return 2

    for warning in warnings:
        print(f'warning: {warning}', file=sys.stderr)

    if args.output is not None:
        output_path = Path(args.output)
        try:
            output_path.write_text(code, encoding='utf-8')
        except OSError as exc:
            print(f'error: cannot write {output_path}: {exc}', file=sys.stderr)
            return 2
        print(f'wrote {output_path}', file=sys.stderr)
    else:
        print(code, end='')
    return 0


if __name__ == '__main__':
    notebook_argv: Sequence[str] | None = [] if _running_in_notebook() else None
    raise SystemExit(main(notebook_argv))
