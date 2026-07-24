# -*- coding: utf-8 -*-

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import logging
from typing import Any, Iterator

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.pool import NullPool

from task_core.cleanup import attempt_all_cleanup
from task_core.types import find_duplicates


@dataclass(frozen=True)
class DbTableResult:
    schema: str
    table_name: str
    full_name: str
    rows: int
    db_table_id_pix: Any | None = None


@dataclass
class DbPayload:
    table_name: str
    schema: str
    columns: list[str]
    rows: list[dict[str, Any]]
    type_overrides: dict[str, Any] | None = None
    db_table_id_pix: Any | None = None


class DbPublishError(RuntimeError):
    pass


_REQUIRED_CRED_KEYS = ('user', 'host', 'dbname')


def validate_pg_creds(creds, *, context='Postgres credentials'):
    if creds is None:
        raise DbPublishError(
            f'{context}: credentials are not configured; pass creds explicitly or provide pgcreds.py'
        )

    if not isinstance(creds, Mapping):
        raise DbPublishError(f'{context}: expected a mapping/dict, got {type(creds).__name__}')

    missing = [key for key in _REQUIRED_CRED_KEYS if not creds.get(key)]
    if missing:
        raise DbPublishError(f'{context}: missing required field(s): {", ".join(missing)}')

    return creds


_TYPE_OVERRIDES = {
    'TEXT': sa.Text,
    'STRING': sa.Text,
    'VARCHAR': sa.Text,
    'CHAR': sa.Text,
    'SMALLINT': sa.SmallInteger,
    'INT2': sa.SmallInteger,
    'BIGINT': sa.BigInteger,
    'INT8': sa.BigInteger,
    'INTEGER': sa.Integer,
    'INT4': sa.Integer,
    'INT': sa.Integer,
    'NUMERIC': sa.Numeric,
    'DECIMAL': sa.Numeric,
    'FLOAT': sa.Float,
    'REAL': sa.REAL,
    'FLOAT4': sa.REAL,
    'DOUBLE': sa.Double,
    'DOUBLE PRECISION': sa.Double,
    'FLOAT8': sa.Double,
    'BOOLEAN': sa.Boolean,
    'BOOL': sa.Boolean,
    'DATE': sa.Date,
    'TIMESTAMP': sa.DateTime,
    'DATETIME': sa.DateTime,
    'TIMESTAMPTZ': lambda: sa.DateTime(timezone=True),
}


def make_engine(creds):
    creds = validate_pg_creds(creds, context='Postgres output credentials')

    query = {}
    if creds.get('options'):
        query['options'] = creds['options']

    url = URL.create(
        drivername='postgresql+psycopg2',
        username=creds.get('user'),
        password=creds.get('password'),
        host=creds.get('host'),
        port=creds.get('port'),
        database=creds.get('dbname'),
        query=query or None,
    )
    return sa.create_engine(url, poolclass=NullPool)



def _validate_unique_columns(columns, *, table_name):
    # Must run before row dicts are built, not after -- once a row is
    # {col: value for col, value in zip(columns, row)}, a duplicate
    # column name has already silently collapsed to whichever value came
    # last, with no trace of the collision left to detect. columns would
    # still list the duplicate twice while rows lost a value entirely --
    # an internally inconsistent DbPayload that fails much later, inside
    # SQLAlchemy's CREATE TABLE with two same-named columns, rather than
    # clearly here. Distinct original labels that stringify to the same
    # value (1 and '1') hit this identically, since columns is already
    # the stringified list by the time this runs.
    duplicates = find_duplicates(columns)
    if duplicates:
        raise DbPublishError(f'{table_name!r}: duplicate output column names: {duplicates!r}')


def _apply_db_contract_columns(columns, rows, db_contract, *, table_name):
    if not db_contract:
        return list(columns), rows

    source_cols = list(db_contract)
    missing = [col for col in source_cols if col not in columns]
    if missing:
        raise DbPublishError(f'{table_name!r}: missing columns for db_contract: {missing!r}')

    rename = dict(db_contract)
    target_cols = [rename[col] for col in source_cols]

    # Two source columns mapping to the same target name would silently
    # collapse rows (Python dict literals with repeated keys keep only
    # the last one) while columns keeps a duplicate entry -- an
    # internally inconsistent DbPayload (columns says 2, rows has 1 key)
    # that fails cryptically much later, inside SQLAlchemy's CREATE
    # TABLE with two same-named columns, rather than clearly here. A
    # hand-written, static db_contract is reviewed once and stays fixed;
    # a dynamically-computed one (get_dynamic_db_contract) is built from
    # runtime data and can accidentally collide if its own logic has a
    # bug -- this isn't specific to that hook, but it makes hitting this
    # case easier, so it's worth closing at the shared boundary both
    # engines (and funnel_pandas's direct callers) go through.
    seen = {}
    duplicates = {}
    for source, target in zip(source_cols, target_cols):
        if target in seen:
            duplicates.setdefault(target, [seen[target]]).append(source)
        else:
            seen[target] = source
    if duplicates:
        raise DbPublishError(
            f'{table_name!r}: db_contract maps multiple source columns to the same '
            f'target name: {duplicates!r}'
        )

    return target_cols, [
        {rename[col]: row.get(col) for col in source_cols}
        for row in rows
    ]


def from_petl(tbl, *, table_name, schema, type_overrides=None, db_contract=None, db_table_id_pix=None):
    if isinstance(tbl, pd.DataFrame):
        raise DbPublishError(
            f'{table_name!r}: from_petl() received a pandas DataFrame, '
            'not a petl table -- use from_pandas() instead'
        )

    iterator = iter(tbl)

    try:
        header = next(iterator)
    except StopIteration:
        raise DbPublishError(f'PETL table for {table_name!r} is empty and has no header row')

    columns = [str(col) for col in header]
    if not columns:
        raise DbPublishError(f'{table_name!r}: no columns to publish -- the source table has no header')
    _validate_unique_columns(columns, table_name=table_name)

    rows = [
        {col: _normalize_value(value) for col, value in zip(columns, row, strict=True)}
        for row in iterator
    ]
    columns, rows = _apply_db_contract_columns(columns, rows, db_contract, table_name=table_name)

    return DbPayload(
        table_name=table_name,
        schema=schema,
        columns=columns,
        rows=rows,
        type_overrides=type_overrides,
        db_table_id_pix=db_table_id_pix,
    )



def from_pandas(df: pd.DataFrame, *, table_name, schema, type_overrides=None, db_contract=None, db_table_id_pix=None):
    if not isinstance(df, pd.DataFrame):
        raise DbPublishError(
            f'{table_name!r}: from_pandas() received a {type(df).__name__!r}, '
            'not a pandas DataFrame -- use from_petl() instead'
        )

    columns = [str(col) for col in df.columns]
    if not columns:
        raise DbPublishError(f'{table_name!r}: no columns to publish -- the source DataFrame has no columns')
    _validate_unique_columns(columns, table_name=table_name)

    prepared = df.copy()
    prepared = prepared.astype(object).where(pd.notna(prepared), None)

    rows = [
        {col: _normalize_value(value) for col, value in zip(columns, row, strict=True)}
        for row in prepared.itertuples(index=False, name=None)
    ]
    columns, rows = _apply_db_contract_columns(columns, rows, db_contract, table_name=table_name)

    return DbPayload(
        table_name=table_name,
        schema=schema,
        columns=columns,
        rows=rows,
        type_overrides=type_overrides,
        db_table_id_pix=db_table_id_pix,
    )


class DbPublisher:
    def __init__(
        self,
        *,
        creds,
        schema,
        logger=None,
        chunk_size=5000,
        type_infer_sample_size=5000,
    ):
        self.creds = validate_pg_creds(
            creds,
            context=f'Postgres output credentials for schema {schema!r}',
        )
        self.schema = schema
        self.log = logger or logging.getLogger(__name__)
        self.chunk_size = int(chunk_size)
        if self.chunk_size < 1:
            raise DbPublishError('chunk_size must be a positive integer')
        if type_infer_sample_size is None:
            self.type_infer_sample_size = None
        else:
            self.type_infer_sample_size = int(type_infer_sample_size)
            if self.type_infer_sample_size < 1:
                raise DbPublishError('type_infer_sample_size must be a positive integer or None')
        self._engine = None
        self._conn = None
        self._tx = None
        self._written_tables = []
        self._committed_tables = []
        self._table_rows = {}
        self._committed = False

    def ensure_connection(self):
        if self._engine is None:
            self._engine = make_engine(self.creds)

        if self._conn is None:
            self._conn = self._engine.connect()

        return self._conn

    def _ensure_transaction(self):
        conn = self.ensure_connection()

        if self._tx is None:
            self._tx = conn.begin()
            self.log.info('db publish transaction started')

        return conn

    def discard_pending_read(self):
        # source_state.build_source_state_store() reads/DDL run via
        # ensure_connection() only, without going through
        # _ensure_transaction(). On SQLAlchemy's autobegin behavior that
        # can still leave an implicit transaction open on the connection.
        # If publish() is called afterwards it calls conn.begin() expecting
        # no transaction in progress, so reset here.
        if self._conn is not None and self._tx is None:
            self._conn.rollback()

    def publish(self, payload: DbPayload):
        self._ensure_transaction()

        table = self._build_table(payload)
        self.log.info('publishing db table %s.%s rows=%s', payload.schema, payload.table_name, len(payload.rows))

        table.drop(self._conn, checkfirst=True)
        table.create(self._conn)

        if payload.rows:
            insert_stmt = table.insert()
            for chunk in _chunked(payload.rows, self.chunk_size):
                self._conn.execute(insert_stmt, chunk)

        full_name = f'{payload.schema}.{payload.table_name}'
        table_result = DbTableResult(
            schema=payload.schema,
            table_name=payload.table_name,
            full_name=full_name,
            rows=len(payload.rows),
            db_table_id_pix=payload.db_table_id_pix,
        )
        self._written_tables.append(table_result)
        self._table_rows[full_name] = table_result.rows

    def commit(self):
        if self._tx is not None:
            self._tx.commit()
            self._tx = None
        elif self._conn is not None and self._conn.in_transaction():
            # No explicit _tx -- but source-state writes (ensure_table/
            # upsert_state, via SourceStateStore) go straight through
            # ensure_connection(), never through _ensure_transaction(),
            # so a source-check-only run (no db_table pipeline at all)
            # never opens an explicit _tx. SQLAlchemy's own autobegin still
            # opens an implicit one the moment anything executes, and that
            # implicit transaction is what actually needs committing here.
            self._conn.commit()
        else:
            return []

        self._committed = True
        self._committed_tables = list(self._written_tables)
        table_names = [item.full_name for item in self._committed_tables]
        self.log.info('db publish transaction committed, tables=%s', ', '.join(table_names) or 'none')
        return list(self._committed_tables)

    def rollback(self):
        if self._tx is not None:
            self._tx.rollback()
            self._tx = None
        elif self._conn is not None and self._conn.in_transaction():
            self._conn.rollback()
        else:
            return

        self._committed = False
        self._committed_tables = []
        self._written_tables = []
        self._table_rows = {}
        self.log.info('db publish transaction rolled back')

    @property
    def committed(self):
        return self._committed

    @property
    def written_tables(self):
        return list(self._written_tables)

    @property
    def committed_tables(self):
        return list(self._committed_tables)

    @property
    def table_rows(self):
        return dict(self._table_rows)

    def close(self):
        conn, self._conn = self._conn, None
        engine, self._engine = self._engine, None

        steps = []
        if conn is not None:
            steps.append(('connection', conn.close))
        if engine is not None:
            steps.append(('engine', engine.dispose))

        attempt_all_cleanup(
            steps,
            close_fn=lambda item: item[1](),
            describe=lambda item: f'while closing DbPublisher {item[0]}',
        )

    def _build_table(self, payload: DbPayload):
        metadata = sa.MetaData()

        def _column_type(col_name):
            type_obj = _resolve_override((payload.type_overrides or {}).get(col_name))
            if type_obj is not None:
                return type_obj
            return _infer_column_type(payload.rows, col_name, sample_size=self.type_infer_sample_size)

        columns = [sa.Column(col_name, _column_type(col_name)) for col_name in payload.columns]

        return sa.Table(payload.table_name, metadata, *columns, schema=payload.schema)


def _chunked(rows: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i:i + chunk_size]



def _normalize_value(value):
    if value is None:
        return None

    try:
        if value != value:
            return None
    except Exception:
        pass

    to_pydatetime = getattr(value, 'to_pydatetime', None)
    if callable(to_pydatetime):
        value = to_pydatetime()
        if value is None:
            return None

    item = getattr(value, 'item', None)
    if callable(item) and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = item()
        except Exception:
            pass

    return value



def _resolve_override(value):
    match value:
        case None:
            return None
        case sa.types.TypeEngine():
            return value
        case type() as cls if issubclass(cls, sa.types.TypeEngine):
            return cls()
        case str():
            key = value.strip().upper()
            try:
                return _TYPE_OVERRIDES[key]()
            except KeyError as e:
                raise DbPublishError(f'unsupported db type override: {value!r}') from e
        case _:
            raise DbPublishError(f'unsupported db type override: {value!r}')



def _infer_column_type(
    rows: list[dict[str, Any]],
    col_name: str,
    *,
    sample_size: int | None = 5000,
):
    families = set()
    rows_to_scan = rows if sample_size is None else rows[:sample_size]

    for row in rows_to_scan:
        value = row.get(col_name)
        if value is None:
            continue
        families.add(_value_family(value))
        if len(families) > 2:
            return sa.Text()

    if not families:
        return sa.Text()

    if families == {'bool'}:
        return sa.Boolean()

    if families == {'int'}:
        return sa.BigInteger()

    if families <= {'int', 'numeric'} and families:
        return sa.Numeric()

    if families == {'date'}:
        return sa.Date()

    if families == {'datetime'}:
        return sa.DateTime()

    if families <= {'date', 'datetime'} and families:
        return sa.DateTime()

    if families == {'text'}:
        return sa.Text()

    if families == {'bytes'}:
        return sa.LargeBinary()

    return sa.Text()



def _value_family(value):
    match value:
        case bool():
            return 'bool'
        case datetime():
            return 'datetime'
        case date():
            return 'date'
        case int():
            return 'int'
        case float() | Decimal():
            return 'numeric'
        case bytes() | bytearray() | memoryview():
            return 'bytes'
        case _:
            return 'text'
