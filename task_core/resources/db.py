# -*- coding: utf-8 -*-
"""Level 2: Postgres input resource. No file_access/source_tracking
dependency -- reads via psycopg2 and task_core.db_publish (internal)
only."""

import re

import petl as etl

from task_core.db_publish import validate_pg_creds


_SAFE_TABLE_IDENTIFIER_RE = re.compile(
    r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$'
)


def _validate_table_identifier(table):
    if not isinstance(table, str) or not _SAFE_TABLE_IDENTIFIER_RE.match(table):
        raise ValueError(
            f'Unsafe table identifier: {table!r}. Use query= for custom SQL.'
        )
    return table


class db_resource:
    def __init__(self, creds):
        self.creds = validate_pg_creds(creds, context='Postgres input credentials')
        self._conn = None
        self._table_cache = {}

    def _ensure_conn(self):
        if self._conn is None:
            import psycopg2
            self._conn = psycopg2.connect(**self.creds)
        return self._conn

    def _load_table(self, *, table=None, query=None):
        if (table is None) == (query is None):
            raise ValueError('Specify exactly one of table or query')

        sql = query if query is not None else f'select * from {_validate_table_identifier(table)}'
        conn = self._ensure_conn()
        return etl.fromdb(conn, sql)

    def get_table(self, *, table=None, query=None, postprocess=None):
        key = ('table', table) if table is not None else ('query', query)

        if key not in self._table_cache:
            self._table_cache[key] = self._load_table(table=table, query=query)

        tbl = self._table_cache[key]
        return postprocess(tbl) if postprocess is not None else tbl

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def build_db_resource(*, creds):
    return db_resource(creds=creds)
