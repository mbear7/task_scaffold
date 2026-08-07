# -*- coding: utf-8 -*-
"""Level 2: Postgres input resource. No source_access/source_tracking
dependency -- reads via psycopg2 and task_core.db.publish (internal)
only."""

import re

import petl as etl

from task_core.db.publish import validate_pg_creds


_SAFE_TABLE_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$')


def _validate_table_identifier(table):
    if not isinstance(table, str) or not _SAFE_TABLE_IDENTIFIER_RE.match(table):
        raise ValueError(f'Unsafe table identifier: {table!r}. Use query= for custom SQL.')
    return table


class db_resource:
    def __init__(self, creds):
        self.creds = validate_pg_creds(creds, context='Postgres input credentials')
        self._conn = None
        # Caches the lazy petl query definition (a DbView), not fetched
        # rows -- confirmed directly: DbView.__iter__ re-issues the
        # underlying SQL query fresh on every single traversal, so this
        # cache alone does not protect against repeated execution, or
        # repeated rows if the same key is requested again. That
        # protection is run_pipelines()'s own job, via
        # adapter.stabilize() wrapping the pipeline's returned table in
        # etl.cache() before any traversal happens -- see the README's
        # "Repeated traversal of a pipeline's output" section for the
        # full reasoning and the confirmed-directly SQL re-execution risk
        # this is guarding against.
        self._table_cache = {}

    def _ensure_conn(self):
        if self._conn is None:
            import psycopg2
            self._conn = psycopg2.connect(**self.creds)
        return self._conn

    def _load_table(self, *, table=None, query=None, server_side_cursor=False, itersize=2000):
        if (table is None) == (query is None):
            raise ValueError('Specify exactly one of table or query')

        sql = query if query is not None else f'select * from {_validate_table_identifier(table)}'
        conn = self._ensure_conn()

        if server_side_cursor:
            # A callable, not a specific cursor instance -- found by a
            # further review: passing a single, already-created named
            # cursor directly meant the resulting DbView held ONE cursor
            # for its whole lifetime. A named cursor can only be
            # iterated once, its result set exhausted afterward, so any
            # repeated traversal of that same DbView -- including two
            # different callers landing on the same cache key -- either
            # got an empty result the second time, or would be sharing
            # the exact same cursor object.
            #
            # petl's own DbView.__iter__ (petl/io/db.py) already has a
            # distinct dispatch branch for a callable specifically,
            # confirmed directly, separate from its direct-cursor
            # branch: it calls the callable FRESH on every traversal to
            # get a brand new cursor each time (_iter_dbapi_mkcurs),
            # and explicitly closes that cursor afterward -- a
            # `finally: cursor.close()`, confirmed directly reading its
            # own source. This also means petl's own "using a DB-API
            # cursor with fromdb() is not recommended" warning never
            # fires for this branch at all -- confirmed directly that
            # warning is specific to the direct-cursor dispatch this
            # replaces, not this one.
            #
            # uuid4 inside the closure, not a fixed name: a named
            # cursor must be unique within its own transaction, and a
            # fresh cursor now gets created on every traversal, not
            # just once.
            import uuid

            def make_cursor():
                cursor = conn.cursor(name=f'task_core_{uuid.uuid4().hex}')
                cursor.itersize = itersize
                return cursor

            return etl.fromdb(make_cursor, sql)

        return etl.fromdb(conn, sql)

    def get_table(self, *, table=None, query=None, postprocess=None, server_side_cursor=False, itersize=2000):
        # itersize is part of the cache key now, but only when it's
        # genuinely relevant (server_side_cursor=True) -- found by a
        # further review. Previously excluded entirely, on the
        # reasoning that itersize only tunes how rows get fetched,
        # never which rows -- true for the plain connection path, where
        # every traversal gets its own, fresh cursor regardless of
        # this resource's own cache. Not true here: a caller explicitly
        # requesting a different itersize for a server-side cursor is a
        # deliberate, explicit performance request, and silently
        # overriding it because some other caller requested the same
        # query first is a genuine surprise, not a harmless cache hit.
        # itersize is folded to None in the key when server_side_cursor
        # is False, so two plain calls with different itersize values
        # still correctly share one cache entry, matching the original
        # reasoning for the case it actually applies to.
        itersize_key = itersize if server_side_cursor else None
        key = (
            ('table', table, server_side_cursor, itersize_key) if table is not None
            else ('query', query, server_side_cursor, itersize_key)
        )

        if key not in self._table_cache:
            self._table_cache[key] = self._load_table(
                table=table, query=query,
                server_side_cursor=server_side_cursor, itersize=itersize,
            )

        tbl = self._table_cache[key]
        return postprocess(tbl) if postprocess is not None else tbl

    def close(self):
        # Swap-then-close, matching excel_resource.close() and
        # DbPublisher.close(). The previous shape --
        #
        #     if self._conn is not None:
        #         self._conn.close()
        #         self._conn = None
        #
        # -- left this resource in a half-closed state whenever close()
        # itself raised: _conn stayed set, so a second close() attempt
        # retried the same failing connection, and _table_cache was never
        # cleared on any path at all, success included.
        #
        # That cache is not inert. petl's DbView holds the connection it was
        # built from directly, on its own .dbo attribute -- confirmed
        # directly, not inferred: after close(), the cached DbView's .dbo
        # was still the now-closed connection object. Keeping the cache
        # meant a later get_table() with the same key handed back a table
        # bound to a dead connection, failing at traversal time (far from
        # the actual cause) rather than rebuilding against a fresh one, and
        # kept the connection object itself reachable after the resource had
        # reported itself closed.
        #
        # The exception is deliberately still allowed to propagate:
        # task_context.close() routes every resource through
        # cleanup.attempt_all_cleanup(), which needs a genuine failure to
        # surface so run_pipelines() can decide whether to log or raise it.
        # Swallowing it here would hide a real leaked connection.
        conn, self._conn = self._conn, None

        try:
            if conn is not None:
                conn.close()
        finally:
            self._table_cache.clear()


def build_db_resource(*, creds):
    return db_resource(creds=creds)
