# -*- coding: utf-8 -*-

"""Staging-table INSERT loader for db publication.

The default loader that fills a staging table during publish(), and the
compatibility baseline against which COPY is measured. Since 0.6.6 the
COPY loader in copy.py is the other member of DB_LOADERS. The path was
factored out of publish.py so the loader has a name and a single call
site, without pulling any of the publisher's lifecycle (transactions,
locks, comments) with it.

The transaction is owned by the publisher: this function neither opens
nor commits one, and neither creates nor drops the staging table.
Chunking is here because SQLAlchemy's executemany rewrite would
otherwise submit one huge statement per publish() call, and because
counting per-chunk gives us the loaded row count the publisher's
integrity check requires -- SQLAlchemy reports
supports_sane_multi_rowcount=False for psycopg2, so a driver rowcount
would be measuring the rewritten statement rather than the logical
rows.

See docs/decisions/0011.
"""

from __future__ import annotations

from typing import Any, Iterator


def _chunked(rows: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i:i + chunk_size]


def load_rows_into_staging(conn, staging_table, rows, chunk_size) -> int:
    """Insert `rows` into `staging_table` in chunks of `chunk_size`.

    Returns the number of rows loaded, counted in this loop rather than
    read from the driver -- see the module docstring for why. The caller
    is responsible for creating the table, opening a transaction, and
    committing.
    """
    loaded = 0
    if rows:
        insert_stmt = staging_table.insert()
        for chunk in _chunked(rows, chunk_size):
            conn.execute(insert_stmt, chunk)
            loaded += len(chunk)
    return loaded
