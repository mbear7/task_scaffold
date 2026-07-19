# -*- coding: utf-8 -*-
"""
Level 2: technical task_scaffold_meta persistence for source-change
tracking. Moved OUT of db_publish.py so db_publish.py stays fully
task-agnostic.
"""

import json
import logging
import re

import sqlalchemy as sa

from task_core.types import SourceCheckError


_SAFE_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _validate_identifier(name, *, kind):
    # Raises SourceCheckError (task_core.types), not db_publish.DbPublishError:
    # "unsafe schema/table identifier" is a source-tracking *configuration*
    # problem (schema/table come from SourceChangeCheckConfig, a task_core
    # concept), not a generic publish failure.
    if not isinstance(name, str) or not _SAFE_IDENTIFIER_RE.match(name):
        raise SourceCheckError(f'Unsafe {kind} identifier: {name!r}')
    return name


class SourceStateStore:
    """Reads/writes the technical `task_scaffold_meta`-style table that backs
    task-level source-change checking. Always used through the same
    DbPublisher connection/transaction as the actual table publish -- see
    build_source_state_store() / update_source_state() below.
    """

    def __init__(self, conn, *, schema, table, logger=None):
        self.conn = conn
        self.schema = _validate_identifier(schema, kind='schema')
        self.table = _validate_identifier(table, kind='table')
        self.log = logger or logging.getLogger(__name__)
        self._full_name = f'{self.schema}.{self.table}'

    def ensure_table(self):
        self.conn.execute(sa.text(f'''
            create table if not exists {self._full_name} (
                task_name text not null,
                source_key text not null,

                source_kind text not null,
                root_path text,
                include_mask text,
                recursive boolean not null default false,

                file_count integer not null default 0,
                total_size_bytes bigint not null default 0,
                max_modified_at_utc timestamptz,
                source_signature text not null,
                source_snapshot jsonb,

                processed_at_utc timestamptz not null default now(),

                primary key (task_name, source_key)
            )
        '''))

    def read_state(self, task_name):
        result = self.conn.execute(
            sa.text(f'''
                select source_key, source_signature
                from {self._full_name}
                where task_name = :task_name
            '''),
            {'task_name': task_name},
        )
        return {row['source_key']: row['source_signature'] for row in result.mappings()}

    def sources_unchanged(self, task_name, fingerprints):
        stored = self.read_state(task_name)
        current_keys = {fp.source_key for fp in fingerprints}

        # No missing or extra source_key vs stored state: adding/removing a
        # tracked source is itself a change, even if every remaining
        # signature still matches.
        if current_keys != set(stored):
            return False

        return all(stored[fp.source_key] == fp.source_signature for fp in fingerprints)

    def upsert_state(self, task_name, fingerprints, *, store_snapshot=True):
        current_keys = [fp.source_key for fp in fingerprints]

        # Keep only the latest successfully processed source state (per the
        # design principle: no history table) -- prune rows for source_keys
        # that are no longer tracked for this task.
        if current_keys:
            self.conn.execute(
                sa.text(f'''
                    delete from {self._full_name}
                    where task_name = :task_name
                    and source_key not in :keep_keys
                ''').bindparams(sa.bindparam('keep_keys', expanding=True)),
                {'task_name': task_name, 'keep_keys': current_keys},
            )
        else:
            self.conn.execute(
                sa.text(f'delete from {self._full_name} where task_name = :task_name'),
                {'task_name': task_name},
            )

        upsert_stmt = sa.text(f'''
            insert into {self._full_name} (
                task_name, source_key, source_kind, root_path, include_mask,
                recursive, file_count, total_size_bytes, max_modified_at_utc,
                source_signature, source_snapshot, processed_at_utc
            ) values (
                :task_name, :source_key, :source_kind, :root_path, :include_mask,
                :recursive, :file_count, :total_size_bytes, :max_modified_at_utc,
                :source_signature, cast(:source_snapshot as jsonb), now()
            )
            on conflict (task_name, source_key) do update set
                source_kind = excluded.source_kind,
                root_path = excluded.root_path,
                include_mask = excluded.include_mask,
                recursive = excluded.recursive,
                file_count = excluded.file_count,
                total_size_bytes = excluded.total_size_bytes,
                max_modified_at_utc = excluded.max_modified_at_utc,
                source_signature = excluded.source_signature,
                source_snapshot = excluded.source_snapshot,
                processed_at_utc = excluded.processed_at_utc
        ''')

        for fp in fingerprints:
            # Two gates, both must allow it: the task-level config flag
            # (global kill switch) AND the fingerprint's own store_snapshot
            # (per-source opt-in/out -- see TrackedDbQuerySource, which
            # defaults this to False since a DB query's result may not be
            # safe to persist in a technical scratch table).
            persist_snapshot = store_snapshot and fp.store_snapshot and fp.source_snapshot is not None
            snapshot_json = json.dumps(fp.source_snapshot, ensure_ascii=False) if persist_snapshot else None
            self.conn.execute(
                upsert_stmt,
                {
                    'task_name': task_name,
                    'source_key': fp.source_key,
                    'source_kind': fp.source_kind,
                    'root_path': fp.root_path,
                    'include_mask': fp.include_mask,
                    'recursive': fp.recursive,
                    'file_count': fp.file_count,
                    'total_size_bytes': fp.total_size_bytes,
                    'max_modified_at_utc': fp.max_modified_at_utc,
                    'source_signature': fp.source_signature,
                    'source_snapshot': snapshot_json,
                },
            )



def build_source_state_store(publisher, *, schema, table):
    return SourceStateStore(
        publisher.ensure_connection(),
        schema=schema,
        table=table,
        logger=publisher.log,
    )


def update_source_state(publisher, *, task_name, fingerprints, config):
    store = build_source_state_store(publisher, schema=config.schema, table=config.table)

    if config.create_if_missing:
        store.ensure_table()

    store.upsert_state(task_name, fingerprints, store_snapshot=config.store_snapshot)
