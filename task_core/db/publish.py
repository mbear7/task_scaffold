from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.pool import NullPool

from task_core.cleanup import attempt_all_cleanup
from task_core.db.copy import load_copy_into_staging, prepare_copy_source
from task_core.db.identifiers import (
    _RUN_TOKEN_HEX,
    MAX_IDENTIFIER_BYTES,
    STAGING_NAME_KIND,
    _quote_identifier,
    _quoted_name,
    advisory_lock_key,
    build_published_comment,
    build_staging_comment,
    new_run_token,
    owned_staging_tokens,
    parse_staging_comment,
    server_identifier_limit,
    staging_table_name,
    staging_target_token,
    validate_identifier,
    validate_portable_identifier,
    validate_published_column_name,
)
from task_core.db.insert import load_rows_into_staging
from task_core.db.payload import (
    DbPayload,
    DbTableResult,
)
from task_core.db.policies import (
    DEFAULT_IDENTIFIER_POLICY,
    CopyLoadPolicy,
    IdentifierPolicy,
    PublicationLockPolicy,
    PublicationPlan,
)
from task_core.db.spool_format import resolve_spool_directory
from task_core.db.spool_io import (
    SpoolIdentity,
    cleanup_predecessor_spools,
    cleanup_spool_paths,
)
from task_core.db.values import (
    DbPublishError,
    DbPublishInvariantError,
    ResolvedColumn,
    ResolvedSchema,
    _resolve_declared_type,
    _resolve_payload_schema,
)
from task_core.types import (
    DB_LOADERS,
    validate_db_loader,
    validate_payload_source_state,
    validate_publication_strategy,
)

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




# Dispatch table for the staging loader. One entry per implemented value
# of DB_LOADERS; the drift check below turns adding a value to DB_LOADERS
# without registering a loader into an import-time RuntimeError rather
# than a KeyError at first publish. Same self-guarding pattern as
# table_adapters._ADAPTERS, and same reason for `if ... raise` over
# `assert`: python -O strips asserts and the guarantee would silently
# disappear in the mode most likely to run in production.
LOADERS = {
    'insert': load_rows_into_staging,
    'copy': load_copy_into_staging,
}

if set(LOADERS) != set(DB_LOADERS):
    raise RuntimeError(
        f'db loader registry has drifted from DB_LOADERS: '
        f'registry={sorted(LOADERS)}, declared={sorted(DB_LOADERS)}'
    )




_FIND_RELATION_SQL = sa.text(
    "select c.oid, c.relkind "
    "from pg_catalog.pg_class c "
    "join pg_catalog.pg_namespace n on n.oid = c.relnamespace "
    "where n.nspname = :schema and c.relname = :table"
)


def _find_relation(conn, schema, table):
    """Return the exact PostgreSQL relation ``(oid, relkind)``, or ``None``.

    Schema and relation names remain separate bound values. Nothing is
    assembled into parser input, so search-path lookup and identifier case
    folding cannot change which catalog object is resolved. Callers own the
    missing-relation and relation-kind policy.
    """
    row = conn.execute(
        _FIND_RELATION_SQL,
        {'schema': schema, 'table': table},
    ).one_or_none()
    if row is None:
        return None
    return int(row[0]), str(row[1])


_RELATION_COLUMNS_SQL = sa.text(
    "select a.attname, a.atttypid, a.atttypmod, a.attnotnull, "
    "a.attcollation, a.attidentity, a.attgenerated, a.atthasdef "
    "from pg_catalog.pg_attribute a "
    "where a.attrelid = :oid and a.attnum > 0 and not a.attisdropped "
    "order by a.attnum"
)

_EXTERNAL_INCOMING_FKS_SQL = sa.text(
    "select n.nspname, c.relname, con.conname "
    "from pg_catalog.pg_constraint con "
    "join pg_catalog.pg_class c on c.oid = con.conrelid "
    "join pg_catalog.pg_namespace n on n.oid = c.relnamespace "
    "where con.contype = 'f' and con.confrelid = :oid "
    "and con.conrelid <> con.confrelid "
    "order by n.nspname, c.relname, con.conname"
)


def _relation_columns(conn, oid):
    """Canonical PostgreSQL physical column metadata for one relation."""
    return tuple(
        (
            str(row[0]), int(row[1]), int(row[2]), bool(row[3]),
            int(row[4]), str(row[5]), str(row[6]), bool(row[7]),
        )
        for row in conn.execute(_RELATION_COLUMNS_SQL, {'oid': oid}).all()
    )


def _external_incoming_foreign_keys(conn, oid):
    return tuple(
        (str(row[0]), str(row[1]), str(row[2]))
        for row in conn.execute(_EXTERNAL_INCOMING_FKS_SQL, {'oid': oid}).all()
    )




@dataclass(frozen=True, kw_only=True)
class PublisherConfig:
    """Everything about how publication behaves, in one frozen object.

    Introduced by executing the plan recorded in decisions/0005: the
    publisher seam had reached six constructor parameters, and
    IdentifierPolicy had itself been created to stop two independently
    defaulted integers drifting apart. A seventh loose argument would have
    repeated the mistake at a larger scale, so the whole set became one
    object instead.

    Per-task facts -- creds, pg_schema -- deliberately stay direct
    run_pipelines() arguments. A task widening a lock timeout should not
    have to restate its credentials to do it. Runner concerns such as
    source_change_check stay outside for the same reason.
    """

    publisher_factory: Any = None
    identifier_policy: IdentifierPolicy = field(default_factory=IdentifierPolicy)
    publication_lock_policy: PublicationLockPolicy = field(
        default_factory=PublicationLockPolicy
    )
    # COPY-enabled tasks inherit this policy unless they override the
    # per-task encryption flag.
    copy_load_policy: CopyLoadPolicy = field(default_factory=CopyLoadPolicy)

    def __post_init__(self):
        # Enforced, not assumed. None on either policy restored exactly the
        # independent downstream defaulting this object exists to
        # eliminate -- the publisher would have substituted its own,
        # silently diverging from what preflight validated against.
        if not isinstance(self.identifier_policy, IdentifierPolicy):
            raise DbPublishError(
                f'identifier_policy must be an IdentifierPolicy, '
                f'got {self.identifier_policy!r}'
            )
        if not isinstance(self.publication_lock_policy, PublicationLockPolicy):
            raise DbPublishError(
                f'publication_lock_policy must be a PublicationLockPolicy, '
                f'got {self.publication_lock_policy!r}'
            )
        if not isinstance(self.copy_load_policy, CopyLoadPolicy):
            raise DbPublishError(
                f'copy_load_policy must be a CopyLoadPolicy, '
                f'got {self.copy_load_policy!r}'
            )
        if self.publisher_factory is not None and not callable(self.publisher_factory):
            raise DbPublishError(
                f'publisher_factory must be callable or None, got {self.publisher_factory!r}'
            )

    def resolved_factory(self):
        # Defaults to DbPublisher, which is defined below this dataclass.
        return self.publisher_factory if self.publisher_factory is not None else DbPublisher




class DbPublisher:
    def __init__(
        self,
        *,
        creds,
        schema,
        logger=None,
        chunk_size=5000,
        type_infer_sample_size=5000,
        identifier_policy=None,
        publication_lock_policy=None,
        publication_plan=None,
        task_name,
        copy_load_policy=None,
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
        self.identifier_policy = identifier_policy or DEFAULT_IDENTIFIER_POLICY
        self.publication_lock_policy = publication_lock_policy or PublicationLockPolicy()
        self.copy_load_policy = copy_load_policy or CopyLoadPolicy()
        if not isinstance(self.copy_load_policy, CopyLoadPolicy):
            raise DbPublishError(
                f'copy_load_policy must be a CopyLoadPolicy, '
                f'got {self.copy_load_policy!r}'
            )
        self.max_identifier_bytes = self.identifier_policy.max_identifier_bytes
        # Work queued by the runner and executed inside the publication
        # transaction, so the source-state write is atomic with the swaps
        # without commit() growing a parameter or the protocol growing a
        # member. Mutable and populated after construction because the
        # fingerprints do not exist until collection has run.
        self.publication_plan = publication_plan if publication_plan is not None else PublicationPlan()
        # Validated here, not discovered at publication. An empty task
        # name derives a lock key and stages successfully, then writes an
        # ownership comment that parse_staging_comment() REJECTS -- so the
        # run reports its own staging artifact as unowned and fails at
        # commit(), after every pipeline has run. It need not be a portable
        # identifier; it only has to be a non-empty stable string.
        # REQUIRED, and required to be usable. None was permitted on the
        # theory that such a publisher 'does not participate in locking or
        # ownership' -- which was simply false: begin_run() derived an
        # advisory key from '' and staging wrote "task": "" into ownership
        # metadata that parse_staging_comment() then REJECTS, so the run
        # prepared successfully and declared its own artifact unowned at
        # publication. The staged PostgreSQL lifecycle cannot operate
        # without a task identity; pretending otherwise only moved the
        # failure later.
        #
        # It need not be a portable identifier -- it never becomes a SQL
        # identifier -- only a non-empty stable string.
        if not isinstance(task_name, str) or not task_name.strip():
            raise DbPublishError(
                f'task_name must be a non-empty string, got {task_name!r}. It '
                f'identifies this run\'s staging artifacts in their ownership '
                f'metadata and derives its advisory lock key; an unusable one '
                f'would otherwise fail only at publication.'
            )
        self.task_name = task_name
        self._engine = None
        self._conn = None
        self._tx = None
        self._connection_lost = False
        self._locked_task_name = None
        self._written_tables = []
        self._pending_swaps = []
        self._resolved_schemas = {}
        self._refill_targets = set()
        self._run_token = new_run_token()
        # Final invariant enforcement. The naming rule already makes a
        # collision impossible by construction and preflight already proved
        # it statically; this is what turns 'impossible' into 'enforced'.
        self._generated_names = set()
        self._server_identifier_bytes = None
        self._committed_tables = []
        self._table_rows = {}
        self._committed = False

    def ensure_connection(self):
        # Three states, not two: never connected, connected, and LOST.
        # Reconnecting after a lost session would silently continue without
        # the advisory lock this run holds on it -- and the lock is what
        # guarantees no other run of this task is live, which in turn is
        # what makes predecessor cleanup safe. A reconnect looks harmless,
        # which is exactly why it has to be refused explicitly.
        if self._connection_lost:
            raise DbPublishError(
                'the publisher connection was lost. Reconnecting would silently '
                'continue without the task advisory lock held on that session, '
                'so this run cannot proceed.'
            )

        # Detected, not merely representable. mark_connection_lost() existed
        # but nothing called it, so the terminal state was unreachable in
        # practice -- and SQLAlchemy will transparently reconnect an
        # invalidated Connection on the next statement, which silently
        # continues on a NEW session that holds none of this run's advisory
        # locks. That is exactly the stale-publisher scenario decisions/0006
        # exists to eliminate, so the check has to run before every reuse.
        if self._conn is not None and self._conn.invalidated:
            self.mark_connection_lost()
            raise DbPublishError(
                'the publisher connection was invalidated. SQLAlchemy would '
                'reconnect transparently on the next statement, continuing on a '
                'session that does not hold this task\'s advisory lock, so this '
                'run cannot proceed.'
            )

        if self._engine is None:
            self._engine = make_engine(self.creds)

        if self._conn is None:
            self._conn = self._engine.connect()

        return self._conn

    def mark_connection_lost(self):
        self._connection_lost = True
        self._locked_task_name = None

    def _note_connection_loss(self):
        """Transition to the terminal state if the session is already gone.

        Called at the top of every NON-RAISING cleanup path. ensure_connection()
        rejects an invalidated connection, but rollback() and
        release_task_lock() use self._conn directly and so bypassed it --
        confirmed directly, rollback() executed DROP TABLE and COMMIT on a
        connection marked invalidated while _connection_lost stayed False.
        SQLAlchemy would reconnect for those statements, running cleanup on
        a session that holds none of this run's advisory locks. Cleanup must
        never reconnect after losing the lock-owning session; the staging
        artifacts are the next run's problem, and it will find them under
        its own lock.
        """
        if self._connection_lost:
            return True
        if self._conn is not None and self._conn.invalidated:
            self.mark_connection_lost()
            self.log.warning(
                'the publisher session was lost; skipping cleanup rather than '
                'reconnecting without the task advisory lock. Staging artifacts '
                'will be removed by the next run of this task.'
            )
            return True
        return False

    # -- task advisory lock -------------------------------------------

    def try_acquire_task_lock(self):
        """Session-level pg_try_advisory_lock. True if this run may proceed.

        try, not the blocking form: a five-minute schedule on a
        twenty-minute task would otherwise build an unbounded queue, which
        is worse than the collision it is meant to prevent. Failing fast is
        the wanted behaviour -- concurrent runs of one scheduled task are a
        misfire, not a feature.

        Session-level, not transaction-level: the staged model splits a run
        into many committed transactions, and pg_advisory_xact_lock would
        release at the first of them. Session scope also gives the right
        failure mode for free -- a killed process drops the connection and
        PostgreSQL releases the lock, with no lock table needing cleanup of
        its own.
        """
        # PostgreSQL session advisory locks are COUNTED: the same session
        # may acquire the same lock repeatedly and must release it as many
        # times. A second acquisition here would leave the server holding
        # one lock after release_task_lock(), while this object reported
        # itself unlocked -- state that disagrees with the database in the
        # direction that matters.
        #
        # Loud rather than silently idempotent: a second begin_run() also
        # repeats predecessor cleanup, so it signals incorrect lifecycle
        # use rather than a harmless retry. The runner calls it once.
        if self._locked_task_name is not None:
            raise DbPublishInvariantError(
                f'internal invariant violated -- the task advisory lock for '
                f'{self._locked_task_name!r} is already held by this publisher. '
                f'PostgreSQL counts session locks, so acquiring twice would '
                f'leave one held after release.'
            )

        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            # No advisory locks outside PostgreSQL. Treated as acquired so
            # non-PostgreSQL test backends exercise the rest of the flow;
            # production is PostgreSQL by construction.
            self._locked_task_name = self.task_name
            return True

        namespace, key = advisory_lock_key(self.task_name)
        acquired = bool(
            conn.execute(
                sa.text('select pg_try_advisory_lock(:ns, :key)'),
                {'ns': namespace, 'key': key},
            ).scalar()
        )
        # The identity, not a flag. A boolean only proved that SOME lock
        # was held, so a publisher holding task_a's lock would happily
        # clean task_b's artifacts -- the exact cross-run risk the guard
        # exists to remove, reached through a direct caller instead.
        self._locked_task_name = self.task_name if acquired else None
        return acquired

    def release_task_lock(self):
        """Explicit unlock before closing, rather than relying on session
        end. Today make_engine() uses NullPool so the two are equivalent --
        but the whole dedicated-connection contract rests on NullPool, and
        an explicit unlock keeps this honest if that ever changes.
        """
        if not self.lock_held or self._conn is None or self._note_connection_loss():
            self._locked_task_name = None
            return

        if self._conn.dialect.name != 'postgresql':
            self._locked_task_name = None
            return

        # Unlocks the task actually locked, not one supplied by the caller.
        # release_task_lock('task_b') used to clear the flag while task_a
        # stayed locked for the rest of the session.
        namespace, key = advisory_lock_key(self._locked_task_name)
        try:
            self._conn.execute(
                sa.text('select pg_advisory_unlock(:ns, :key)'),
                {'ns': namespace, 'key': key},
            )
            self._conn.commit()
        finally:
            self._locked_task_name = None

    @property
    def lock_held(self):
        return self._locked_task_name is not None

    @property
    def locked_task_name(self):
        return self._locked_task_name

    def _ensure_transaction(self):
        conn = self.ensure_connection()

        if self._tx is None:
            # Close out any transaction SQLAlchemy autobegan on our behalf
            # before opening an explicit one -- conn.begin() rejects being
            # called on an already-transacted connection. The only thing
            # that autobegins here is the source-state read phase, which
            # under the staged model is a bounded phase that SHOULD commit
            # rather than be discarded, so committing is correct and not
            # merely convenient.
            #
            # This absorbs what used to be an explicit runner-invoked reset
            # of that pending read. Doing it here removes both a lifecycle
            # state the new architecture makes impossible and a protocol
            # member with it.
            if conn.in_transaction():
                conn.commit()
            self._tx = conn.begin()
            self.log.info('transaction started')

        return conn

    def begin_run(self):
        """Claim this task for this run. False means another run holds it.

        The ONE member the staged model adds to the publisher protocol.
        Lock acquisition and predecessor cleanup are folded together
        deliberately: they are a single precondition -- "this run owns the
        task, and nothing a dead predecessor left is still lying around" --
        and splitting them would put two more members on a seam that has
        already been expanded by accident twice.

        Cleanup is safe here precisely because the lock was just acquired:
        no other run of this task is live, so any staging artifact
        positively identified as this task's belongs to a predecessor that
        is gone. No age threshold, no race with a running peer.
        """
        # The server's real identifier limit BEFORE any DDL, including
        # cleanup's. Cleanup works from catalog-returned names so it is
        # unlikely to truncate anything, but 'verified before the first
        # DDL' is the stated contract and this is where it is cheapest to
        # honour.
        self._effective_identifier_limit()

        if not self.try_acquire_task_lock():
            return False

        self._cleanup_predecessor_spools()
        self.cleanup_predecessor_artifacts()
        return True

    def _cleanup_predecessor_spools(self):
        """Delete positively owned predecessor spools under the task lock.

        A process crash can prevent the current-run cleanup path from running.
        The next execution cleans the residue only after acquiring the same
        advisory lock, which proves that no live run of this task can own it.
        Unknown, malformed and foreign files remain untouched.
        """
        self._require_task_lock('clean up predecessor COPY spools')
        directory = resolve_spool_directory(self.copy_load_policy)
        deleted, _preserved = cleanup_predecessor_spools(
            directory,
            task=self.task_name,
        )
        if deleted:
            self.log.warning(
                'deleted %s COPY spool(s) left by a previous run of %s: %s',
                len(deleted),
                self.task_name,
                ', '.join(str(path) for path in deleted),
            )
        return deleted

    def _require_task_lock(self, action):
        """The unconditional-lock contract, enforced by the publisher
        rather than trusted to the caller.

        The runner always calls begin_run() first, but publisher_factory is
        an advertised extension seam and DbPublisher is usable directly --
        so a caller could previously publish without ever claiming the
        task, which makes predecessor cleanup unsafe for everyone else.
        """
        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            return
        if self._locked_task_name is None:
            raise DbPublishError(
                f'cannot {action}: this publisher does not hold the task '
                f'advisory lock. Call begin_run() first -- without it, another '
                f'run of the same task may be preparing or publishing '
                f'concurrently, and predecessor cleanup is not safe.'
            )
        # Identity, not presence. Holding SOME lock is not authority over
        # THIS task's artifacts.
        if self._locked_task_name != self.task_name:
            raise DbPublishInvariantError(
                f'internal invariant violated -- cannot {action}: the held '
                f'advisory lock is for task {self._locked_task_name!r}, but this '
                f'publisher is configured for {self.task_name!r}'
            )

    @classmethod
    def preflight(cls, specs, *, schema, source_state_target=None,
                  identifier_policy=None, max_identifier_bytes=None):
        """Backend-specific validation of DECLARED targets, invoked by the
        engine-neutral runner before build_context().

        This is the seam that keeps validate_pipeline_classes() free of
        PostgreSQL: the runner knows there is a backend to ask and knows
        when to ask it, but never learns what 63 means. A classmethod, not
        an instance: no connection, no cleanup obligation, and nothing that
        could leak if build_context() raises afterwards.

        Runs regardless of output_db -- an unpublishable table name is a
        defect in the task definition, not a property of one invocation,
        and gating it on this run's flags would let it hide until the first
        run that happened to enable DB output. But it performs no backend
        I/O at all, so a disabled DB path stays genuinely free.

        Skipped entirely when no spec declares db_table: a DB-free task
        should not have backend policy applied to it.
        """
        # One policy object shared with the publisher that will do the
        # work, rather than a second independently-defaulted integer. The
        # explicit max_identifier_bytes argument stays for direct callers
        # and tests; the policy wins when both are given.
        if identifier_policy is not None:
            max_identifier_bytes = identifier_policy.max_identifier_bytes
        elif max_identifier_bytes is None:
            max_identifier_bytes = MAX_IDENTIFIER_BYTES

        declaring = {name: spec for name, spec in specs.items() if spec.db_table}
        if not declaring and not source_state_target:
            return

        # The package has one identifier contract: every schema, table and
        # published column name is a portable lower-case identifier.
        if schema and declaring:
            validate_identifier(schema, max_identifier_bytes, kind='schema')
            validate_portable_identifier(schema, kind='schema')

        # The source-state table is a real PostgreSQL table that a
        # source-check-enabled run creates and writes, and it was missing
        # from this model entirely. Two consequences, both confirmed
        # directly.
        #
        # Its identifiers were length-unchecked: SourceStateStore's own
        # _validate_identifier() applies the portable regex and nothing
        # else, so a 64-byte lower-case source-state table name was
        # accepted and would have been silently truncated by PostgreSQL --
        # exactly the failure this whole mechanism exists to prevent,
        # still reachable through the technical table.
        #
        # And nothing stopped a pipeline from declaring it as a business
        # target. Reproduced end to end: a pipeline with
        # db_table='task_scaffold_meta' staged its output, the run updated
        # fingerprints in the real table, then the swap dropped that table
        # and renamed the pipeline's staging table over it. The source
        # state was destroyed, replaced by pipeline output, and the run
        # reported success. The staging design is what makes this succeed
        # silently -- under the previous direct publication the later
        # upsert would probably have failed on missing columns.
        reserved = None
        if source_state_target:
            state_schema, state_table = source_state_target
            for value, kind in ((state_schema, 'source-state schema'),
                                (state_table, 'source-state table name')):
                if value:
                    validate_identifier(value, max_identifier_bytes, kind=kind)
                    validate_portable_identifier(value, kind=kind)
            reserved = (state_schema, state_table)

        seen_staging = {}
        for pipeline_name, spec in declaring.items():
            context = f'{pipeline_name}: '
            validate_identifier(spec.db_table, max_identifier_bytes, kind='table name', context=context)
            validate_portable_identifier(spec.db_table, kind='table name', context=context)

            if reserved is not None and (schema, spec.db_table) == reserved:
                raise DbPublishError(
                    f'{context}db_table {spec.db_table!r} in schema {schema!r} is the '
                    f'source-state table, which this run also reads and writes. '
                    f'Publishing over it destroys the stored fingerprints and the '
                    f'run still reports success. Choose a different db_table, or a '
                    f'different SourceChangeCheckConfig table.'
                )

            # The derived physical name, checked here because its suffix is
            # fixed width and its target token excludes the run -- so this
            # is exactly the name the real run will generate, not an
            # approximation of it.
            staging = staging_table_name(
                schema, spec.db_table, 'x' * _RUN_TOKEN_HEX, max_bytes=max_identifier_bytes,
            )
            validate_identifier(staging, max_identifier_bytes, kind='generated staging name', context=context)

            key = (schema, staging)
            if key in seen_staging:
                raise DbPublishError(
                    f'{context}generated staging name collides with pipeline '
                    f'{seen_staging[key]!r}: {staging!r} (targets '
                    f'{spec.db_table!r} and {declaring[seen_staging[key]].db_table!r})'
                )
            seen_staging[key] = pipeline_name

            for column in cls._declared_column_targets(spec):
                validate_identifier(column, max_identifier_bytes, kind='column name', context=context)
                validate_published_column_name(column, context=context)

            if spec.output_schema is not None:
                for column in spec.output_schema:
                    try:
                        _resolve_declared_type(column.type)
                    except DbPublishError as exc:
                        raise DbPublishError(
                            f'{context}output_schema column {column.name!r}: {exc}'
                        ) from exc

    @staticmethod
    def _declared_column_targets(spec):
        """Column names knowable without running anything. Covers a static
        db_contract's TARGET names (its values, not its keys -- the keys are
        raw spreadsheet headers, 77 of 79 of which are Cyrillic in this
        project and are renamed away precisely so they never reach DDL),
        db_output, and db_updated_at's column when it is a literal name.

        Dynamic contracts via get_dynamic_db_contract() cannot appear here
        by definition; those are caught at payload construction instead.
        """
        targets = []
        if spec.db_contract:
            targets += [str(value) for value in spec.db_contract.values()]
        if spec.db_output:
            targets += [str(value) for value in spec.db_output]
        if spec.db_not_null_columns:
            targets += [str(value) for value in spec.db_not_null_columns]
        if spec.output_schema:
            targets += [column.name for column in spec.output_schema]
        if isinstance(spec.db_updated_at, str):
            targets.append(spec.db_updated_at)
        elif spec.db_updated_at:
            targets.append('etl_updated_at')
        return targets

    def _effective_identifier_limit(self):
        if self._server_identifier_bytes is None:
            self._server_identifier_bytes = server_identifier_limit(
                self.ensure_connection(), self.max_identifier_bytes,
            )
        return min(self.max_identifier_bytes, self._server_identifier_bytes)


    def _validate_payload_identifiers(self, payload, limit):
        """Validate actual output names after contracts and projections apply.

        This is the only point where every runtime column name exists. All
        database identifiers follow the same portable lower-case contract;
        direct ``DbPayload`` callers receive the same validation as the normal
        ``PipelineSpec`` path.
        """
        context = f'{payload.table_name!r}: '

        validate_identifier(payload.table_name, limit, kind='table name')
        validate_portable_identifier(payload.table_name, kind='table name', context=context)

        if payload.schema:
            validate_identifier(payload.schema, limit, kind='schema')
            validate_portable_identifier(payload.schema, kind='schema', context=context)

        for column in payload.columns:
            validate_identifier(column, limit, kind='column name', context=context)
            validate_published_column_name(column, context=context)

    def _prepare_copy_payload(self, payload: DbPayload):
        """Consume one COPY row source into the final encrypted spool.

        Runs before the preparation transaction opens. The returned schema is
        authoritative for staging DDL, and the exact row count replaces every
        former ``len(payload.rows)`` assumption on the COPY path.
        """
        if payload.row_source is None:
            raise DbPublishInvariantError(
                "internal invariant violated -- COPY payload has no row_source"
            )

        policy = self.copy_load_policy
        if payload.copy_spool_encryption is not None:
            policy = replace(
                policy, encrypt_spools=payload.copy_spool_encryption,
            )
            if not policy.encrypt_spools:
                self.log.warning(
                    'COPY spool encryption disabled by DbPayload for %s.%s',
                    payload.schema, payload.table_name,
                )

        resolved_framework_columns = tuple(
            ResolvedColumn(
                column.name,
                _resolve_declared_type(column.type),
                column.nullable,
            )
            for column in payload.framework_columns
        )
        if payload.output_schema is not None:
            declared_columns = tuple(
                ResolvedColumn(
                    column.name,
                    _resolve_declared_type(column.type),
                    column.nullable,
                )
                for column in payload.output_schema
            ) + resolved_framework_columns
        else:
            declared_columns = None

        identity = SpoolIdentity(
            task=self.task_name,
            target_schema=payload.schema or '<default-schema>',
            target_table=payload.table_name,
            run_start_utc=datetime.now(timezone.utc),
            pid=os.getpid(),
        )
        return prepare_copy_source(
            row_source=payload.row_source.iter_rows(),
            columns=payload.columns,
            declared_schema=declared_columns,
            identity=identity,
            directory=resolve_spool_directory(policy),
            policy=policy,
            framework_columns=resolved_framework_columns,
            type_overrides=payload.type_overrides,
            not_null_columns=payload.not_null_columns,
        )

    def publish(self, payload: DbPayload):
        """Prepare one output for publication, in its own committed
        transaction. The live table is not touched.

        INSERT resolves from materialized mappings. COPY consumes its one-shot
        source into a bounded encrypted spool before the transaction opens,
        then streams that spool through the existing psycopg2 connection into
        the same ordinary logged staging table. From staging verification
        onward both loaders use one publication protocol.
        """
        self._require_task_lock('publish')

        validate_publication_strategy(
            payload.publication_strategy,
            output_schema=payload.output_schema,
            field_name='publication_strategy',
            error_type=DbPublishError,
        )
        validate_db_loader(
            payload.db_loader,
            field_name='db_loader',
            error_type=DbPublishError,
        )
        validate_payload_source_state(
            payload.db_loader, payload.rows, payload.row_source,
            error_type=DbPublishError,
        )
        if payload.copy_spool_encryption is not None and type(
            payload.copy_spool_encryption
        ) is not bool:
            raise DbPublishError('copy_spool_encryption must be bool or None')

        if payload.schema != self.schema:
            raise DbPublishError(
                f'payload targets schema {payload.schema!r} but this publisher is '
                f'configured for {self.schema!r}. Staging artifacts are cleaned up '
                f'per-schema, so a target outside the publisher\'s schema would '
                f'leave orphans no later run scans.'
            )

        limit = self._effective_identifier_limit()
        self._validate_payload_identifiers(payload, limit)

        staging_name = staging_table_name(
            payload.schema, payload.table_name, self._run_token, max_bytes=limit,
        )
        validate_identifier(
            staging_name, limit, kind='generated staging name',
            context=f'{payload.table_name!r}: ', invariant=True,
        )
        key = (payload.schema, staging_name)
        if key in self._generated_names:
            raise DbPublishInvariantError(
                f'internal invariant violated -- generated staging-table collision: '
                f'{payload.schema}.{staging_name} (target {payload.table_name!r})'
            )

        prepared_copy = None
        try:
            if payload.db_loader == 'copy':
                prepared_copy = self._prepare_copy_payload(payload)
                # Declared preparation may reorder columns. The staging table,
                # verification and later INSERT FROM staging must all use the
                # authoritative resolved order, not the source projection order.
                payload.columns = [column.name for column in prepared_copy.columns]
                resolved_schema = ResolvedSchema(
                    columns=prepared_copy.columns,
                    source=(
                        'declared' if payload.output_schema is not None else 'inferred'
                    ),
                )
                expected_rows = prepared_copy.row_count
                loader_input = prepared_copy
            else:
                resolved_schema = _resolve_payload_schema(
                    payload, sample_size=self.type_infer_sample_size,
                )
                expected_rows = len(payload.rows)
                loader_input = payload.rows

            staging_table = self._build_table(
                payload, resolved_schema=resolved_schema, table_name=staging_name,
            )

            conn = self.ensure_connection()
            self._ensure_transaction()

            self.log.info(
                'preparing %s.%s as %s rows=%s schema=%s loader=%s',
                payload.schema, payload.table_name, staging_name, expected_rows,
                resolved_schema.source, payload.db_loader,
            )

            # No drop-first: after predecessor cleanup and a fresh run token,
            # an existing exact name is a collision, not something to erase.
            staging_table.create(conn)

            loader = LOADERS[payload.db_loader]
            loaded = loader(
                conn, staging_table, loader_input, self.chunk_size,
            )

            # The final spool contains business data and its key exists only on
            # PreparedCopySource. Remove it before the staging transaction can
            # commit. A cleanup failure on the success path is fatal; on a COPY
            # failure the except block retries cleanup without replacing the
            # primary exception.
            if prepared_copy is not None:
                failed_cleanup = cleanup_spool_paths([prepared_copy.path])
                if failed_cleanup:
                    raise DbPublishError(
                        f'could not remove completed COPY spool(s): '
                        f'{failed_cleanup!r}'
                    )
                prepared_copy = None

            self._verify_prepared_table(
                payload, staging_name, loaded, expected_rows=expected_rows,
            )
            self._attach_staging_comment(payload, staging_name)
            self._commit_transaction()

            full_name = f'{payload.schema}.{payload.table_name}'
            table_result = DbTableResult(
                schema=payload.schema,
                table_name=payload.table_name,
                full_name=full_name,
                rows=expected_rows,
                db_table_id_pix=payload.db_table_id_pix,
            )
            self._generated_names.add(key)
            self._written_tables.append(table_result)
            self._table_rows[full_name] = table_result.rows
            target_key = (payload.schema, payload.table_name)
            self._resolved_schemas[target_key] = resolved_schema
            if payload.publication_strategy == 'refill':
                self._refill_targets.add(target_key)
            self._pending_swaps.append((
                payload.schema, payload.table_name, staging_name, expected_rows,
            ))
            return table_result
        except BaseException:
            if prepared_copy is not None:
                cleanup_spool_paths([prepared_copy.path])
            raise

    def _verify_prepared_table(
        self, payload, staging_name, loaded, *, expected_rows=None,
    ):
        """Mechanical integrity of what was just written, inside the same
        transaction that wrote it.

        Deliberately mechanical only. Business checks -- non-emptiness, key
        uniqueness, value ranges -- belong to tasks, not to the scaffold,
        and putting them here would make every task pay for one task's
        rule.

        ``expected_rows`` is the exact logical source count: INSERT uses the
        materialized mapping length; COPY uses the one-shot count captured
        while preparing the spool. The loader returns the number it attempted
        to send, which catches chunking/dispatch drift before commit. This is
        deliberately not presented as an independent ``COUNT(*)`` check of
        PostgreSQL storage.
        """
        if expected_rows is None:
            expected_rows = len(payload.rows)
        if loaded != expected_rows:
            raise DbPublishInvariantError(
                f'internal invariant violated -- {payload.table_name!r}: loaded '
                f'{loaded} rows into {staging_name} but preparation produced '
                f'{expected_rows}'
            )

        actual = self._reflect_column_names(payload.schema, staging_name)
        if actual is None:
            return

        # Exact ORDERED name equality, not SQL type equality. Ordinal
        # position is trustworthy here precisely because the table was just
        # created -- attnum develops gaps after DROP COLUMN, so this check
        # would need care on a reused table and needs none on a fresh one.
        # Type comparison is deliberately out of scope: it would require a
        # SQLAlchemy-to-information_schema type map that drifts silently
        # when it is not maintained.
        if actual != list(payload.columns):
            raise DbPublishInvariantError(
                f'internal invariant violated -- {payload.table_name!r}: staging '
                f'table {staging_name} has columns {actual}, payload declared '
                f'{list(payload.columns)}'
            )

    def _reflect_column_names(self, schema, table_name):
        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            return None
        result = conn.execute(
            sa.text(
                'select column_name from information_schema.columns '
                'where table_schema = :schema and table_name = :table '
                'order by ordinal_position'
            ),
            {'schema': schema, 'table': table_name},
        )
        return [row[0] for row in result]

    def _attach_staging_comment(self, payload, staging_name):
        """Ownership metadata, committed atomically with the table itself.

        This is what makes cleanup possible without a registry table: the
        catalog carries who owns each staging artifact, readable with one
        obj_description() query and with no second source of truth to fall
        out of sync.
        """
        comment = build_staging_comment(
            task_name=self.task_name,
            run_token=self._run_token,
            schema=payload.schema,
            table_name=payload.table_name,
        )
        self._set_comment(payload.schema, staging_name, comment)

    def _set_comment(self, schema, table_name, comment):
        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            return
        # Do not embed JSON inside sa.text(). TextClause scans colon-number
        # sequences even inside SQL string literals, so compact JSON such as
        # ``"v":1`` is misread as a bind parameter named ``1``. Use the
        # dedicated SQLAlchemy DDL construct: it renders the comment as a
        # literal through the PostgreSQL dialect without bind parsing.
        table = sa.Table(
            sa.quoted_name(table_name, quote=True),
            sa.MetaData(),
            schema=(
                sa.quoted_name(schema, quote=True)
                if schema is not None
                else None
            ),
            comment=comment,
        )
        conn.execute(sa.schema.SetTableComment(table))

    def _commit_transaction(self):
        if self._tx is not None:
            self._tx.commit()
            self._tx = None
        elif self._conn is not None and self._conn.in_transaction():
            self._conn.commit()

    def _verify_prepared_artifacts(self):
        """Verify exact staging identity and ownership inside publication.

        Every check is O(number of tables), never O(rows). The exact catalog
        resolver is shared only with live-target locking; each caller retains
        its own missing-relation and relation-kind policy.
        """
        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            return

        for schema, table_name, staging_name, _rows in self._pending_swaps:
            relation = _find_relation(conn, schema, staging_name)
            if relation is None:
                raise DbPublishError(
                    f'staging table {staging_name} is missing; refusing to publish '
                    f'it over {table_name!r}'
                )

            oid, relkind = relation
            if relkind != 'r':
                raise DbPublishError(
                    f'staging relation {schema}.{staging_name} exists but is not an '
                    f'ordinary table (relkind={relkind!r}); refusing to publish it'
                )

            comment = conn.execute(
                sa.text("select obj_description(:oid, 'pg_class')"),
                {'oid': oid},
            ).scalar()
            owner = parse_staging_comment(comment)
            if owner is None:
                raise DbPublishError(
                    f'staging table {staging_name} no longer carries this run\'s '
                    f'ownership metadata; refusing to publish it over {table_name!r}'
                )
            if owner.get('run') != self._run_token or owner.get('task') != self.task_name:
                raise DbPublishError(
                    f'staging table {staging_name} belongs to task '
                    f'{owner.get("task")!r} run {owner.get("run")!r}, not to this run'
                )
            if owner.get('target_table') != table_name or owner.get('target_schema') != schema:
                raise DbPublishError(
                    f'staging table {staging_name} was prepared for '
                    f'{owner.get("target_schema")}.{owner.get("target_table")}, '
                    f'not {schema}.{table_name}'
                )

    # SQLSTATEs, and why only one of them is retried.
    #
    # 55P03 lock_not_available is unambiguous here: this code never issues
    # NOWAIT, and the connection is exclusively ours -- dedicated, NullPool,
    # never returned to a pool -- so nothing else can be requesting locks on
    # it. It means precisely "some conflicting holder still held a target
    # when my budget expired", which is the retryable condition.
    #
    # It does NOT mean the holder was a reader, which this comment claimed
    # until 0.7.13. Measured against PostgreSQL 18.4: the holder was an
    # autovacuum worker on the live target, taking SHARE UPDATE EXCLUSIVE
    # after the previous publication filled it. That conflicts with ACCESS
    # EXCLUSIVE exactly as a reader would, and it is arguably the more
    # common cause on a table this scaffold has just written. See
    # decisions/0016 for why lock_timeout_ms is not raised to outlast it.
    #
    # 57014 query_canceled is NOT uniquely statement_timeout. An operator's
    # pg_cancel_backend(), a client-side cancel, or a role- or
    # database-level statement_timeout set outside this code all produce
    # it. Retrying would mean the scaffold arguing with a human who
    # deliberately stopped it, so it is terminal -- deliberately
    # conservative, and it costs only the case where retrying might have
    # worked anyway.
    #
    # 40P01 deadlock_detected is retryable in principle, but sorted lock
    # order already prevents deadlock between two task_core publications.
    # Seeing one means something outside this scaffold takes exclusive
    # locks on published tables, which a retry does not fix and which
    # should be loud.
    _RETRYABLE_SQLSTATES = frozenset({'55P03'})
    _TERMINAL_LOUD_SQLSTATES = frozenset({'40P01'})

    @staticmethod
    def _sqlstate(exc):
        return getattr(getattr(exc, 'orig', None), 'pgcode', None)

    @staticmethod
    def _relation_kind_name(relkind):
        return {
            'r': 'ordinary table',
            'p': 'partitioned table',
            'v': 'view',
            'm': 'materialized view',
            'f': 'foreign table',
        }.get(relkind, f'relation kind {relkind!r}')

    def _resolved_schema_for(self, schema, table_name):
        try:
            return self._resolved_schemas[(schema, table_name)]
        except KeyError as exc:
            raise DbPublishInvariantError(
                f'internal invariant violated -- no resolved schema recorded for '
                f'{schema}.{table_name}'
            ) from exc

    def _verify_declared_target_compatibility(
        self, *, schema, table_name, staging_name, target_oid, staging_oid,
    ):
        conn = self.ensure_connection()
        target_columns = _relation_columns(conn, target_oid)
        staging_columns = _relation_columns(conn, staging_oid)
        if target_columns != staging_columns:
            raise DbPublishError(
                f'declared publication target {schema}.{table_name} does not match '
                f'output_schema. The existing table must have the exact same ordered '
                f'column names, PostgreSQL types and modifiers, nullability, collation, '
                f'identity, generated-column, and default metadata as prepared staging table '
                f'{staging_name}. Migrate or recreate the target explicitly before rerunning.'
            )

        incoming = _external_incoming_foreign_keys(conn, target_oid)
        if incoming:
            described = ', '.join(
                f'{fk_schema}.{fk_table} ({constraint})'
                for fk_schema, fk_table, constraint in incoming
            )
            raise DbPublishError(
                f'declared publication target {schema}.{table_name} has incoming '
                f'foreign-key references from {described}. Stable publication uses '
                f'TRUNCATE without CASCADE, so external incoming references require '
                f'explicit handling outside task_core.'
            )

    def _create_and_fill_declared_target(
        self, *, schema, table_name, staging_name, rows,
    ):
        conn = self.ensure_connection()
        resolved = self._resolved_schema_for(schema, table_name)
        metadata = sa.MetaData()
        target_table = sa.Table(
            table_name,
            metadata,
            *[
                sa.Column(column.name, column.type, nullable=column.nullable)
                for column in resolved.columns
            ],
            schema=schema,
        )
        target_table.create(conn)

        target_qualified = _quoted_name(schema, table_name)
        staging_qualified = _quoted_name(schema, staging_name)
        columns = ', '.join(_quote_identifier(column.name) for column in resolved.columns)
        conn.execute(sa.text(
            f'insert into {target_qualified} ({columns}) '
            f'select {columns} from {staging_qualified}'
        ))
        self._set_comment(schema, table_name, build_published_comment(
            task_name=self.task_name, run_token=self._run_token, rows=rows,
        ))
        conn.execute(sa.text(f'drop table {staging_qualified}'))

    def _prepare_declared_targets_before_lock(self):
        """Validate existing stable targets and build absent ones before locks.

        This phase is deliberately before source-state work and before the
        first live-target lock. A new target is invisible until commit, so its
        complete create-and-fill work does not block readers of an existing
        object. Existing targets are only inspected here; their rows remain
        untouched until every live lock is held.
        """
        conn = self.ensure_connection()
        if not self._refill_targets:
            return set()
        if conn.dialect.name != 'postgresql':
            raise DbPublishError(
                'fully declared stable-target publication requires PostgreSQL'
            )

        created = set()
        for schema, table_name, staging_name, rows in sorted(
            self._pending_swaps, key=lambda item: (item[0] or '', item[1])
        ):
            key = (schema, table_name)
            if key not in self._refill_targets:
                continue

            staging_relation = _find_relation(conn, schema, staging_name)
            if staging_relation is None:
                raise DbPublishError(
                    f'prepared staging table {schema}.{staging_name} is missing'
                )
            staging_oid, staging_kind = staging_relation
            if staging_kind != 'r':
                raise DbPublishError(
                    f'prepared staging relation {schema}.{staging_name} is '
                    f'{self._relation_kind_name(staging_kind)}, not an ordinary table'
                )

            target_relation = _find_relation(conn, schema, table_name)
            if target_relation is None:
                self.log.info(
                    'creating first explicit-refill target %s.%s from %s',
                    schema, table_name, staging_name,
                )
                self._create_and_fill_declared_target(
                    schema=schema, table_name=table_name,
                    staging_name=staging_name, rows=rows,
                )
                created.add(key)
                continue

            target_oid, target_kind = target_relation
            if target_kind != 'r':
                raise DbPublishError(
                    f'declared publication target {schema}.{table_name} is '
                    f'{self._relation_kind_name(target_kind)}, not an ordinary table. '
                    f'Stable publication requires an ordinary table target.'
                )
            self._verify_declared_target_compatibility(
                schema=schema,
                table_name=table_name,
                staging_name=staging_name,
                target_oid=target_oid,
                staging_oid=staging_oid,
            )
        return created

    def _refill_declared_target(self, *, schema, table_name, staging_name, rows):
        conn = self.ensure_connection()
        resolved = self._resolved_schema_for(schema, table_name)
        target_qualified = _quoted_name(schema, table_name)
        staging_qualified = _quoted_name(schema, staging_name)
        columns = ', '.join(_quote_identifier(column.name) for column in resolved.columns)

        self.log.info('refilling explicit stable target %s from %s', target_qualified, staging_qualified)
        conn.execute(sa.text(f'truncate table {target_qualified}'))
        conn.execute(sa.text(
            f'insert into {target_qualified} ({columns}) '
            f'select {columns} from {staging_qualified}'
        ))
        self._set_comment(schema, table_name, build_published_comment(
            task_name=self.task_name, run_token=self._run_token, rows=rows,
        ))
        conn.execute(sa.text(f'drop table {staging_qualified}'))

    def _lock_publication_targets(self, deadline, *, exclude_targets=()):
        """Take ACCESS EXCLUSIVE on every existing target, in one statement.

        One statement rather than letting each DROP acquire its own,
        because the incremental form holds locks on already-swapped tables
        while queuing for the next -- and each held lock is itself blocking
        new readers, so the amplification compounds. Either every lock is
        held here or none is.

        Sorted, which is what stops two tasks with overlapping targets
        deadlocking against each other.

        Budgets are derived from the remaining horizon rather than taken
        from the policy directly: the horizon gates COMPLETION of this
        phase, so a final attempt may legitimately run with far less than
        the configured timeout. SET LOCAL, so neither setting escapes the
        transaction.
        """
        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            return []

        excluded = set(exclude_targets)
        targets = sorted(
            {
                (schema, table)
                for schema, table, _staging, _rows in self._pending_swaps
                if (schema, table) not in excluded
            }
        )
        existing = []
        for schema, table in targets:
            relation = _find_relation(conn, schema, table)
            if relation is None:
                continue

            _oid, relkind = relation
            if relkind != 'r':
                raise DbPublishError(
                    f'publication target {schema}.{table} exists but is not an '
                    f'ordinary table (relkind={relkind!r})'
                )
            existing.append((schema, table))
        if not existing:
            # A first-ever publication has nothing to lock.
            return []

        policy = self.publication_lock_policy
        target_count = len(existing)
        required_ms = (
            target_count * policy.lock_timeout_ms + policy.TIMEOUT_MARGIN_MS
        )
        if policy.acquisition_timeout_ms < required_ms:
            max_lock_ms = (
                policy.acquisition_timeout_ms - policy.TIMEOUT_MARGIN_MS
            ) // target_count
            if max_lock_ms >= 1:
                advice = (
                    f'Lower lock_timeout_ms to at most {max_lock_ms}ms, raise '
                    'acquisition_timeout_ms deliberately, or split the publication.'
                )
            else:
                advice = (
                    'No positive lock_timeout_ms fits this target count under the '
                    'current aggregate budget; publish fewer targets together or '
                    'raise acquisition_timeout_ms deliberately.'
                )
            raise DbPublishError(
                'publication lock policy cannot preserve retry classification for '
                f'{target_count} existing target(s): acquisition_timeout_ms must be '
                f'at least n * lock_timeout_ms + margin = {required_ms}ms '
                f'({target_count} * {policy.lock_timeout_ms} + '
                f'{policy.TIMEOUT_MARGIN_MS}), got '
                f'{policy.acquisition_timeout_ms}ms. Otherwise cumulative waits may '
                'raise terminal 57014 before an individual wait reaches retryable '
                f'55P03. {advice}'
            )

        budgets = policy.attempt_budgets_ms(
            deadline - time.monotonic(), target_count=target_count,
        )
        if budgets is None:
            raise DbPublishError(
                'publication horizon exhausted before its target locks could be '
                'requested with a usable timeout budget for multiple targets'
            )
        statement_ms, lock_ms = budgets

        conn.execute(sa.text(f'set local lock_timeout = {lock_ms}'))
        conn.execute(sa.text(f'set local statement_timeout = {statement_ms}'))

        names = ', '.join(_quoted_name(schema, table) for schema, table in existing)
        self.log.info('locking %s publication target(s) for swap', len(existing))
        conn.execute(sa.text(f'lock table {names} in access exclusive mode'))

        # Budgets lifted once the locks are held. The horizon bounds lock
        # ACQUISITION, not the publication critical section. Replacement is
        # normally catalog-time work; explicit stable refill includes
        # TRUNCATE, row-proportional INSERT, index/constraint maintenance and
        # commit, so its post-acquisition duration is not bounded here.
        conn.execute(sa.text('set local lock_timeout = 0'))
        conn.execute(sa.text('set local statement_timeout = 0'))
        return existing

    def commit(self):
        """Publish every prepared target and queued source-state change atomically.

        Replacement targets use the short DROP/RENAME path. Explicit refill
        targets preserve their ordinary-table identity with TRUNCATE plus a
        second row write, so this transaction is not necessarily short.

        Source-state work and refill preflight run before the first live-target
        lock. Publication comments follow each replacement or refill because
        they belong to the final live relation.
        """
        # Queued work counts. A publication plan holding only the
        # source-state update writes to the database just as a swap does,
        # and could previously be committed by a direct caller without ever
        # claiming the task.
        if self._pending_swaps or len(self.publication_plan):
            self._require_task_lock('commit')

        policy = self.publication_lock_policy
        started = time.monotonic()
        deadline = started + policy.retry_horizon_seconds
        attempt = 0

        while True:
            attempt += 1
            try:
                return self._publish_once(deadline)
            except BaseException as exc:
                # Rolled back on EVERY unsuccessful attempt, before any
                # classification. Catching only DBAPIError left the
                # publication transaction OPEN whenever the failure was
                # anything else -- a DbPublishError from an exhausted
                # horizon, an invariant violation, or KeyboardInterrupt --
                # after _publish_once() had already opened the transaction,
                # verified the staging artifacts, and possibly run the
                # source-state plan. The runner's cleanup happens to reach
                # rollback() eventually; a direct caller, or one that
                # catches the exception, was left holding a dirty
                # transaction.
                self._drop_open_transaction()

                if not isinstance(exc, sa.exc.DBAPIError):
                    raise

                state = self._sqlstate(exc)

                if state in self._TERMINAL_LOUD_SQLSTATES:
                    # Deliberately phase-neutral. Target locking is sorted,
                    # so it cannot deadlock two task_core publications
                    # against each other -- but the publication plan now
                    # runs BEFORE locking, and a deadlock there is
                    # reachable with zero lock attempts. Naming the target
                    # locks would misdirect the investigation.
                    self.log.error(
                        'publication encountered a deadlock (%s) on attempt %s. '
                        'Automatic retry is disabled: a deadlock indicates an '
                        'external lock-order conflict that needs investigation '
                        'rather than repetition.', state, attempt,
                    )
                    raise
                if state not in self._RETRYABLE_SQLSTATES:
                    raise

                elapsed = time.monotonic() - started
                remaining = deadline - time.monotonic()

                # The jitter range is DERIVED from what is left, not sampled
                # and then rejected. Sampling first threw the run away
                # whenever the draw happened to exceed the remaining
                # horizon -- with 4s left and a 1-5s range, a 4.5s draw
                # ended it while a shorter delay and another bounded
                # attempt would have fitted, and the error then claimed the
                # horizon was exhausted while deliberately leaving part of
                # it unused.
                reserved = policy.acquisition_timeout_ms / 1000.0
                latest_delay = remaining - reserved

                # ACTUAL elapsed and attempts, not the configured policy:
                # budgets are derived from the remaining horizon, so a final
                # attempt may have run with far less than
                # acquisition_timeout_ms and the two would not reconcile.
                if attempt >= policy.max_attempts:
                    raise DbPublishError(
                        f'publication could not acquire its target locks: {attempt} '
                        f'attempts in {elapsed:.1f}s hit the defensive max_attempts '
                        f'ceiling of {policy.max_attempts}'
                    ) from exc

                # Stop rather than sleep past the horizon: sleeping in order
                # to give up wastes the wait and holds the task advisory
                # lock longer for nothing.
                if latest_delay < policy.retry_delay_min_seconds:
                    raise DbPublishError(
                        f'publication could not acquire its target locks within '
                        f'{policy.retry_horizon_seconds}s ({attempt} attempts, '
                        f'{elapsed:.1f}s elapsed). Something holds a conflicting '
                        f'lock on a target: a long-running reader, or an autovacuum '
                        f'worker on a table this run has just filled. Join pg_locks '
                        f'to pg_stat_activity to name it.'
                    ) from exc

                delay = random.uniform(
                    policy.retry_delay_min_seconds,
                    min(policy.retry_delay_max_seconds, latest_delay),
                )

                self.log.warning(
                    'publication lock unavailable (attempt %s, %.1fs elapsed, %.1fs '
                    'of horizon left); retrying in %.1fs',
                    attempt, elapsed, remaining, delay,
                )
                # Already rolled back above, so nothing is held across the
                # sleep -- holding locks while waiting is the disease.
                time.sleep(delay)

    def _publish_once(self, deadline):
        conn = self.ensure_connection()
        self._ensure_transaction()

        self._verify_prepared_artifacts()

        # Only explicit-refill targets need stable-target preflight. Absent
        # refill targets are created and completely filled inside this
        # transaction while still invisible to other sessions. Existing refill
        # targets are only validated here; their rows remain untouched until
        # all locks exist. Replacement targets need no compatibility preflight.
        created_refill_targets = self._prepare_declared_targets_before_lock()

        if len(self.publication_plan):
            self.publication_plan.run(self.log)

        # Locks taken LAST, immediately before the swaps/refills. The publication
        # plan is not harmless catalog work: on the standard runner path it
        # runs create-if-not-exists, a DELETE and an upsert against the
        # source-state table. With the locks already held -- and both
        # timeouts reset to zero the moment they were acquired -- any wait
        # in that work kept every live target under ACCESS EXCLUSIVE for
        # its whole duration, recreating the read outage this is supposed
        # to bound.
        #
        # Atomicity is unchanged: a 55P03 here still rolls back the
        # source-state write with everything else and the whole attempt is
        # retried. The exclusive window now contains replacement catalog work
        # and any explicitly selected row-dependent refills.
        if created_refill_targets:
            self._lock_publication_targets(
                deadline, exclude_targets=created_refill_targets,
            )
        else:
            self._lock_publication_targets(deadline)

        swapped = []
        # Sorted by final name. Two tasks publishing an overlapping set of
        # tables in different orders could otherwise deadlock against each
        # other on these locks; a deterministic global order removes that
        # class of problem for free.
        for schema, table_name, staging_name, rows in sorted(
            self._pending_swaps, key=lambda item: (item[0] or '', item[1])
        ):
            qualified = _quoted_name(schema, table_name)
            staging_qualified = _quoted_name(schema, staging_name)

            key = (schema, table_name)
            if key in self._refill_targets:
                if key not in created_refill_targets:
                    self._refill_declared_target(
                        schema=schema, table_name=table_name,
                        staging_name=staging_name, rows=rows,
                    )
                swapped.append(f'{schema}.{table_name}' if schema else table_name)
                continue

            self.log.info('publishing %s from %s', qualified, staging_qualified)
            conn.execute(sa.text(f'drop table if exists {qualified}'))
            # RENAME takes the new name UNQUALIFIED -- the table keeps the
            # schema it was created in, and PostgreSQL rejects a qualified
            # target here rather than silently moving it.
            conn.execute(sa.text(
                f'alter table {staging_qualified} rename to {_quote_identifier(table_name)}'
            ))
            # ALTER TABLE ... RENAME preserves the comment, so without this
            # every published table would carry staging ownership metadata
            # and look to cleanup like an abandoned artifact. Replacing it
            # is both the safeguard and useful provenance.
            self._set_comment(schema, table_name, build_published_comment(
                task_name=self.task_name, run_token=self._run_token, rows=rows,
            ))
            swapped.append(f'{schema}.{table_name}' if schema else table_name)

        self._commit_transaction()
        self._pending_swaps = []
        self._resolved_schemas = {}
        self._refill_targets = set()
        self.publication_plan.clear()

        self._committed = True
        self._committed_tables = list(self._written_tables)
        table_names = [item.full_name for item in self._committed_tables]
        self.log.info('publication committed, tables=%s', ', '.join(table_names) or 'none')
        return list(self._committed_tables)

    def rollback(self):
        """Abort the unpublished run.

        Its meaning changed with the staged model. Preparation transactions
        are already committed, so this cannot roll them back -- it must
        DROP this run's staging tables instead. That makes rollback capable
        of failing for new reasons, notably a lost connection, so it never
        raises: the original exception that caused the abort matters more
        than a cleanup failure, and losing it would be the worse outcome.

        Must run while the task advisory lock is still held. Releasing
        first would let a waiting run start while this one is still
        dropping tables it believes it owns.
        """
        self._pending_swaps = []
        self._resolved_schemas = {}
        self._refill_targets = set()
        self.publication_plan.clear()
        self._committed = False
        self._committed_tables = []
        self._written_tables = []
        self._table_rows = {}

        if self._note_connection_loss() or self._conn is None:
            # Nothing to do and nothing possible. PostgreSQL releases the
            # session lock on its own, and the next run cleans the orphans
            # under its own lock.
            self._drop_open_transaction()
            return

        self._drop_open_transaction()

        dropped = []
        for schema, staging_name in sorted(self._generated_names):
            try:
                self._conn.execute(
                    sa.text(f'drop table if exists {_quoted_name(schema, staging_name)}')
                )
                self._conn.commit()
                dropped.append(staging_name)
            except Exception:
                # break, not continue. A failed DROP leaves the PostgreSQL
                # transaction in an aborted state, so every subsequent
                # statement fails too -- continuing would produce a cascade
                # of misleading errors rather than dropping more tables.
                # 'Drop as many as possible' is the plausible-looking edit
                # here; it does not work without an intervening rollback,
                # and the orphans are safely the next run's problem either
                # way.
                self.log.warning(
                    'could not drop staging table %s during rollback; it and any '
                    'remaining ones will be cleaned by the next run of this task',
                    staging_name, exc_info=True,
                )
                break

        self._generated_names = set()
        # Reports what this actually did, not why it was called. The old
        # message was 'run aborted; staging artifacts dropped
        # best-effort', logged unconditionally -- but rollback() serves
        # two callers, and only one of them is an abort. The other is the
        # source-change skip, which calls it to release the open read
        # transaction after finding the sources unchanged. That is a
        # normal, successful outcome, and it ended every such run with a
        # line saying the run had been aborted.
        #
        # Nothing was wrong underneath: _generated_names is empty on that
        # path, so no table was dropped and nothing was rolled back but a
        # SELECT. The message was the whole defect, and the abort path
        # loses nothing -- its caller logs 'task %s failed' immediately
        # afterwards.
        if dropped:
            self.log.info(
                'dropped %s staging table(s) prepared by this run: %s',
                len(dropped), ', '.join(dropped),
            )

    def _drop_open_transaction(self):
        if self._tx is not None:
            try:
                self._tx.rollback()
            except Exception:
                self.log.warning('rolling back the open transaction failed', exc_info=True)
            self._tx = None
        elif self._conn is not None:
            try:
                if self._conn.in_transaction():
                    self._conn.rollback()
            except Exception:
                self.log.warning('rolling back the open transaction failed', exc_info=True)

    def cleanup_predecessor_artifacts(self):
        """Drop staging tables left by a dead previous run of this task.

        Safe without any age threshold, and that is the point. This runs
        while holding the task's advisory lock, which means no other run of
        this task is live -- so any staging artifact positively identified
        as belonging to this task belongs to a predecessor that is gone.
        No timestamp to compare, no window to tune, no race with a running
        peer.

        Positively identified is the operative phrase. A table whose
        comment is missing, unparseable, or of an unrecognised version has
        UNKNOWN ownership, and unknown is never dropped. That rule is what
        keeps cleanup from becoming an outage.
        """
        # Enforced here, not merely by begin_run() calling it in the right
        # order. This method drops tables; leaving a load-bearing invariant
        # to caller ordering means a direct caller can delete another live
        # run's artifacts -- confirmed directly, it dropped a staging table
        # with lock_held False.
        self._require_task_lock('clean up predecessor staging artifacts')

        conn = self.ensure_connection()
        if conn.dialect.name != 'postgresql':
            return []

        rows = conn.execute(
            sa.text(
                'select c.relname, obj_description(c.oid, \'pg_class\') '
                'from pg_class c join pg_namespace n on n.oid = c.relnamespace '
                'where n.nspname = :schema and c.relkind = \'r\' '
                'and c.relname like :pattern'
            ),
            # Every underscore escaped. Leaving the leading two unescaped
            # made them single-character wildcards, so the scan also
            # returned names like 'xastg_...'. The strict regex filtered
            # them out anyway, but a pattern should say what it means.
            {'schema': self.schema, 'pattern': f'%\\_\\_{STAGING_NAME_KIND}\\_%'},
        ).all()

        dropped = []
        for relname, comment in rows:
            # Strict physical name AND valid ownership metadata AND a
            # matching task -- all three, not any one. The SQL LIKE cannot
            # express the token shapes, so the pattern check happens here.
            tokens = owned_staging_tokens(relname)
            if tokens is None:
                continue
            target_token, run_token = tokens

            owner = parse_staging_comment(comment)
            if owner is None or owner.get('task') != self.task_name:
                continue
            if run_token == self._run_token:
                continue

            # The name and the comment must agree with each other, not
            # merely each be well-formed on its own. A comment naming a
            # different schema, a different run, or a logical target that
            # does not hash to this name's target token is not positive
            # identification of this artifact.
            if owner.get('target_schema') != self.schema:
                continue
            if owner.get('run') != run_token:
                continue
            if staging_target_token(owner.get('target_schema'), owner.get('target_table')) != target_token:
                continue

            conn.execute(sa.text(f'drop table if exists {_quoted_name(self.schema, relname)}'))
            dropped.append(relname)

        if dropped:
            self.log.warning(
                'dropped %s staging table(s) left by a previous run of %s: %s',
                len(dropped), self.task_name, ', '.join(dropped),
            )
        conn.commit()
        return dropped

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
        # Explicit unlock before closing rather than relying on session
        # end. Today make_engine() uses NullPool so the two are
        # equivalent -- but the dedicated-connection contract rests on
        # NullPool, and an explicit unlock keeps this honest if that ever
        # changes. Failures here must not mask whatever is already
        # unwinding.
        if self.lock_held:
            try:
                self.release_task_lock()
            except Exception:
                self.log.warning('releasing the task advisory lock failed', exc_info=True)
                self._locked_task_name = None

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

    def _build_table(self, payload: DbPayload, *, resolved_schema, table_name=None):
        metadata = sa.MetaData()
        columns = [
            sa.Column(column.name, column.type, nullable=column.nullable)
            for column in resolved_schema.columns
        ]
        return sa.Table(
            table_name or payload.table_name, metadata, *columns, schema=payload.schema
        )

