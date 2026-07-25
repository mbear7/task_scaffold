# -*- coding: utf-8 -*-

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import logging
from itertools import islice
from uuid import uuid4
from typing import Any, Iterator

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.pool import NullPool

from task_core.cleanup import attempt_all_cleanup
from task_core.types import IDENTIFIER_MODES, PORTABLE_IDENTIFIER_RE, find_duplicates


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
    # Carried from PipelineSpec.db_identifier_mode so publish() can apply
    # the right column-name policy at the only point where the real column
    # names exist. Defaults to the strict mode, so a payload built directly
    # (tests, ad-hoc callers) is validated rather than waved through.
    identifier_mode: str = 'portable'


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


def from_petl(tbl, *, table_name, schema, type_overrides=None, db_contract=None, db_table_id_pix=None, identifier_mode='portable'):
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
        identifier_mode=identifier_mode,
    )



def from_pandas(df: pd.DataFrame, *, table_name, schema, type_overrides=None, db_contract=None, db_table_id_pix=None, identifier_mode='portable'):
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
        identifier_mode=identifier_mode,
    )


class DbPublishInvariantError(DbPublishError):
    """An internal guarantee of this module failed -- not something a task
    author did wrong, and not something they can fix. Raised only where
    the code is supposed to have made a failure impossible by
    construction: a generated staging identifier over the byte limit, or
    two generated names colliding after preflight already proved they
    could not.

    Subclasses DbPublishError deliberately, so existing cleanup paths that
    catch DbPublishError still catch it, while isinstance() still tells the
    two apart. The distinction is for diagnostics: the message says the
    invariant was violated rather than implying a bad declaration.
    """


# PostgreSQL truncates any identifier past NAMEDATALEN-1 = 63 BYTES, and
# announces it with a NOTICE rather than an error -- and psycopg2 exposes
# notices on the connection, where nothing in this project reads them. So an
# over-long identifier does not fail; it quietly becomes a different name
# than the one this code believes it created.
#
# Canonical default only. NAMEDATALEN is compile-time configurable, so this
# is an assumption about a stock build, not a fact about the server in
# front of us. Three levels, in increasing authority: this constant, a
# constructor-injected override for tests and nonstandard builds, and the
# server's own max_identifier_length read before the first DDL. The
# configured value can only ever LOWER the effective limit, never raise it
# past what the server will actually accept.
MAX_IDENTIFIER_BYTES = 63

# Staging is a named internal namespace constant, not a live parameter.
# Passing a 'purpose' argument would advertise supported variation that
# does not exist -- there is exactly one kind of generated table today.
# Generalize when a second use case actually arrives, not before.
STAGING_NAME_KIND = 'stg'

_STAGING_TOKEN_HEX = 8
_RUN_TOKEN_HEX = 8


def validate_identifier(name, max_bytes, *, kind, context='', invariant=False):
    """Every generated or declared identifier passes through here before it
    reaches SQL. `invariant=True` marks a name this module constructed
    itself, where a failure means this module is broken rather than the
    task being wrong -- see DbPublishInvariantError.
    """
    if not isinstance(name, str) or not name:
        raise DbPublishError(f'{context}empty or non-string {kind}: {name!r}')

    if '\x00' in name:
        raise DbPublishError(f'{context}{kind} contains a NUL byte: {name!r}')

    actual = len(name.encode('utf-8'))
    if actual > max_bytes:
        error = DbPublishInvariantError if invariant else DbPublishError
        prefix = 'internal invariant violated -- ' if invariant else ''
        raise error(
            f'{prefix}{context}PostgreSQL {kind} exceeds limit: '
            f'{actual} bytes, maximum {max_bytes}: {name!r}'
        )
    return name


def validate_portable_identifier(name, *, kind, context=''):
    # fullmatch(), not match(). Python's `$` also matches immediately
    # before a trailing newline, so match() accepted 'foo\n' as portable --
    # confirmed directly. That name is interpolated unquoted into
    # source-state SQL (where the newline is just whitespace) and quoted
    # for output tables (where it becomes part of the identifier). Neither
    # is what this convention promises.
    if not PORTABLE_IDENTIFIER_RE.fullmatch(name):
        raise DbPublishError(
            f'{context}{kind} is not a portable identifier '
            f'({PORTABLE_IDENTIFIER_RE.pattern}): {name!r}. '
            f"Rename it, or set db_identifier_mode='quoted' on the pipeline spec."
        )
    return name


def server_identifier_limit(conn, configured):
    """The identifier byte limit actually in force: the lower of what the
    caller configured and what the server reports.

    Configuration can only ever TIGHTEN. A configured value larger than the
    server's would produce names the server silently truncates, which is
    the failure this whole mechanism exists to prevent.

    Branches on the dialect rather than catching every exception. A
    catch-all exists to accommodate backends with no such setting, but it
    also swallows real PostgreSQL failures -- which makes the authoritative
    runtime check not authoritative, since any error silently restores the
    assumed value. Worse, the statement may run inside an open transaction,
    and a failed statement leaves a PostgreSQL transaction aborted, so the
    next DDL fails with a secondary transaction-aborted error obscuring the
    real cause.

    Module-level rather than a DbPublisher method so source_state.py can
    use it for the technical table without the publisher protocol growing
    another member -- that protocol is an advertised extension seam and has
    already been expanded once by accident.
    """
    if conn.dialect.name != 'postgresql':
        return configured

    try:
        value = conn.execute(sa.text('show max_identifier_length')).scalar()
    except Exception as exc:
        raise DbPublishError(
            'could not read max_identifier_length from PostgreSQL; refusing to '
            'assume a limit that generated identifiers would then be silently '
            'truncated against'
        ) from exc

    return min(configured, int(value))


def _quote_identifier(name):
    """Double-quote for interpolation into DDL that cannot be parameterised.
    DROP TABLE and ALTER TABLE ... RENAME take identifiers, not bind
    parameters. Embedded quotes are doubled.
    """
    return '"' + str(name).replace('"', '""') + '"'


def _quoted_name(schema, table_name):
    if schema:
        return f'{_quote_identifier(schema)}.{_quote_identifier(table_name)}'
    return _quote_identifier(table_name)


def _truncate_utf8(text, max_bytes):
    """Cut to a byte budget without ever emitting a partial character.

    Bytes, not characters, because PostgreSQL's limit is bytes and this
    project handles Russian data: confirmed directly that a 62-character
    Cyrillic name is 116 UTF-8 bytes, so truncating to 41 *characters*
    still leaves 77 bytes and blows the budget anyway.

    errors='ignore' drops a trailing multi-byte sequence the slice cut in
    half, rather than producing invalid UTF-8.
    """
    encoded = text.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode('utf-8', errors='ignore')


def staging_target_token(schema, table_name):
    """The collision-bearing half. A function of the TARGET only -- schema,
    final table name, and the namespace constant -- and deliberately not of
    the run, nor of pipeline position.

    Excluding the run is what makes cross-spec collisions statically
    checkable: preflight computes exactly the tokens the real run will use,
    so a collision is caught before any resource is built rather than
    depending on which run id happened to come up. Folding the run into
    this hash instead would make two targets collide under one run and not
    another.

    Excluding position is what keeps a repeated publication of the same
    target detectable: it produces the same name, so the generated-name
    registry sees it. Including position would produce two different
    staging names that both swap into one final table, silently -- the same
    overwrite class that duplicate-target rejection exists to prevent,
    reappearing a layer down.
    """
    material = '\x1f'.join((schema or '', table_name, STAGING_NAME_KIND))
    return hashlib.blake2b(material.encode('utf-8'), digest_size=_STAGING_TOKEN_HEX // 2).hexdigest()


def staging_table_name(schema, table_name, run_token, *, max_bytes=MAX_IDENTIFIER_BYTES):
    """`<shortened readable prefix>__stg_<target_token>_<run_token>`, e.g.

        employee_funnel__stg_a13f294c_7b32e910

    Only the human-readable prefix is ever shortened. The uniqueness-bearing
    suffix is fixed width and is never truncated -- truncating it would
    defeat the entire reason it exists. The suffix being fixed width is also
    what lets preflight calculate the full length statically.
    """
    suffix = f'__{STAGING_NAME_KIND}_{staging_target_token(schema, table_name)}_{run_token}'
    return _truncate_utf8(table_name, max_bytes - len(suffix.encode('utf-8'))) + suffix


def new_run_token():
    return uuid4().hex[:_RUN_TOKEN_HEX]


class DbPublisher:
    def __init__(
        self,
        *,
        creds,
        schema,
        logger=None,
        chunk_size=5000,
        type_infer_sample_size=5000,
        max_identifier_bytes=MAX_IDENTIFIER_BYTES,
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
        self.max_identifier_bytes = max_identifier_bytes
        self._pending_swaps = []
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

    @classmethod
    def preflight(cls, specs, *, schema, source_state_target=None,
                  max_identifier_bytes=MAX_IDENTIFIER_BYTES):
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
        declaring = {name: spec for name, spec in specs.items() if spec.db_table}
        if not declaring and not source_state_target:
            return

        # Schema is task-wide, so it is always portable and never governed
        # by any pipeline's db_identifier_mode -- see PipelineSpec's own
        # note on why a per-spec flag must not reach a per-run value.
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
            portable = spec.db_identifier_mode == 'portable'

            validate_identifier(spec.db_table, max_identifier_bytes, kind='table name', context=context)
            if portable:
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
                if portable:
                    validate_portable_identifier(column, kind='column name', context=context)

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
        if isinstance(spec.db_updated_at, str):
            targets.append(spec.db_updated_at)
        return targets

    def _effective_identifier_limit(self):
        if self._server_identifier_bytes is None:
            self._server_identifier_bytes = server_identifier_limit(
                self.ensure_connection(), self.max_identifier_bytes,
            )
        return min(self.max_identifier_bytes, self._server_identifier_bytes)


    def _validate_payload_identifiers(self, payload, limit):
        """The runtime half: the actual column names, after db_contract has
        been applied, after any db_output projection, and after
        apply_db_updated_at() has appended its column -- which is the only
        point at which they all exist. Preflight covers what is statically
        declarable; this covers dynamic contracts and pipelines that
        declare no contract at all.

        Placement is load-bearing, not incidental. Confirmed directly: run
        before the contract, this rejects 77 of 79 source names (raw
        Cyrillic spreadsheet headers) and breaks every hr_task pipeline;
        run after, all 145 target names pass. Do not move it upstream.
        """
        validate_identifier(payload.table_name, limit, kind='table name')
        if payload.schema:
            validate_identifier(payload.schema, limit, kind='schema')

        # Portability on the table name and schema, not only on columns.
        # The standard runner path catches a bad table through preflight,
        # but a DbPayload built directly does not go through it -- and this
        # function already validates the payload's own identifier_mode,
        # which means direct construction is treated as part of the
        # contract. Under that contract the strict default was permitting a
        # non-portable table name, confirmed directly.
        #
        # The schema is checked regardless of the payload's mode, matching
        # the rule preflight applies: schema is task-wide while the mode is
        # per-payload, so a per-payload flag must not be able to relax it.
        if payload.schema:
            validate_portable_identifier(
                payload.schema, kind='schema', context=f'{payload.table_name!r}: ',
            )

        context = f'{payload.table_name!r}: '

        # Validated, not merely compared. PipelineSpec checks its own mode,
        # but a DbPayload built directly does not go through it -- and
        # `mode == 'portable'` means any typo ('portbale') silently selects
        # the permissive branch. Confirmed directly: a misspelled mode let
        # a Cyrillic column through.
        if payload.identifier_mode not in IDENTIFIER_MODES:
            raise DbPublishError(
                f'{context}identifier_mode must be one of {IDENTIFIER_MODES}, '
                f'got {payload.identifier_mode!r}'
            )
        portable = payload.identifier_mode == 'portable'
        if portable:
            validate_portable_identifier(payload.table_name, kind='table name', context=context)

        for column in payload.columns:
            validate_identifier(column, limit, kind='column name', context=context)
            if portable:
                validate_portable_identifier(column, kind='column name', context=context)

    def publish(self, payload: DbPayload):
        """Stage only. The live table is not touched here -- see
        finalize_published_tables() for the swap.

        Previously this did DROP + CREATE + INSERT on the live table
        directly, inside the pipeline loop, while the run's single commit
        came only at the very end. That took an ACCESS EXCLUSIVE lock on
        the published table at its first publish and held it for the whole
        remainder of the run. Confirmed directly by instrumenting a
        three-pipeline run: the first table was locked for 3.08s of a
        4.62s run, and the work filling that window was other pipelines
        opening remote files and writing Excel -- nothing to do with the
        locked table. On a task publishing eight tables from SMB-hosted
        workbooks, the first table is unavailable for very nearly the
        entire run.

        Rows now go into a per-run staging table with the freshly inferred
        schema, and the live table stays readable until the publication
        phase swaps it. Schema evolution is completely unaffected: the
        staging table's columns are whatever _infer_column_type() decided
        this run, exactly as before. This is deliberately NOT a
        TRUNCATE + INSERT into the existing table, which would require the
        old schema to stay compatible and push column migration back onto
        every task.
        """
        self._ensure_transaction()

        limit = self._effective_identifier_limit()
        self._validate_payload_identifiers(payload, limit)

        staging_name = staging_table_name(
            payload.schema, payload.table_name, self._run_token, max_bytes=limit,
        )
        # Asserted immediately after generation. Should be impossible --
        # staging_table_name() sizes the prefix to fit -- which is exactly
        # why it is worth asserting: it protects the guarantee against a
        # future edit that quietly breaks it.
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
        self._generated_names.add(key)

        staging_table = self._build_table(payload, table_name=staging_name)

        self.log.info(
            'staging db table %s.%s as %s rows=%s',
            payload.schema, payload.table_name, staging_name, len(payload.rows),
        )

        # checkfirst on the STAGING name: within one transaction this
        # cannot already exist (duplicate db_table targets are rejected by
        # validate_pipeline_classes() before any of this runs), but a
        # retry after a rolled-back run costs nothing to tolerate.
        staging_table.drop(self._conn, checkfirst=True)
        staging_table.create(self._conn)

        if payload.rows:
            insert_stmt = staging_table.insert()
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
        # Reported under the FINAL name, never the staging one -- the
        # RunResult is what tasks log, and a staging identifier appearing
        # there would be noise at best and misleading at worst.
        self._written_tables.append(table_result)
        self._table_rows[full_name] = table_result.rows
        self._pending_swaps.append((payload.schema, payload.table_name, staging_name))

    def _finalize_published_tables(self):
        """The publication phase: drop each live table and rename its
        staging table into place. Called by run_pipelines() after the
        pipeline loop and after update_source_state(), immediately before
        commit() -- which is what holds the ACCESS EXCLUSIVE window to its
        floor rather than spanning the run.

        PRIVATE, and called by commit() rather than by the runner. The
        first version exposed this publicly and relied on run_pipelines()
        to call it, which was wrong twice over. Confirmed directly against
        a real engine: publish() followed by commit(), without the runner
        in between, reported committed=True and committed_tables=['None.t']
        while the live table still held its old rows and the staging table
        was committed permanently -- a publisher that reports success
        having published nothing. And it expanded the publisher_factory
        protocol, which is explicitly a testing and extension seam: an
        otherwise-valid publisher written against the previous contract
        died with AttributeError at the end of a successful run.

        Folding it into commit() fixes both: publish() + commit() is
        correct standalone, finalization still happens immediately before
        the real commit, and the staging protocol stays inside the
        publisher instead of leaking into every fake.

        Sorted by final name, deliberately. Two tasks publishing an
        overlapping set of tables in different orders would otherwise be
        able to deadlock against each other on these locks; a deterministic
        global order removes that class of problem for free.

        Still one transaction, so atomicity is exactly as before: either
        every table swaps and the source state advances, or nothing does.
        """
        if not self._pending_swaps:
            return []

        self._ensure_transaction()
        swapped = []

        for schema, table_name, staging_name in sorted(self._pending_swaps, key=lambda item: (item[0] or '', item[1])):
            qualified = _quoted_name(schema, table_name)
            staging_qualified = _quoted_name(schema, staging_name)

            self.log.info('publishing %s from %s', qualified, staging_qualified)
            self._conn.execute(sa.text(f'drop table if exists {qualified}'))
            # RENAME takes the new name UNQUALIFIED -- the table keeps the
            # schema it was created in, and PostgreSQL rejects a qualified
            # target here rather than silently moving it.
            self._conn.execute(sa.text(
                f'alter table {staging_qualified} rename to {_quote_identifier(table_name)}'
            ))
            swapped.append(f'{schema}.{table_name}' if schema else table_name)

        self._pending_swaps = []
        return swapped

    def commit(self):
        # Finalization is part of committing, not a separate step callers
        # must remember -- see _finalize_published_tables() for what went
        # wrong when it was separate.
        self._finalize_published_tables()

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
        # Staged tables are undone by the rollback itself -- PostgreSQL's
        # DDL is transactional, so the CREATEs vanish with everything else
        # and there is no orphaned staging table to clean up afterwards.
        # This list is cleared because it describes swaps that will now
        # never happen, not because anything needs dropping.
        self._pending_swaps = []
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

    def _build_table(self, payload: DbPayload, *, table_name=None):
        metadata = sa.MetaData()

        def _column_type(col_name):
            type_obj = _resolve_override((payload.type_overrides or {}).get(col_name))
            if type_obj is not None:
                return type_obj
            return _infer_column_type(payload.rows, col_name, sample_size=self.type_infer_sample_size)

        columns = [sa.Column(col_name, _column_type(col_name)) for col_name in payload.columns]

        return sa.Table(table_name or payload.table_name, metadata, *columns, schema=payload.schema)


def _chunked(rows: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i:i + chunk_size]



def _is_scalar_like(value):
    """True for a genuine scalar, and also for a zero-dimensional numpy
    array, which wraps exactly one scalar and has no container semantics
    to preserve.

    `pd.api.types.is_scalar()` alone says False for `np.array(5)` -- it is
    an ndarray, so it is not a scalar -- which is correct as far as it
    goes and wrong for what both callers below actually need to know.
    Confirmed directly: `np.array(5)` reached the DB driver as an array
    rather than the int 5, while `np.array([5])` is correctly preserved as
    a real, one-element container. `.ndim == 0` is the exact line between
    those two, and it is the same line for both callers, which is why this
    is one shared predicate rather than a check duplicated in each.

    This also closes an asymmetry that predates the zero-dimensional
    question and is easy to miss, confirmed directly across every dtype
    tried: `pd.isna()` returns a plain numpy bool for a *typed* zero-dim
    array (float64, datetime64) but a zero-dim *array* for an object one.
    So `np.array(np.nan)` already normalized to None, while
    `np.array(pd.NaT)`, `np.array(None)` and `np.array(pd.NA)` did not --
    four values that all hold nothing, behaving two different ways
    depending only on the dtype numpy happened to pick. Applying this
    predicate to `pd.isna()`'s own result in is_missing() makes all four
    agree.

    numpy's own scalar types (np.int64, np.float64, ...) also report
    `.ndim == 0`, but they are already `is_scalar()`, so the first branch
    short-circuits and this changes nothing for them -- confirmed
    directly, not assumed.
    """
    if pd.api.types.is_scalar(value):
        return True
    return getattr(value, 'ndim', None) == 0


def is_missing(value):
    """True if value is a missing-value marker (NaN, pd.NA, pd.NaT, or
    any other value pd.isna() recognizes) that should normalize to None.
    Shared between this module's own _normalize_value() and
    table_adapters.py's normalize_for_excel() -- found independently
    broken in both, from the same underlying cause: a bare
    `value != value` check, which correctly detects a plain NaN but
    genuinely raises TypeError for pd.NA specifically ("boolean value
    of NA is ambiguous"), confirmed directly, not assumed. Both callers
    wrapped that check in a broad except Exception: pass, which silently
    swallowed the TypeError and let a raw pd.NA fall through unconverted
    instead of becoming None -- a real, live gap, not just an inaccurate
    comment, confirmed directly against both functions in isolation
    before this fix (each returned pd.NA itself, untouched).

    pd.isna() isn't a fully safe drop-in on its own, either: for a
    multi-element list/array-like value it returns an array rather than
    a scalar, which itself raises inside a plain `if`/bool() for
    anything but a single-element array. Still safely handled here by
    the same broad except Exception both callers already had -- if
    pd.isna()'s result can't be evaluated as a single, unambiguous truth
    value, this treats the original value as not missing, the same
    pre-existing fallback behavior either caller already had for any
    value pd.isna() itself doesn't like. Verified directly against
    every case this needs to distinguish: pd.NA, np.nan, None, pd.NaT,
    ordinary scalars, and multi-/empty-element lists.

    That verification was itself incomplete, found by a further review:
    a ONE-element container (list, array, Index) makes pd.isna() return
    a one-element array, and unlike a multi-element array, bool() on a
    single-element array succeeds rather than raising -- so the
    except Exception: pass fallback above never triggered for exactly
    this shape, and is_missing([None]) genuinely, silently returned
    True, corrupting a real, non-missing container value (a list
    containing one None) into a bare None. pd.api.types.is_scalar()
    checks pd.isna()'s own result is genuinely a single boolean before
    trusting it at all -- a container is never itself "the missing
    marker" regardless of its own size, only ever something that might
    hold missing values inside it, a separate question this function
    was never meant to answer. Verified directly against every case
    this needs to distinguish, this time including the one-element
    shape that was missed before: pd.NA, np.nan, None, pd.NaT, ordinary
    scalars, and empty/single-/multi-element lists, arrays, and a
    pd.Index, each holding either a missing or a non-missing value."""
    try:
        result = pd.isna(value)
        # _is_scalar_like(), not is_scalar(): pd.isna() hands back a
        # zero-dim ARRAY for an object-dtype zero-dim input and a plain
        # numpy bool for a typed one, so is_scalar() alone made
        # np.array(np.nan) missing and np.array(pd.NaT) not -- see
        # _is_scalar_like()'s own docstring.
        return bool(result) if _is_scalar_like(result) else False
    except Exception:
        return False


def _normalize_value(value):
    if value is None:
        return None

    if is_missing(value):
        return None

    # Found by a further review: is_missing() itself was already
    # correct at this point, but the duck-typed conversions below --
    # completely separate logic, a different mechanism entirely -- were
    # still silently collapsing a one-element container down to its
    # sole element. numpy's own .item() is genuinely designed to do
    # exactly that for an array of size 1 (raising for anything larger,
    # which the except Exception: pass below was already catching) --
    # confirmed directly, not assumed: np.array([5]).item() succeeds
    # and returns the plain int 5, silently discarding the array itself.
    # pd.api.types.is_scalar() stops this before it starts: a genuine
    # numpy/pandas scalar (np.int64, pd.Timestamp, ...) still reaches
    # to_pydatetime()/.item() below and normalizes correctly, since
    # is_scalar() is True for those -- confirmed directly. An array,
    # Index, Series, or any other container now returns unchanged
    # instead, reaching the DB driver intact or failing honestly there
    # rather than silently changing shape here.
    if not _is_scalar_like(value):
        return value

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



def _scan_families(rows, col_name, *, sample_size):
    """The value families present in the scanned rows. Returned as a set
    rather than folded straight into a type so _infer_column_type() can
    tell 'this column is text' apart from 'this sample saw nothing at all'
    -- both of which resolve to Text, and only one of which is an
    observation. Distinguishing them without a second pass over the sample
    is the whole reason this is split out.
    """
    families = set()
    rows_to_scan = rows if sample_size is None else rows[:sample_size]

    for row in rows_to_scan:
        value = row.get(col_name)
        if value is None:
            continue
        families.add(_value_family(value))
        if len(families) > 2:
            # Three families can only ever resolve to Text; the exact set
            # stops mattering, so stop reading.
            return families

    return families


def _infer_from_scan(
    rows: list[dict[str, Any]],
    col_name: str,
    *,
    sample_size: int | None,
):
    """The raw family scan. Split out of _infer_column_type() (below) so
    that function can re-run it over the full rows list after its own
    sampled answer is found to be narrower than the real data -- see
    _infer_column_type()'s own docstring for why that verification pass
    exists at all."""
    return _resolve_families(_scan_families(rows, col_name, sample_size=sample_size))


def _resolve_families(families):
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


# The only two inferred types PostgreSQL will silently widen a later,
# unsampled value into, rather than rejecting it -- confirmed directly
# against a real PostgreSQL instance by the project owner, not assumed
# from the documentation:
#
#   create temp table t (v bigint);
#   insert into t values (3.5);      -- succeeds, stores 4 (assignment
#                                    -- cast rounds; NO error)
#   create temp table d (v date);
#   insert into d values (timestamp '2024-01-01 13:30');
#                                    -- succeeds, stores 2024-01-01
#                                    -- (the time is silently dropped)
#
# Every other narrowing this inference can produce fails loudly at insert
# time instead ('N/A' or True into bigint both error), so a sampled answer
# that turns out too narrow is self-announcing for those and needs no
# verification pass. These two do not announce themselves anywhere: the
# task reports success, the row count matches, and the corruption is only
# visible by comparing published values against the source.
#
# The paired Python type is the EXACT type a value must have to be
# consistent with the inferred column type -- `type(value) is int`, not
# isinstance(). Exactness matters in both directions here: bool is a
# subclass of int (so isinstance would wave True through into a bigint
# column, which PostgreSQL then rejects loudly anyway), and datetime is a
# subclass of date (so isinstance would wave a datetime through into a
# date column, which is precisely the silent truncation above).
_SILENTLY_WIDENABLE = (
    (sa.BigInteger, int),
    (sa.Date, date),
)


def _silently_widenable_exact_type(inferred):
    for sa_type, exact_type in _SILENTLY_WIDENABLE:
        if type(inferred) is sa_type:
            return exact_type
    return None


def _infer_column_type(
    rows: list[dict[str, Any]],
    col_name: str,
    *,
    sample_size: int | None = 5000,
):
    """Column type inferred from the first `sample_size` rows, then
    verified against the remainder when -- and only when -- a later value
    could silently widen the sampled answer.

    The sample alone was genuinely wrong, not merely imprecise: confirmed
    directly that a column whose first 5000 rows are ints and whose row
    5001 is 3.5 infers BigInteger, and that PostgreSQL then rounds 3.5 to
    4 on insert without error (see _SILENTLY_WIDENABLE above for the exact
    reproduction). Same shape for a date column meeting a later datetime,
    which loses the time. Both are data-dependent, appear only once a
    table grows past the sample, and are invisible in the log -- the task
    reports success either way.

    Scanning every row instead was measured before being rejected: on a
    1,000,000-row, 20-column table it cost ~7.2s against the sample's
    ~35ms, a ~200x penalty paid on every publish to correct something the
    sample already gets right in the overwhelming majority of cases. The
    verification pass below costs ~1.0s on that same table (~30x, and only
    ~3-5% on top of that table's own ~18s payload construction, before the
    network insert that dwarfs both), because it runs only for the two
    narrowable types and uses a bare exact-type check per row rather than
    the full _value_family() match statement. Confirmed directly that it
    returns the identical answer to a full scan for every case tried: a
    late float, Decimal, datetime, text, and bool, plus clean int and
    clean date columns with no late value at all.

    A third approach -- one pass over rows accumulating families for every
    column at once, retiring each column as soon as its answer can no
    longer change -- was also built and measured: ~2.5s on the same table,
    still worse than verification and a substantially larger change to a
    function every publish depends on. Not pursued.

    sample_size=None still means "scan every row", exactly as before; the
    verification pass is skipped in that case because a full scan has
    nothing left to verify against.
    """
    families = _scan_families(rows, col_name, sample_size=sample_size)

    # A sample that saw no non-null value at all tells us nothing, and
    # _infer_from_scan()'s Text fallback for that case is a guess, not an
    # observation. Confirmed directly: 5000 nulls followed by an int
    # inferred Text where a full scan gives BigInteger, and likewise for a
    # late float, date or datetime. Sparse business columns routinely have
    # long leading null runs after sorting or monthly expansion, so this is
    # a realistic shape rather than a contrived one. The rows are already
    # materialized, so re-scanning costs CPU and no additional source I/O.
    if sample_size is not None and len(rows) > sample_size and not families:
        return _infer_from_scan(rows, col_name, sample_size=None)

    inferred = _resolve_families(families)

    if sample_size is None or len(rows) <= sample_size:
        return inferred

    exact_type = _silently_widenable_exact_type(inferred)
    if exact_type is None:
        return inferred

    for row in islice(rows, sample_size, None):
        # islice, not rows[sample_size:] -- the slice would copy the
        # entire unsampled remainder into a second list (995,000 dicts on
        # the table measured above) purely to iterate it once.
        value = row.get(col_name)
        if value is None or type(value) is exact_type:
            continue
        # Something outside the sample genuinely doesn't fit. Re-infer
        # across every row rather than trying to widen the sampled answer
        # in place: the correct result depends on the full set of families
        # present (a late float widens int to Numeric, but a late string
        # collapses the whole column to Text), and _infer_from_scan()
        # already encodes that resolution exactly once.
        return _infer_from_scan(rows, col_name, sample_size=None)

    return inferred



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
