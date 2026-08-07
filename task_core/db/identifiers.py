"""PostgreSQL identifier rules, staging names, and ownership comments.

Split out of publish.py in 0.7.4. Everything here answers "what is this
object called, and how do we recognise one we created" -- byte limits and
truncation, portable-identifier validation, the deterministic staging name,
the advisory-lock key, and the COMMENT ON TABLE ownership metadata that
predecessor cleanup parses.

None of it decides whether to publish. `server_identifier_limit()` takes a
connection but only reads `max_identifier_length`; nothing here issues DDL,
holds a transaction, or imports the publisher.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import uuid4

import sqlalchemy as sa

from task_core.db.values import DbPublishError, DbPublishInvariantError
from task_core.types import PORTABLE_IDENTIFIER_RE, PUBLISHED_COLUMN_RE

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
            f'({PORTABLE_IDENTIFIER_RE.pattern}): {name!r}. Rename it.'
        )
    return name


def validate_published_column_name(name, *, kind='column name', context=''):
    """Columns may carry dots; schemas, tables and relations may not.

    Separate from validate_portable_identifier() rather than a widened
    version of it, because the two make different promises. A portable
    identifier never needs quoting downstream. A dotted column always does:
    `select lev.1` parses as a qualified reference, not as the column. See
    decisions/0014.

    fullmatch() for the same reason as above -- `$` also matches before a
    trailing newline, so match() would accept 'lev.1\\n'.
    """
    if not PUBLISHED_COLUMN_RE.fullmatch(name):
        raise DbPublishError(
            f'{context}{kind} is not a valid published column name '
            f'({PUBLISHED_COLUMN_RE.pattern}): {name!r}. Lower case, and a '
            f'dot may separate parts but may not lead, trail or repeat. '
            f'Rename it.'
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

# Advisory lock namespace. The two-int form gives a 32-bit namespace in the
# high half; advisory locks are database-wide and shared with anything else
# using them, so a bare hashtext(task_name) could collide with an unrelated
# application's lock and present as this task mysteriously refusing to run.
_ADVISORY_LOCK_NAMESPACE = 0x7A5C  # 'task_core', arbitrary but fixed

# Ownership metadata attached to every staging table via COMMENT ON TABLE.
# Compact JSON so cleanup can parse it, versioned so a future change can be
# recognised rather than guessed at.
STAGING_COMMENT_VERSION = 1
_COMMENT_MARKER = 'task_core'


def advisory_lock_key(task_name):
    """(namespace, key) for pg_try_advisory_lock's two-int form.

    32 bits of task-name hash. A collision means two DIFFERENT tasks
    serialize against each other -- safe, since neither can corrupt the
    other's data, but confusing to diagnose: one task appears to skip
    because 'another run is in progress' when the culprit is a different
    task entirely. Birthday-bounded far beyond any plausible number of
    tasks, so not worth widening; worth knowing before someone spends an
    afternoon on it.
    """
    digest = hashlib.blake2b(task_name.encode('utf-8'), digest_size=4).digest()
    # Signed 32-bit, which is what PostgreSQL's int4 accepts.
    key = int.from_bytes(digest, 'big', signed=True)
    return _ADVISORY_LOCK_NAMESPACE, key


def build_staging_comment(*, task_name, run_token, schema, table_name):
    return json.dumps(
        {
            'marker': _COMMENT_MARKER,
            'v': STAGING_COMMENT_VERSION,
            'task': task_name,
            'run': run_token,
            'target_schema': schema,
            'target_table': table_name,
            'created_at': datetime.now(timezone.utc).isoformat(),
        },
        separators=(',', ':'),
        ensure_ascii=False,
    )


def build_published_comment(*, task_name, run_token, rows):
    """Replaces the staging comment on the live table after the swap.

    Two purposes. It stops a published table from carrying staging
    ownership metadata that cleanup would later read -- ALTER TABLE ...
    RENAME preserves comments, so without this every published table looks
    like an abandoned staging artifact. And it is genuinely useful
    provenance: 'which run produced this data' answered from the catalog is
    the question actually asked when a number looks wrong.
    """
    return json.dumps(
        {
            'marker': _COMMENT_MARKER,
            'v': STAGING_COMMENT_VERSION,
            'published_by': task_name,
            'run': run_token,
            'rows': rows,
            'published_at': datetime.now(timezone.utc).isoformat(),
        },
        separators=(',', ':'),
        ensure_ascii=False,
    )


# \Z, not $. Python's `$` also matches immediately before a trailing
# newline, and a quoted PostgreSQL identifier may contain one -- so
# 'x__stg_deadbeef_deadbeef\n' satisfied a rule advertised as exact.
_STAGING_NAME_SUFFIX_RE = re.compile(
    rf'__{STAGING_NAME_KIND}_([0-9a-f]{{{_STAGING_TOKEN_HEX}}})_([0-9a-f]{{{_RUN_TOKEN_HEX}}})\Z'
)


def owned_staging_tokens(relname):
    """(target_token, run_token) if this name has the exact staging shape,
    else None.

    The catalog scan uses a broad LIKE because SQL cannot express the
    token shapes; this is what turns that into the strict rule. Without
    it, any table whose name merely contained the infix could be dropped
    on the strength of a syntactically valid comment -- confirmed directly
    with `not_really__stg_whatever`.

    The readable prefix is deliberately NOT recomputed. It may have been
    truncated under a different configured identifier limit, so a prefix
    comparison would refuse to clean up artifacts this project genuinely
    created.
    """
    match = _STAGING_NAME_SUFFIX_RE.search(relname)
    if match is None:
        return None
    return match.group(1), match.group(2)


def parse_staging_comment(comment):
    """Ownership metadata, or None when this is not ours.

    Defensive by design: an unparseable or unrecognised comment means
    ownership is UNKNOWN, and the cleanup rule is to drop only what can be
    positively identified. A parse failure must never fall through to a
    drop -- that is the failure mode that turns cleanup from hygiene into
    an outage.
    """
    if not comment:
        return None
    try:
        parsed = json.loads(comment)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    # `type(...) is int`, not equality: Python considers True == 1 and
    # 1.0 == 1, so a comment carrying "v": true or "v": 1.0 passed a
    # straight comparison -- confirmed directly. A version field that is
    # not an integer is not a version this code wrote.
    if parsed.get('marker') != _COMMENT_MARKER:
        return None
    if type(parsed.get('v')) is not int or parsed['v'] != STAGING_COMMENT_VERSION:
        return None

    # EVERY documented field, with its type checked. Requiring only the
    # marker, the version and the presence of task/run meant a comment
    # missing target_schema, target_table and created_at still authorized
    # a drop -- metadata that does not satisfy the documented format was
    # being treated as positive identification, which is precisely what
    # 'unknown ownership is never dropped' is supposed to prevent.
    #
    # Extra fields are tolerated on purpose, so a later version can add
    # one without older code refusing to recognise its own artifacts.
    required_strings = ('task', 'run', 'target_table', 'created_at')
    for field in required_strings:
        value = parsed.get(field)
        if not isinstance(value, str) or not value:
            return None

    # target_schema may legitimately be None (an unqualified target), but
    # the key must be present and, when set, a non-empty string.
    if 'target_schema' not in parsed:
        return None
    schema = parsed['target_schema']
    if schema is not None and (not isinstance(schema, str) or not schema):
        return None

    return parsed


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
