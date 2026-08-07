"""What a COPY spool file is: identity, location, header and framing.

Split out of copy.py in 0.7.4. This module defines the on-disk grammar and
nothing that performs I/O policy: the five ownership ingredients and the
token derived from them, the filename form, the versioned header, where
spools live by default, and the type-neutral binary framing that lets an
inferred run replay its rows once the schema is known.

ADR 0011 §Spool ownership and cleanup requires *both* an exact filename
grammar and an internal header before predecessor cleanup deletes anything.
Both live here; the deleting happens in spool_io.py.

The neutral format is private, versioned and length-framed. It is not
pickle, not CSV, not PostgreSQL binary COPY, and not a user-facing export.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import struct
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO

from task_core.db.policies import CopyLoadPolicy
from task_core.db.values import DbPublishError

# --- Exceptions --------------------------------------------------------

class SpoolFormatError(DbPublishError):
    """Raised when a spool file cannot be positively identified as ours.

    Inherits DbPublishError so callers that already wrap the publication
    exception hierarchy do not need a new except clause. Predecessor
    cleanup treats any file that raises SpoolFormatError as "not ours,
    leave it alone", per ADR 0011 §Spool ownership: "unknown or
    malformed files preserved rather than guessed to be ours".
    """


# --- Header format constants ------------------------------------------

# 6-byte magic. Fixed length keeps the framing trivial and makes a
# grep-friendly signature ("TCCPY") that never collides with UTF-8 text
# or the PostgreSQL COPY binary magic ("PGCOPY\n\377\r\n\0").
MAGIC = b'TCCPY\x00'

# uint16, big-endian. Bumping this is the only way to change the header
# payload shape. Old readers refuse (SpoolFormatError) rather than
# guess -- see read_spool_header.
FORMAT_VERSION = 2

# Supported spool stages, per ADR 0011 §Schema resolution and final COPY
# spool. Inferred mode uses both; declared mode writes only the final
# copytext spool because its target schema is known before traversal.
#   'neutral'  - inferred-mode type-neutral normalization output
#   'copytext' - final target-aware PostgreSQL COPY text spool
SPOOL_STAGES = ('neutral', 'copytext')

PROTECTION_NONE = 'none'
PROTECTION_AES256_GCM = 'aes-256-gcm'
SPOOL_PROTECTIONS = (PROTECTION_NONE, PROTECTION_AES256_GCM)

_AES_KEY_BYTES = 32
_GCM_NONCE_BYTES = 12
_GCM_FOOTER_MAGIC = b'TCGM'
_GCM_FOOTER = struct.Struct('>4s16s')


# --- Filename grammar --------------------------------------------------

# `task_core-copy-<40 lowercase hex>-(neutral|copytext).spool`.
# Anchored both ends: partial matches are not filenames, they are
# suspicious substrings. 40 hex = SHA-1 hexdigest length. SHA-1 chosen
# over SHA-256 for filename readability; the hash is an ownership token,
# not a security primitive (ownership is enforced by the internal header
# match, which the filename only preselects).
SPOOL_FILENAME_RE = re.compile(
    r'^task_core-copy-([0-9a-f]{40})-(neutral|copytext)\.spool$'
)

# What resolve_spool_directory falls back to when the policy leaves it
# unset. Kept as a module constant so the tests can reference the same
# name and predecessor cleanup can list the right directory.
DEFAULT_SPOOL_SUBDIR = 'task_core-copy-spool'


# --- Ownership token --------------------------------------------------

# Unit separator. Chosen deliberately because it cannot appear in a
# well-formed identifier, filename or ISO-8601 timestamp: two ingredients
# that both contain a `\x1f` cannot be forged into the same token by
# swapping bytes across their boundary. A single-character text
# delimiter like `|` would allow exactly that.
_TOKEN_DELIMITER = '\x1f'


def compose_ownership_token(
    *,
    task: str,
    target_schema: str,
    target_table: str,
    run_start_utc: datetime,
    pid: int,
) -> str:
    """Deterministic 40-char lowercase hex digest of the five ownership
    ingredients. Same ingredients -> same token; any difference (case,
    whitespace, timezone offset representation) -> different token.

    Callers must pass an aware datetime in UTC. A naive datetime would
    encode differently on different machines and defeat the digest's
    purpose as a stable ownership marker.
    """
    if not isinstance(task, str) or not task:
        raise DbPublishError(f'task must be a non-empty str, got {task!r}')
    if not isinstance(target_schema, str) or not target_schema:
        raise DbPublishError(
            f'target_schema must be a non-empty str, got {target_schema!r}'
        )
    if not isinstance(target_table, str) or not target_table:
        raise DbPublishError(
            f'target_table must be a non-empty str, got {target_table!r}'
        )
    if not isinstance(run_start_utc, datetime):
        raise DbPublishError(
            f'run_start_utc must be a datetime, got {type(run_start_utc).__name__}'
        )
    if run_start_utc.tzinfo is None or run_start_utc.utcoffset() != timezone.utc.utcoffset(None):
        # utcoffset() comparison catches non-UTC aware datetimes as well
        # as naive ones. An `Asia/Tokyo` timestamp would digest differently
        # from the same instant expressed as UTC; that ambiguity is what
        # this guard exists to prevent.
        raise DbPublishError(
            f'run_start_utc must be a UTC-aware datetime, got tzinfo={run_start_utc.tzinfo!r}'
        )
    # bool subclasses int; same trap as CopyLoadPolicy.buffer_bytes.
    if type(pid) is not int or pid < 1:
        raise DbPublishError(f'pid must be a positive integer, got {pid!r}')

    raw = _TOKEN_DELIMITER.join([
        task,
        target_schema,
        target_table,
        run_start_utc.isoformat(),
        str(pid),
    ]).encode('utf-8')
    return hashlib.sha1(raw).hexdigest()


# --- Filename compose / parse -----------------------------------------

def compose_spool_filename(*, token: str, stage: str) -> str:
    """Assemble `task_core-copy-<token>-<stage>.spool`.

    Rejects any token or stage that would produce a filename the
    round-trip parser would refuse. Callers should not be able to build
    an unparseable filename accidentally.
    """
    if not isinstance(token, str) or not re.fullmatch(r'[0-9a-f]{40}', token):
        raise DbPublishError(
            f'token must be a 40-char lowercase hex str, got {token!r}'
        )
    if stage not in SPOOL_STAGES:
        raise DbPublishError(
            f'stage must be one of {SPOOL_STAGES}, got {stage!r}'
        )
    return f'task_core-copy-{token}-{stage}.spool'


def parse_spool_filename(name: str) -> dict[str, str] | None:
    """Inverse of compose_spool_filename. Returns
    {'token': ..., 'stage': ...} on match or None on any deviation.

    None (not raise) because predecessor cleanup asks "is this ours?"
    of every file in the spool directory, and the negative answer is
    the common case.
    """
    if not isinstance(name, str):
        return None
    match = SPOOL_FILENAME_RE.match(name)
    if match is None:
        return None
    return {'token': match.group(1), 'stage': match.group(2)}


# --- Internal header write / read -------------------------------------

# Struct format for the fixed-width header prefix:
#   6s   MAGIC (6 bytes)
#   H    uint16 version (big-endian)
#   I    uint32 payload_len (big-endian)
# Big-endian ('>') so on-disk order is stable across machines. Total
# fixed prefix = 12 bytes; JSON payload follows immediately.
_HEADER_PREFIX = struct.Struct('>6sHI')

# Ceiling on the JSON payload size. The header is metadata for a handful
# of small strings and one int; a runaway allocation from a corrupted
# length field is what this cap defends against, not a real growth
# curve. Well over any legitimate header we would ever write.
_MAX_HEADER_PAYLOAD_BYTES = 64 * 1024


def _encode_spool_header(
    *,
    task: str,
    target_schema: str,
    target_table: str,
    run_start_utc: datetime,
    pid: int,
    token: str,
    stage: str,
    protection: str,
    nonce: bytes | None,
) -> bytes:
    if stage not in SPOOL_STAGES:
        raise DbPublishError(
            f'stage must be one of {SPOOL_STAGES}, got {stage!r}'
        )
    if protection not in SPOOL_PROTECTIONS:
        raise DbPublishError(
            f'protection must be one of {SPOOL_PROTECTIONS}, got {protection!r}'
        )
    if protection == PROTECTION_AES256_GCM:
        if not isinstance(nonce, bytes) or len(nonce) != _GCM_NONCE_BYTES:
            raise DbPublishError(
                f'{PROTECTION_AES256_GCM} requires a {_GCM_NONCE_BYTES}-byte nonce'
            )
        nonce_hex = nonce.hex()
    else:
        if nonce is not None:
            raise DbPublishError('plaintext spool protection must not carry a nonce')
        nonce_hex = None

    expected_token = compose_ownership_token(
        task=task,
        target_schema=target_schema,
        target_table=target_table,
        run_start_utc=run_start_utc,
        pid=pid,
    )
    if token != expected_token:
        raise DbPublishError(
            f'token {token!r} does not match ingredients (expected {expected_token!r})'
        )

    payload = {
        'task': task,
        'target_schema': target_schema,
        'target_table': target_table,
        'run_start_utc': run_start_utc.isoformat(),
        'pid': pid,
        'token': token,
        'stage': stage,
        'protection': protection,
        'nonce': nonce_hex,
    }
    payload_bytes = json.dumps(
        payload, sort_keys=True, ensure_ascii=True, separators=(',', ':'),
    ).encode('ascii')
    if len(payload_bytes) > _MAX_HEADER_PAYLOAD_BYTES:
        raise DbPublishError(
            f'header payload too large: {len(payload_bytes)} bytes '
            f'(limit {_MAX_HEADER_PAYLOAD_BYTES})'
        )
    return _HEADER_PREFIX.pack(MAGIC, FORMAT_VERSION, len(payload_bytes)) + payload_bytes


def write_spool_header(
    fp: BinaryIO,
    *,
    task: str,
    target_schema: str,
    target_table: str,
    run_start_utc: datetime,
    pid: int,
    token: str,
    stage: str,
    protection: str = PROTECTION_NONE,
    nonce: bytes | None = None,
) -> bytes:
    """Write and return the exact versioned ownership header bytes.

    The returned bytes are also authenticated as AEAD associated data for
    encrypted spools. Business data never appears in this plaintext header.
    """
    header_bytes = _encode_spool_header(
        task=task,
        target_schema=target_schema,
        target_table=target_table,
        run_start_utc=run_start_utc,
        pid=pid,
        token=token,
        stage=stage,
        protection=protection,
        nonce=nonce,
    )
    _write_all(fp, header_bytes)
    return header_bytes


def _read_spool_header_with_bytes(fp: BinaryIO) -> tuple[dict[str, Any], bytes]:
    prefix = fp.read(_HEADER_PREFIX.size)
    if len(prefix) != _HEADER_PREFIX.size:
        raise SpoolFormatError(
            f'short read on header prefix: got {len(prefix)} bytes, '
            f'expected {_HEADER_PREFIX.size}'
        )
    magic, version, payload_len = _HEADER_PREFIX.unpack(prefix)
    if magic != MAGIC:
        raise SpoolFormatError(f'wrong magic: {magic!r}')
    if version != FORMAT_VERSION:
        raise SpoolFormatError(
            f'unknown format version {version} (this build understands {FORMAT_VERSION})'
        )
    if payload_len > _MAX_HEADER_PAYLOAD_BYTES:
        raise SpoolFormatError(
            f'header payload length {payload_len} exceeds cap {_MAX_HEADER_PAYLOAD_BYTES}'
        )
    payload_bytes = fp.read(payload_len)
    if len(payload_bytes) != payload_len:
        raise SpoolFormatError(
            f'short read on header payload: got {len(payload_bytes)} bytes, '
            f'expected {payload_len}'
        )
    try:
        payload = json.loads(payload_bytes.decode('ascii'))
    except (ValueError, UnicodeDecodeError) as exc:
        raise SpoolFormatError(f'header payload not valid ASCII JSON: {exc}') from exc
    if not isinstance(payload, Mapping):
        raise SpoolFormatError(
            f'header payload must decode to a mapping, got {type(payload).__name__}'
        )
    required = (
        'task', 'target_schema', 'target_table', 'run_start_utc', 'pid',
        'token', 'stage', 'protection', 'nonce',
    )
    missing = [key for key in required if key not in payload]
    if missing:
        raise SpoolFormatError(f'header payload missing keys: {missing}')
    if payload['stage'] not in SPOOL_STAGES:
        raise SpoolFormatError(
            f'header payload stage {payload["stage"]!r} not in {SPOOL_STAGES}'
        )
    if payload['protection'] not in SPOOL_PROTECTIONS:
        raise SpoolFormatError(
            f'header payload protection {payload["protection"]!r} not in '
            f'{SPOOL_PROTECTIONS}'
        )
    if payload['protection'] == PROTECTION_AES256_GCM:
        nonce_hex = payload['nonce']
        if not isinstance(nonce_hex, str):
            raise SpoolFormatError('encrypted spool header nonce must be hex text')
        try:
            nonce = bytes.fromhex(nonce_hex)
        except ValueError as exc:
            raise SpoolFormatError('encrypted spool header nonce is not valid hex') from exc
        if len(nonce) != _GCM_NONCE_BYTES:
            raise SpoolFormatError(
                f'encrypted spool nonce has {len(nonce)} bytes, '
                f'expected {_GCM_NONCE_BYTES}'
            )
    elif payload['nonce'] is not None:
        raise SpoolFormatError('plaintext spool header must not carry a nonce')

    try:
        parsed_run_start = datetime.fromisoformat(payload['run_start_utc'])
        expected_token = compose_ownership_token(
            task=payload['task'],
            target_schema=payload['target_schema'],
            target_table=payload['target_table'],
            run_start_utc=parsed_run_start,
            pid=payload['pid'],
        )
    except (TypeError, ValueError, DbPublishError) as exc:
        raise SpoolFormatError(f'header ownership fields are invalid: {exc}') from exc
    if payload['token'] != expected_token:
        raise SpoolFormatError(
            f'header token {payload["token"]!r} does not match its ownership fields'
        )

    return dict(payload), prefix + payload_bytes


def read_spool_header(fp: BinaryIO) -> dict[str, Any]:
    """Read the public plaintext ownership header only.

    The encrypted body remains unreadable without the in-memory key, but a
    successor run can still identify and delete positively owned residue.
    """
    payload, _ = _read_spool_header_with_bytes(fp)
    return payload


# --- Directory resolution ---------------------------------------------

def resolve_spool_directory(policy: CopyLoadPolicy | None) -> Path:
    """Return the directory where spool files for this process land.

    Best-effort mkdir with mode=0o700. On POSIX the mode restricts the
    directory to the owning user; on Windows the mode is silently
    ignored by the platform, and access control for the spool directory
    is inherited from the parent (typically %TEMP%). ADR 0011 §Spool
    ownership calls this out: "Tasks handling sensitive data must place
    the spool directory on storage governed by the same access controls
    as their sources."

    `parents=True, exist_ok=True` so an already-created directory (from
    a prior run, or one the operator provisioned) is accepted. No mode
    is re-applied to an existing directory: changing permissions on
    something the operator set up would surprise them.
    """
    if policy is None:
        policy = CopyLoadPolicy()
    if not isinstance(policy, CopyLoadPolicy):
        raise DbPublishError(
            f'policy must be a CopyLoadPolicy or None, got {type(policy).__name__}'
        )
    if policy.spool_directory is not None:
        target = policy.spool_directory
    else:
        target = Path(tempfile.gettempdir()) / DEFAULT_SPOOL_SUBDIR
    # resolve(strict=False) folds any `..` components and yields an
    # absolute path without requiring the target to exist yet. Both the
    # policy-supplied and tempdir-derived branches go through this so
    # downstream containment checks start
    # from the same normalized form.
    target = target.resolve(strict=False)
    target.mkdir(mode=0o700, parents=True, exist_ok=True)
    return target


# --- Type-neutral spool format ----------------------------------------

# Value tags. Single byte, deliberately in a range disjoint from the row
# markers (0xFE, 0xFF) so a corrupted body byte cannot masquerade as a
# marker at the wrong position -- read_neutral_row can distinguish "this
# should be a tag" from "this should be a marker" by positional context
# alone. Reserved space above 0x0A is left for future scalar families
# (interval, uuid, jsonb-as-text, etc.) without needing a format-version
# bump if additions are strictly appended.
_TAG_NULL          = 0x00
_TAG_BOOL_FALSE    = 0x01
_TAG_BOOL_TRUE     = 0x02
_TAG_INT           = 0x03
_TAG_FLOAT         = 0x04
_TAG_DECIMAL       = 0x05
_TAG_STR           = 0x06
_TAG_BYTES         = 0x07
_TAG_DATE          = 0x08
_TAG_DATETIME_NAIVE = 0x09
_TAG_DATETIME_AWARE = 0x0A

_VALID_TAGS = frozenset(range(0x00, 0x0B))

# Row-frame markers. Distinct from every tag; positional context (the
# reader knows it is looking for a marker, not a tag) does the actual
# distinguishing, but keeping the numeric ranges disjoint means a
# single-byte flip in one place shows up as a rejected tag in the other.
_ROW_START = 0xFE
_TERMINATOR = 0xFF

# Struct layouts. All big-endian for on-disk stability.
_PREAMBLE_COUNT     = struct.Struct('>I')          # column count
_COL_NAME_LEN       = struct.Struct('>I')          # each column name len
_INT_LEN            = struct.Struct('>B')          # length prefix for int
_FLOAT_STRUCT       = struct.Struct('>d')          # IEEE-754 double
_LEN32              = struct.Struct('>I')          # length prefix (decimal/str/bytes)
_DATE_STRUCT        = struct.Struct('>HBB')        # year, month, day
_DATETIME_STRUCT    = struct.Struct('>HBBBBBI')    # y, m, d, h, mi, s, us
_TZ_OFFSET_STRUCT   = struct.Struct('>i')          # signed int32 seconds

# Ceiling on any single length-prefixed field to defend against a
# corrupted length causing a runaway allocation. 512 MiB is well over
# any legitimate cell value we would ever transport.
_MAX_FIELD_BYTES = 512 * 1024 * 1024


def _read_exact(fp: BinaryIO, n: int) -> bytes:
    data = fp.read(n)
    if len(data) != n:
        raise SpoolFormatError(
            f'short read on neutral spool: got {len(data)} bytes, expected {n}'
        )
    return data


def _min_signed_bytes(v: int) -> int:
    # Minimum length in bytes to encode `v` as two's-complement big-endian
    # signed. 0 -> 1, ±1 -> 1, 127 -> 1, 128 -> 2, -128 -> 1, -129 -> 2.
    # Derived closed-form rather than a try/except loop so absurdly large
    # ints (Python has no bound) do not force many failed to_bytes calls.
    if v >= 0:
        return (v.bit_length() // 8) + 1
    return ((~v).bit_length() // 8) + 1


def _write_value(buf: BinaryIO, v: Any) -> None:
    """Serialize a single scalar to `buf` as (tag + payload).

    Raises DbPublishError -- not SpoolFormatError -- because an
    unsupported type at write time is a caller-side type error, not
    a corrupted file. Distinguishing the two is what lets the reader
    treat SpoolFormatError as "not ours, leave alone" further downstream.
    """
    if v is None:
        buf.write(bytes([_TAG_NULL]))
        return
    # bool must precede int: bool is a subclass of int, so isinstance(v, int)
    # would match True/False and encode them as 0x03/0x01 rather than
    # 0x01/0x02. The tag distinction matters: pass 2 needs to route bool
    # to Boolean, not to Integer.
    if v is True:
        buf.write(bytes([_TAG_BOOL_TRUE]))
        return
    if v is False:
        buf.write(bytes([_TAG_BOOL_FALSE]))
        return
    if type(v) is int:
        length = _min_signed_bytes(v)
        if length > 255:
            raise DbPublishError(
                f'int too wide for neutral spool: needs {length} bytes '
                f'(limit 255)'
            )
        buf.write(bytes([_TAG_INT]))
        buf.write(_INT_LEN.pack(length))
        buf.write(v.to_bytes(length, 'big', signed=True))
        return
    if type(v) is float:
        buf.write(bytes([_TAG_FLOAT]))
        buf.write(_FLOAT_STRUCT.pack(v))
        return
    if isinstance(v, Decimal):
        # str(Decimal) round-trips exactly through Decimal(s) for finite
        # values *and* preserves the special-value spellings ('NaN',
        # 'Infinity', '-Infinity'). Using str is what keeps the format
        # neutral: pass 2 decides whether to serialize as PG numeric or
        # PG text based on the resolved target type.
        payload = str(v).encode('utf-8')
        if len(payload) > _MAX_FIELD_BYTES:
            raise DbPublishError(f'decimal repr too large: {len(payload)} bytes')
        buf.write(bytes([_TAG_DECIMAL]))
        buf.write(_LEN32.pack(len(payload)))
        buf.write(payload)
        return
    if type(v) is str:
        payload = v.encode('utf-8')
        if len(payload) > _MAX_FIELD_BYTES:
            raise DbPublishError(f'str too large: {len(payload)} bytes')
        buf.write(bytes([_TAG_STR]))
        buf.write(_LEN32.pack(len(payload)))
        buf.write(payload)
        return
    if isinstance(v, (bytes, bytearray, memoryview)):
        payload = bytes(v)
        if len(payload) > _MAX_FIELD_BYTES:
            raise DbPublishError(f'bytes too large: {len(payload)} bytes')
        buf.write(bytes([_TAG_BYTES]))
        buf.write(_LEN32.pack(len(payload)))
        buf.write(payload)
        return
    # datetime must precede date: datetime is a subclass of date, so
    # isinstance(v, date) matches datetime objects too.
    if isinstance(v, datetime):
        if v.tzinfo is None:
            buf.write(bytes([_TAG_DATETIME_NAIVE]))
            buf.write(_DATETIME_STRUCT.pack(
                v.year, v.month, v.day, v.hour, v.minute, v.second, v.microsecond,
            ))
        else:
            off = v.utcoffset()
            total = off.total_seconds()
            secs = int(total)
            if total != secs:
                raise DbPublishError(
                    f'timezone offset with subsecond component not supported: {off!r}'
                )
            if secs < -86400 or secs > 86400:
                # Python's timezone requires the offset in (-1 day, 1 day);
                # this is defensive against a hypothetical custom tzinfo.
                raise DbPublishError(
                    f'timezone offset out of range: {secs} seconds'
                )
            buf.write(bytes([_TAG_DATETIME_AWARE]))
            buf.write(_DATETIME_STRUCT.pack(
                v.year, v.month, v.day, v.hour, v.minute, v.second, v.microsecond,
            ))
            buf.write(_TZ_OFFSET_STRUCT.pack(secs))
        return
    if isinstance(v, date):
        buf.write(bytes([_TAG_DATE]))
        buf.write(_DATE_STRUCT.pack(v.year, v.month, v.day))
        return
    raise DbPublishError(
        f'unsupported value type in neutral spool: {type(v).__name__}'
    )


def _read_value(fp: BinaryIO) -> Any:
    tag_byte = _read_exact(fp, 1)
    tag = tag_byte[0]
    if tag not in _VALID_TAGS:
        raise SpoolFormatError(f'unknown value tag: 0x{tag:02x}')
    if tag == _TAG_NULL:
        return None
    if tag == _TAG_BOOL_FALSE:
        return False
    if tag == _TAG_BOOL_TRUE:
        return True
    if tag == _TAG_INT:
        length = _INT_LEN.unpack(_read_exact(fp, _INT_LEN.size))[0]
        if length == 0:
            raise SpoolFormatError('int payload length 0 is invalid')
        return int.from_bytes(_read_exact(fp, length), 'big', signed=True)
    if tag == _TAG_FLOAT:
        return _FLOAT_STRUCT.unpack(_read_exact(fp, _FLOAT_STRUCT.size))[0]
    if tag == _TAG_DECIMAL:
        length = _LEN32.unpack(_read_exact(fp, _LEN32.size))[0]
        if length > _MAX_FIELD_BYTES:
            raise SpoolFormatError(f'decimal length {length} exceeds cap')
        try:
            return Decimal(_read_exact(fp, length).decode('utf-8'))
        except (UnicodeDecodeError, ValueError) as exc:
            raise SpoolFormatError(f'invalid decimal payload: {exc}') from exc
    if tag == _TAG_STR:
        length = _LEN32.unpack(_read_exact(fp, _LEN32.size))[0]
        if length > _MAX_FIELD_BYTES:
            raise SpoolFormatError(f'str length {length} exceeds cap')
        try:
            return _read_exact(fp, length).decode('utf-8')
        except UnicodeDecodeError as exc:
            raise SpoolFormatError(f'invalid utf-8 in str payload: {exc}') from exc
    if tag == _TAG_BYTES:
        length = _LEN32.unpack(_read_exact(fp, _LEN32.size))[0]
        if length > _MAX_FIELD_BYTES:
            raise SpoolFormatError(f'bytes length {length} exceeds cap')
        return _read_exact(fp, length)
    if tag == _TAG_DATE:
        y, m, d = _DATE_STRUCT.unpack(_read_exact(fp, _DATE_STRUCT.size))
        try:
            return date(y, m, d)
        except ValueError as exc:
            raise SpoolFormatError(f'invalid date components: {exc}') from exc
    if tag == _TAG_DATETIME_NAIVE:
        parts = _DATETIME_STRUCT.unpack(_read_exact(fp, _DATETIME_STRUCT.size))
        try:
            return datetime(*parts)
        except ValueError as exc:
            raise SpoolFormatError(f'invalid datetime components: {exc}') from exc
    if tag == _TAG_DATETIME_AWARE:
        parts = _DATETIME_STRUCT.unpack(_read_exact(fp, _DATETIME_STRUCT.size))
        secs = _TZ_OFFSET_STRUCT.unpack(_read_exact(fp, _TZ_OFFSET_STRUCT.size))[0]
        try:
            return datetime(*parts, tzinfo=timezone(timedelta(seconds=secs)))
        except ValueError as exc:
            raise SpoolFormatError(f'invalid aware datetime: {exc}') from exc
    # Unreachable: every member of _VALID_TAGS is dispatched above. If a
    # new tag is added to _VALID_TAGS without a branch here, this raises
    # -- which is what we want.
    raise SpoolFormatError(f'tag 0x{tag:02x} accepted but not handled')


def write_neutral_preamble(fp: BinaryIO, *, columns: Sequence[str]) -> None:
    """Write the type-neutral spool preamble (column count + names).

    Called once, before any rows. Column names are UTF-8 length-prefixed
    even though PORTABLE_IDENTIFIER_RE restricts them to ASCII: the
    on-disk format has no reason to bake that restriction in, and the
    reader can validate on its own if the caller cares.
    """
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
        raise DbPublishError(
            f'columns must be a Sequence of str, got {type(columns).__name__}'
        )
    fp.write(_PREAMBLE_COUNT.pack(len(columns)))
    for name in columns:
        if not isinstance(name, str):
            raise DbPublishError(
                f'column name must be str, got {type(name).__name__}'
            )
        payload = name.encode('utf-8')
        if len(payload) > _MAX_FIELD_BYTES:
            raise DbPublishError(f'column name too large: {len(payload)} bytes')
        fp.write(_COL_NAME_LEN.pack(len(payload)))
        fp.write(payload)


def read_neutral_preamble(fp: BinaryIO) -> tuple[str, ...]:
    """Inverse of write_neutral_preamble. Returns column names as a
    tuple so the caller cannot mutate them accidentally between rows.
    """
    count = _PREAMBLE_COUNT.unpack(_read_exact(fp, _PREAMBLE_COUNT.size))[0]
    names = []
    for _ in range(count):
        length = _COL_NAME_LEN.unpack(_read_exact(fp, _COL_NAME_LEN.size))[0]
        if length > _MAX_FIELD_BYTES:
            raise SpoolFormatError(
                f'column name length {length} exceeds cap'
            )
        try:
            names.append(_read_exact(fp, length).decode('utf-8'))
        except UnicodeDecodeError as exc:
            raise SpoolFormatError(f'invalid utf-8 in column name: {exc}') from exc
    return tuple(names)


def write_neutral_row(
    fp: BinaryIO,
    row: Sequence[Any],
    *,
    expected_width: int,
) -> None:
    """Serialize one row: marker byte + N tag/value pairs.

    Buffers the entire row in an in-memory `BytesIO` and writes with a
    single `fp.write` call. That does not make the underlying OS write
    atomic -- no such guarantee exists for regular files on any platform
    -- but it does mean that at the Python layer no two logical rows
    from the same writer can interleave partially. A mid-row exception
    inside `_write_value` never reaches `fp` at all, which is the
    property the reader relies on.

    `expected_width` is required and enforced before any bytes are
    written to `fp`: a mismatch is a caller-side bug, not a corrupted
    spool, and catching it at write time prevents a poison row from
    reaching disk.
    """
    if type(expected_width) is not int or expected_width < 0:
        raise DbPublishError(
            f'expected_width must be a non-negative int, got {expected_width!r}'
        )
    if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
        raise DbPublishError(
            f'row must be a Sequence, got {type(row).__name__}'
        )
    if len(row) != expected_width:
        raise DbPublishError(
            f'row width {len(row)} does not match expected {expected_width}'
        )
    buffer = io.BytesIO()
    buffer.write(bytes([_ROW_START]))
    for value in row:
        _write_value(buffer, value)
    fp.write(buffer.getvalue())


def write_neutral_terminator(fp: BinaryIO) -> None:
    """Write the single-byte terminator that ends the row stream.

    A separate call rather than an auto-close on the last row: the writer
    does not know which row is last, and requiring an explicit terminator
    makes truncated spools (writer crashed mid-stream) detectable by the
    reader as "no terminator found".
    """
    fp.write(bytes([_TERMINATOR]))


def read_neutral_row(
    fp: BinaryIO,
    column_count: int,
) -> tuple[Any, ...] | None:
    """Read one row and return its values as a tuple, or None at the
    terminator. Raises SpoolFormatError on any framing violation.

    `column_count` comes from the preamble; the reader trusts it to size
    each row rather than encoding a per-row count. Every legitimate row
    has exactly `column_count` values because the writer enforces
    `expected_width` at write time.
    """
    if type(column_count) is not int or column_count < 0:
        raise DbPublishError(
            f'column_count must be a non-negative int, got {column_count!r}'
        )
    marker = _read_exact(fp, 1)[0]
    if marker == _TERMINATOR:
        return None
    if marker != _ROW_START:
        raise SpoolFormatError(
            f'expected row-start (0x{_ROW_START:02x}) or terminator '
            f'(0x{_TERMINATOR:02x}), got 0x{marker:02x}'
        )
    values = [_read_value(fp) for _ in range(column_count)]
    return tuple(values)


def _write_all(fp: BinaryIO, data: bytes) -> None:
    """Write every byte or fail; do not treat a short raw write as success."""
    view = memoryview(data)
    while view:
        written = fp.write(view)
        if written is None or written <= 0:
            raise OSError('short write while writing COPY spool')
        view = view[written:]
