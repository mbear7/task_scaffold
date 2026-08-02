# -*- coding: utf-8 -*-
"""COPY loader subsystem: local spool preparation for db_loader='copy'.

Layering (ADR 0011 §Implementation sequence):

    db_publish  ->  db_copy  ->  db_values

`db_copy` is one level *below* `db_publish`. It knows nothing about
publication, staging tables, advisory locks, transactions, or the
`DbPublisher` class. It cannot import `db_publish`, cannot open or manage
a database transaction (no `.begin(`, `.commit(`, `.rollback(`), and
cannot create engines or connections. Everything here is pure
file/bytes/schema work.

The module is deliberately name-clean of those forbidden identifiers even
in comments, so the Phase 5.i AST-based architecture tests can enforce
the boundary by simple grep-of-Import + grep-of-Attribute.

Public shape as of 0.6.4 (still test-only until Phase 6 lifts the public
rejection of `db_loader='copy'`):

- `CopyLoadPolicy`               - config dataclass, moved here from
                                    db_publish in 0.6.4 so its home
                                    matches its layer
- `SpoolFormatError`             - malformed or unowned spool
- `MAGIC`, `FORMAT_VERSION`      - internal header constants
- `SPOOL_STAGES`                 - the two spool stages a run produces
- `SPOOL_FILENAME_RE`            - exact portable grammar
- `compose_ownership_token`      - digest of the five ownership ingredients
- `compose_spool_filename`       - `task_core-copy-<token>-<stage>.spool`
- `parse_spool_filename`         - inverse; `None` on any deviation
- `write_spool_header` /
  `read_spool_header`            - the versioned internal header
- `resolve_spool_directory`      - best-effort creation with 0o700

ADR 0011 §Spool ownership and cleanup requires *both* an exact filename
grammar and an internal header for positive ownership before predecessor
cleanup deletes anything. This module provides both primitives; Phase 5.f
uses them under the task advisory lock.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
import re
import struct
import tempfile
from pathlib import Path
from typing import Any, BinaryIO

from task_core.db_values import (
    DbPublishError,
    ResolvedColumn,
    _declared_type_family,
    _InferenceStreamState,
    _validate_declared_value,
)
from task_core.types import find_duplicates


# --- Config ------------------------------------------------------------

@dataclass(frozen=True)
class CopyLoadPolicy:
    """Where and how the COPY loader spools rows before database transport.

    Two settings, both with defaults that keep every existing insert-path
    caller unchanged: `db_loader='copy'` remains publicly rejected in
    0.6.4, so this object has no observable effect yet. Phase 6 lifts
    the rejection and turns these defaults into the ones tasks inherit
    when they set `db_loader='copy'` without overriding anything.

    `spool_directory=None` means "resolve at consumption time via the
    platform tempdir". Nothing here creates the directory or touches
    the filesystem: construction stays pure so a configuration error is
    visible before any resource is built. Directory creation with
    mode 0o700 belongs in `resolve_spool_directory`.

    `buffer_bytes=1 MiB` sizes the I/O buffers on both spool write and
    replay. The bounded-memory contract (ADR 0011 §Local spool design)
    is "proportional to columns + one row + bounded I/O buffers, not
    to row count". This is the "bounded I/O buffers" number.

    A Path -- not str -- for spool_directory: PathLike is easy to
    accept and hard to be strict about later, and the boundary between
    "the config value" and "a filesystem path" is worth keeping crisp.
    A caller with a string can spell Path(s) themselves.
    """

    spool_directory: Path | None = None
    buffer_bytes: int = 1_048_576

    def __post_init__(self):
        if self.spool_directory is not None and not isinstance(self.spool_directory, Path):
            raise DbPublishError(
                f'spool_directory must be a pathlib.Path or None, '
                f'got {type(self.spool_directory).__name__}'
            )
        # `type(...) is int`, not isinstance: bool subclasses int, so
        # CopyLoadPolicy(buffer_bytes=True) would silently produce a
        # one-byte buffer. Same guard IdentifierPolicy uses on its own
        # int field, for the same reason.
        if type(self.buffer_bytes) is not int or self.buffer_bytes < 1:
            raise DbPublishError(
                f'buffer_bytes must be a positive integer, got {self.buffer_bytes!r}'
            )


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
FORMAT_VERSION = 1

# The two stages a run produces, per ADR 0011 §Schema resolution and
# final COPY spool:
#   'neutral'  - type-neutral first spool (source normalization output)
#   'copytext' - final target-aware PostgreSQL COPY text spool
# Deletion of the neutral spool after copytext is written is part of the
# lifecycle, not this module's concern.
SPOOL_STAGES = ('neutral', 'copytext')


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
# name and the Phase 5.f cleanup pass can list the right directory.
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
) -> None:
    """Serialize magic + version + JSON ownership payload at the current
    position of `fp`. Called once at spool creation, before any row
    bytes. Does not seek; the caller decides the file's position.

    The payload duplicates fields already digested into `token`. That
    duplication is intentional: predecessor cleanup compares the header
    payload against the values it computes for the current run
    (task, schema, table, run_start_utc, pid, token) before deleting.
    The filename alone -- which is only the token -- is not enough
    positive ownership per ADR 0011 §Spool ownership.
    """
    if stage not in SPOOL_STAGES:
        raise DbPublishError(
            f'stage must be one of {SPOOL_STAGES}, got {stage!r}'
        )
    expected_token = compose_ownership_token(
        task=task,
        target_schema=target_schema,
        target_table=target_table,
        run_start_utc=run_start_utc,
        pid=pid,
    )
    if token != expected_token:
        # Refuse to write a header whose token disagrees with its own
        # ingredients. That combination would leave the file self-
        # inconsistent and defeat predecessor cleanup.
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
    }
    # sort_keys so byte-identical inputs produce byte-identical headers.
    # ensure_ascii to keep the on-disk header in a single-byte-per-char
    # subset; task/schema/table names are ASCII per PORTABLE_IDENTIFIER_RE
    # anyway, so this constrains nothing real.
    payload_bytes = json.dumps(payload, sort_keys=True, ensure_ascii=True).encode('ascii')
    if len(payload_bytes) > _MAX_HEADER_PAYLOAD_BYTES:
        raise DbPublishError(
            f'header payload too large: {len(payload_bytes)} bytes '
            f'(limit {_MAX_HEADER_PAYLOAD_BYTES})'
        )
    fp.write(_HEADER_PREFIX.pack(MAGIC, FORMAT_VERSION, len(payload_bytes)))
    fp.write(payload_bytes)


def read_spool_header(fp: BinaryIO) -> dict[str, Any]:
    """Deserialize the header at the current position of `fp` and
    return the payload dict. Raises SpoolFormatError on any deviation
    (short read, wrong magic, unknown version, oversized length,
    malformed JSON, missing required key).

    Predecessor cleanup treats SpoolFormatError as "not ours, leave
    alone". Any other exception type would be a bug in this module.
    """
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
    required = ('task', 'target_schema', 'target_table',
                'run_start_utc', 'pid', 'token', 'stage')
    missing = [key for key in required if key not in payload]
    if missing:
        raise SpoolFormatError(f'header payload missing keys: {missing}')
    if payload['stage'] not in SPOOL_STAGES:
        raise SpoolFormatError(
            f'header payload stage {payload["stage"]!r} not in {SPOOL_STAGES}'
        )
    # Return a plain dict so callers can mutate safely.
    return dict(payload)


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
    # comparisons downstream (containment checks in Phase 5.f) start
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


# --- COPY text serialization ------------------------------------------

# PostgreSQL COPY text format wire rules used here:
#   - field separator: TAB (0x09)
#   - row terminator: LF (0x0A)
#   - NULL marker: the two bytes 0x5C 0x4E  (`\N`)
#   - escape character: 0x5C (backslash)
#
# We escape only the four bytes COPY treats as structural: backslash, tab,
# newline, carriage return. Everything else -- including UTF-8 continuation
# bytes and any control character other than TAB/LF/CR -- passes through
# unchanged. `_validate_declared_value` has already rejected NUL (0x00) in
# text columns, so we never need to escape it here.
#
# Ordering matters: backslash must be escaped FIRST. Otherwise a genuine
# `\` in user data would combine with a following `n` we just inserted
# and be read back as a newline by COPY. Test 10 covers the ordering.


def _escape_copytext_text(text: str) -> bytes:
    """Encode `text` as UTF-8 with COPY text escaping applied."""
    escaped = (
        text.replace('\\', '\\\\')
            .replace('\t', '\\t')
            .replace('\n', '\\n')
            .replace('\r', '\\r')
    )
    return escaped.encode('utf-8')


def _serialize_value_copytext(value: Any, column) -> bytes:
    """Serialize one non-None value to its COPY text field payload.

    Callers must handle `value is None` themselves (the row serializer
    does, emitting the two-byte NULL marker without dispatching here).
    Everything else is dispatched on the declared type family, which
    `_validate_declared_value` has already checked matches the value's
    Python type.
    """
    family = _declared_type_family(column.type)

    if family == 'bool':
        return b't' if value else b'f'

    if family in {'smallint', 'integer', 'bigint'}:
        return str(value).encode('ascii')

    if family == 'numeric':
        # str(Decimal) is the exact digit sequence; str(int) is ASCII-safe.
        # Both round-trip through PostgreSQL NUMERIC parsing.
        return str(value).encode('ascii')

    if family == 'float':
        # PostgreSQL accepts NaN, Infinity, and -Infinity in float columns,
        # but its parser is case-sensitive on those spellings -- str(float)
        # produces 'nan', 'inf', '-inf', which PostgreSQL rejects. Explicit
        # branches keep this obvious and testable per-value.
        if value != value:
            return b'NaN'
        if value == float('inf'):
            return b'Infinity'
        if value == float('-inf'):
            return b'-Infinity'
        return repr(value).encode('ascii')

    if family == 'text':
        return _escape_copytext_text(value)

    if family == 'bytes':
        # bytea in COPY text expects the wire field to be `\x<hex>`. The
        # backslash is COPY's escape character, so the wire must carry
        # `\\x<hex>` for COPY's unescape pass to yield literal `\x<hex>`
        # for the bytea input parser.
        if isinstance(value, (bytearray, memoryview)):
            payload = bytes(value)
        else:
            payload = value
        return b'\\\\x' + payload.hex().encode('ascii')

    if family == 'date':
        return value.isoformat().encode('ascii')

    if family == 'datetime':
        # `sep=' '` matches PostgreSQL's canonical timestamp text form.
        # None of the isoformat characters (digits, `-`, `:`, `.`, `+`,
        # space) are structural for COPY, so no escaping is needed.
        return value.isoformat(sep=' ').encode('ascii')

    raise DbPublishError(
        f'internal invariant violated -- unsupported family {family!r} in copy serializer'
    )


def serialize_row_to_copytext(
    row: Mapping[str, Any],
    columns: Sequence,
    table_name: str,
    row_number: int,
) -> bytes:
    """Convert one validated row to its COPY text wire representation.

    Every cell is passed through `_validate_declared_value` first -- the
    same kernel the insert path uses -- so this function is the single
    boundary at which a mistyped value or an unexpected NULL in a
    non-nullable column produces a `DbPublishError`. Escaping exists
    exactly once in the codebase, inside `_escape_copytext_text`.

    Returns the wire bytes for the row: fields joined by TAB, terminated
    by LF. `columns` is the resolved schema in wire order (framework
    columns already appended); `row` must supply exactly those keys, but
    iteration order is taken from `columns` so the wire order is stable.
    """
    fields: list[bytes] = []
    for column in columns:
        value = row[column.name]
        _validate_declared_value(table_name, column, row_number, value)
        if value is None:
            fields.append(b'\\N')
        else:
            fields.append(_serialize_value_copytext(value, column))
    return b'\t'.join(fields) + b'\n'


# --- Spool file lifecycle ---------------------------------------------

# SpoolIdentity bundles the five ownership ingredients so callers do not
# thread five keyword arguments through every spool primitive. Frozen
# because a partially-mutated identity would silently detach the derived
# token from its ingredients and defeat header verification. The token
# is computed once, in __post_init__, so any ingredient-level validation
# error (empty task, non-UTC timestamp, ...) surfaces at construction
# time -- before any file is created.

@dataclass(frozen=True)
class SpoolIdentity:
    task: str
    target_schema: str
    target_table: str
    run_start_utc: datetime
    pid: int
    token: str = field(init=False)

    def __post_init__(self):
        # compose_ownership_token does the input validation for us.
        token = compose_ownership_token(
            task=self.task,
            target_schema=self.target_schema,
            target_table=self.target_table,
            run_start_utc=self.run_start_utc,
            pid=self.pid,
        )
        object.__setattr__(self, 'token', token)


def open_spool_for_write(
    directory: Path,
    *,
    stage: str,
    identity: SpoolIdentity,
    buffer_bytes: int = 1_048_576,
) -> tuple[BinaryIO, Path]:
    """Atomically create the spool file for `stage` and write the header.

    O_EXCL: opening fails with FileExistsError if a file at that path
    already exists. Predecessor cleanup runs first under the task
    advisory lock and removes any prior spool at this path; if one
    still exists after that pass, either the operator dropped a file
    in by hand or two runners collided somehow, and silently
    overwriting either would destroy evidence. Refusing is correct.

    On POSIX, mode 0o600 restricts the file to the owning user; on
    Windows the mode is ignored and access control comes from the
    parent directory ACL (per ADR 0011 §Spool ownership).

    Returns (fp, path). The header is written before return, so the
    file on disk always carries positive ownership from the moment it
    exists. If the header write raises, we close the fp and unlink the
    file so a half-created spool cannot outlive this call.
    """
    if not isinstance(directory, Path):
        raise DbPublishError(
            f'directory must be a pathlib.Path, got {type(directory).__name__}'
        )
    if stage not in SPOOL_STAGES:
        raise DbPublishError(
            f'stage must be one of {SPOOL_STAGES}, got {stage!r}'
        )
    if not isinstance(identity, SpoolIdentity):
        raise DbPublishError(
            f'identity must be a SpoolIdentity, got {type(identity).__name__}'
        )
    if type(buffer_bytes) is not int or buffer_bytes < 1:
        raise DbPublishError(
            f'buffer_bytes must be a positive integer, got {buffer_bytes!r}'
        )

    filename = compose_spool_filename(token=identity.token, stage=stage)
    path = directory / filename

    # O_BINARY is a Windows-only flag that disables CRLF translation on
    # the fd. POSIX has no such flag; getattr defaulting to 0 keeps the
    # bitmask correct on both platforms.
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0)
    fd = os.open(path, flags, 0o600)
    try:
        fp = os.fdopen(fd, 'wb', buffering=buffer_bytes)
    except BaseException:
        # os.fdopen failure leaves the fd unclosed; recover it before
        # re-raising so this branch cannot leak an fd.
        os.close(fd)
        raise
    try:
        write_spool_header(
            fp,
            task=identity.task,
            target_schema=identity.target_schema,
            target_table=identity.target_table,
            run_start_utc=identity.run_start_utc,
            pid=identity.pid,
            token=identity.token,
            stage=stage,
        )
    except BaseException:
        try:
            fp.close()
        finally:
            try:
                path.unlink()
            except OSError:
                # If we cannot unlink, propagate the original exception
                # rather than mask it with an unlink failure. The
                # cleanup pass on the next run will treat the
                # header-less file as unknown and preserve it, which
                # is the correct default.
                pass
        raise
    return fp, path


def open_spool_for_read(
    path: Path,
    *,
    identity: SpoolIdentity,
    stage: str,
    buffer_bytes: int = 1_048_576,
) -> BinaryIO:
    """Open `path` for read and verify the header names this identity+stage.

    Strict: any framing error propagates as SpoolFormatError (the file
    is not a well-formed spool); an identity or stage mismatch raises
    DbPublishError (the file is well-formed but names someone else --
    a bug if the caller is reading a spool it wrote in this run).

    The distinction matters for predecessor cleanup vs own-spool
    replay: predecessor cleanup uses read_spool_header directly and
    tolerates SpoolFormatError, while own-spool replay must not.
    """
    if not isinstance(path, Path):
        raise DbPublishError(
            f'path must be a pathlib.Path, got {type(path).__name__}'
        )
    if stage not in SPOOL_STAGES:
        raise DbPublishError(
            f'stage must be one of {SPOOL_STAGES}, got {stage!r}'
        )
    if not isinstance(identity, SpoolIdentity):
        raise DbPublishError(
            f'identity must be a SpoolIdentity, got {type(identity).__name__}'
        )
    if type(buffer_bytes) is not int or buffer_bytes < 1:
        raise DbPublishError(
            f'buffer_bytes must be a positive integer, got {buffer_bytes!r}'
        )

    fp = open(path, 'rb', buffering=buffer_bytes)
    try:
        header = read_spool_header(fp)
    except BaseException:
        fp.close()
        raise

    if header['token'] != identity.token:
        fp.close()
        raise DbPublishError(
            f'spool at {path} header token {header["token"]!r} does not match '
            f'identity token {identity.token!r}'
        )
    if header['stage'] != stage:
        fp.close()
        raise DbPublishError(
            f'spool at {path} header stage {header["stage"]!r} does not match '
            f'expected stage {stage!r}'
        )
    return fp


def cleanup_spool_paths(paths: Sequence[Path]) -> list[Path]:
    """Best-effort delete every path. Returns the paths that could not
    be removed (already-missing counts as success).

    Used to reap the spools this run created, in both success and
    failure paths. Never raises: cleanup happens in `finally` blocks
    where an exception would mask the original one.
    """
    failed: list[Path] = []
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            # Already gone -- another cleanup pass, another process,
            # or the OS reaped it. Success.
            pass
        except OSError:
            failed.append(path)
    return failed


def cleanup_predecessor_spools(
    directory: Path,
    *,
    task: str,
) -> tuple[list[Path], list[Path]]:
    """Reap spool files that positively belong to this task from prior runs.

    Must be called only while the task's advisory lock is held: the
    invariant it relies on is "no other run of this task is in
    progress", which the lock is what enforces. A spool whose header
    task-field matches ours therefore cannot be a live spool from a
    concurrent runner -- it can only be a stale one from a run that
    crashed, was killed, or exited before its cleanup ran.

    Positive ownership requires BOTH the filename grammar match AND
    the internal header task-field match (ADR 0011 §Spool ownership:
    "unknown or malformed files preserved rather than guessed to be
    ours"). Any other file -- foreign task, wrong magic, truncated
    header, non-spool filename -- is preserved.

    Returns (deleted, preserved) as sorted lists of Paths. The
    directory need not exist; a first-run task has nothing to clean.
    A non-directory at that path is a DbPublishError: the operator
    configured the spool location to something we cannot manage, and
    proceeding would either fail obscurely later or clobber their file.
    """
    if not isinstance(directory, Path):
        raise DbPublishError(
            f'directory must be a pathlib.Path, got {type(directory).__name__}'
        )
    if not isinstance(task, str) or not task:
        raise DbPublishError(f'task must be a non-empty str, got {task!r}')

    if not directory.exists():
        return ([], [])
    if not directory.is_dir():
        raise DbPublishError(
            f'spool directory path exists but is not a directory: {directory}'
        )

    deleted: list[Path] = []
    preserved: list[Path] = []
    for entry in sorted(directory.iterdir()):
        if not entry.is_file():
            # Subdirectory, symlink to a directory, device node, ...
            # never ours.
            preserved.append(entry)
            continue
        if parse_spool_filename(entry.name) is None:
            # Filename does not match the grammar -- not a spool we
            # would have written.
            preserved.append(entry)
            continue
        try:
            with entry.open('rb') as fp:
                header = read_spool_header(fp)
        except SpoolFormatError:
            # Filename matches but header does not. Could be a garbled
            # file that happens to share our filename prefix, or a
            # forward-version spool. Preserve.
            preserved.append(entry)
            continue
        except OSError:
            # Permission denied, disappeared under us, ... preserve.
            preserved.append(entry)
            continue
        if header.get('task') != task:
            # Foreign task. Preserve.
            preserved.append(entry)
            continue
        try:
            entry.unlink()
        except OSError:
            preserved.append(entry)
            continue
        deleted.append(entry)
    return (deleted, preserved)


# --- Orchestrator ------------------------------------------------------

def prepare_copy_source(
    *,
    row_source: Iterable[Sequence[Any]],
    columns: Sequence[str],
    declared_schema: Sequence[ResolvedColumn] | None,
    identity: SpoolIdentity,
    directory: Path,
    policy: CopyLoadPolicy | None = None,
    framework_columns: Sequence[ResolvedColumn] = (),
) -> tuple[Path, tuple[ResolvedColumn, ...]]:
    """Prepare a COPY-text spool from a positional row source in two passes.

    Pass 1 writes the type-neutral spool while (optionally) feeding a
    schema-inference accumulator. Pass 2 replays the neutral spool through
    the target-aware COPY-text serializer, resolving each cell against the
    now-known column types.

    `row_source` must yield tuples whose positional order matches `columns`.
    Values must arrive pre-normalized to native Python scalars (or None) --
    normalization is the caller's responsibility, per ADR 0011 §Row-source
    contract. If a raw pandas/petl value reaches here it may misclassify
    against the inference stream or fail an `_write_value` type check.

    `declared_schema=None` triggers inference. Otherwise the caller-supplied
    columns are used verbatim and must line up name-for-name with
    `columns` -- a mismatch is a configuration error, caught here rather
    than allowed to produce silently reordered output.

    `framework_columns` pins the resolved type of technical columns whose
    value is caller-supplied and constant (e.g. `etl_updated_at`) after
    inference completes. Necessary because the row-source accumulator has
    no way to know a constant timezone-aware datetime should stay
    aware -- inference on the datetime family alone resolves to a naive
    `sa.DateTime()`. In declared mode this override is a no-op (declared
    columns already carry their pinned type), but the same framework
    tuple is accepted and validated for symmetry with the caller.

    On success: returns (copytext_path, resolved_columns), and the neutral
    spool has been reaped. On any exception both spools this call created
    are best-effort deleted before the exception propagates, so a failed
    call leaves no trailing spool files.

    Test-only in 0.6.4: no public entry point invokes this yet.
    `db_loader='copy'` remains publicly rejected until Phase 6 lifts the
    gate.
    """
    # --- Input validation (all before any file is created) ------------
    if policy is None:
        policy = CopyLoadPolicy()
    if not isinstance(policy, CopyLoadPolicy):
        raise DbPublishError(
            f'policy must be a CopyLoadPolicy or None, got {type(policy).__name__}'
        )
    if not isinstance(identity, SpoolIdentity):
        raise DbPublishError(
            f'identity must be a SpoolIdentity, got {type(identity).__name__}'
        )
    if not isinstance(directory, Path):
        raise DbPublishError(
            f'directory must be a pathlib.Path, got {type(directory).__name__}'
        )
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
        raise DbPublishError(
            f'columns must be a Sequence of str, got {type(columns).__name__}'
        )
    columns_tuple = tuple(columns)
    if not columns_tuple:
        raise DbPublishError('columns must be non-empty')
    for name in columns_tuple:
        if not isinstance(name, str) or not name:
            raise DbPublishError(
                f'column names must be non-empty strings, got {name!r}'
            )
    # Duplicate column names would silently collapse in pass 2's
    # `dict(zip(columns_tuple, values))`, causing later columns of the
    # same name to overwrite earlier ones in the serialized row. The
    # caller-side RowProjection already blocks duplicates upstream, so
    # this is defensive symmetry rather than the primary guard.
    duplicates = find_duplicates(columns_tuple)
    if duplicates:
        raise DbPublishError(
            f'columns must not contain duplicate names, got duplicates: {duplicates!r}'
        )
    if declared_schema is not None:
        declared_tuple = tuple(declared_schema)
        for col in declared_tuple:
            if not isinstance(col, ResolvedColumn):
                raise DbPublishError(
                    f'declared_schema entries must be ResolvedColumn, '
                    f'got {type(col).__name__}'
                )
        declared_names = [c.name for c in declared_tuple]
        if declared_names != list(columns_tuple):
            raise DbPublishError(
                f'declared_schema column names {declared_names!r} do not '
                f'match columns {list(columns_tuple)!r} in the same order'
            )
    framework_tuple = tuple(framework_columns)
    for col in framework_tuple:
        if not isinstance(col, ResolvedColumn):
            raise DbPublishError(
                f'framework_columns entries must be ResolvedColumn, '
                f'got {type(col).__name__}'
            )
    if framework_tuple:
        column_set = set(columns_tuple)
        unknown = [c.name for c in framework_tuple if c.name not in column_set]
        if unknown:
            raise DbPublishError(
                f'framework_columns include names not present in columns: {unknown!r}'
            )
    if row_source is None:
        raise DbPublishError('row_source must not be None')

    # --- Execute the two passes ---------------------------------------

    neutral_path: Path | None = None
    copytext_path: Path | None = None
    try:
        # Pass 1: type-neutral spool. Inference runs alongside only if the
        # caller did not supply a schema -- otherwise the accumulator's
        # output would be discarded, which is a false parallel worth
        # avoiding for readability.
        if declared_schema is None:
            state = _InferenceStreamState(len(columns_tuple))
        else:
            state = None

        neutral_fp, neutral_path = open_spool_for_write(
            directory,
            stage='neutral',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
        )
        try:
            write_neutral_preamble(neutral_fp, columns=columns_tuple)
            for row in row_source:
                if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
                    raise DbPublishError(
                        f'row source yielded non-sequence {type(row).__name__}'
                    )
                row_tuple = tuple(row)
                if len(row_tuple) != len(columns_tuple):
                    raise DbPublishError(
                        f'row width {len(row_tuple)} does not match column '
                        f'count {len(columns_tuple)}'
                    )
                if state is not None:
                    state.feed_row(row_tuple)
                # write_neutral_row re-validates width, but the check here
                # covers the state.feed_row() call above which would raise
                # first with a less specific message.
                write_neutral_row(
                    neutral_fp, row_tuple, expected_width=len(columns_tuple),
                )
            write_neutral_terminator(neutral_fp)
        finally:
            neutral_fp.close()

        # Resolve schema between the passes: after inference is complete,
        # before copytext serialization begins.
        if declared_schema is not None:
            resolved_columns = tuple(declared_schema)
        else:
            resolved_types = state.resolve()
            resolved_columns = tuple(
                ResolvedColumn(
                    name=columns_tuple[i],
                    type=resolved_types[i],
                    nullable=True,
                )
                for i in range(len(columns_tuple))
            )
            # Pin framework-column types. Inference on a constant
            # timezone-aware datetime resolves to naive DateTime and
            # would then reject the aware value at serialize time, so
            # the caller's pinned type wins by name. This mirrors the
            # INSERT path's framework-column bypass in
            # _resolve_payload_schema; the two loaders resolve
            # framework-column types the same way.
            if framework_tuple:
                by_name = {c.name: c for c in framework_tuple}
                resolved_columns = tuple(
                    by_name.get(c.name, c) for c in resolved_columns
                )

        # Pass 2: replay neutral -> copytext. Every value goes through
        # `_validate_declared_value` (inside serialize_row_to_copytext),
        # which is where a mistyped cell or an unexpected NULL in a
        # non-nullable column raises DbPublishError.
        copytext_fp, copytext_path = open_spool_for_write(
            directory,
            stage='copytext',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
        )
        try:
            neutral_read = open_spool_for_read(
                neutral_path,
                identity=identity,
                stage='neutral',
                buffer_bytes=policy.buffer_bytes,
            )
            try:
                preamble = read_neutral_preamble(neutral_read)
                if preamble != columns_tuple:
                    # A defensive assertion: the writer above sets this
                    # value from the same `columns_tuple` the reader
                    # checks against, so a mismatch here would signal a
                    # bug in the neutral-spool round-trip itself.
                    raise DbPublishError(
                        f'neutral spool preamble columns {preamble!r} do '
                        f'not match expected {columns_tuple!r}'
                    )
                row_number = 0
                while True:
                    values = read_neutral_row(neutral_read, len(columns_tuple))
                    if values is None:
                        break
                    row_number += 1
                    row_dict = dict(zip(columns_tuple, values, strict=True))
                    copytext_fp.write(serialize_row_to_copytext(
                        row_dict,
                        resolved_columns,
                        identity.target_table,
                        row_number,
                    ))
            finally:
                neutral_read.close()
        finally:
            copytext_fp.close()

        # Success path: reap the neutral spool now that copytext is
        # committed to disk. Copytext survives for the caller to hand to
        # the COPY consumer in Phase 5.h.
        cleanup_spool_paths([neutral_path])
        return copytext_path, resolved_columns
    except BaseException:
        # Any failure -- input rejection, mid-pass source exception,
        # value-validation error, unlink refusal -- reaps every spool
        # this call created before the exception propagates. Files that
        # never got created (path is None) are skipped; already-missing
        # files are treated as success by cleanup_spool_paths.
        to_cleanup = [p for p in (neutral_path, copytext_path) if p is not None]
        if to_cleanup:
            cleanup_spool_paths(to_cleanup)
        raise


__all__ = [
    'CopyLoadPolicy',
    'SpoolFormatError',
    'MAGIC',
    'FORMAT_VERSION',
    'SPOOL_STAGES',
    'SPOOL_FILENAME_RE',
    'DEFAULT_SPOOL_SUBDIR',
    'SpoolIdentity',
    'compose_ownership_token',
    'compose_spool_filename',
    'parse_spool_filename',
    'write_spool_header',
    'read_spool_header',
    'resolve_spool_directory',
    'write_neutral_preamble',
    'read_neutral_preamble',
    'write_neutral_row',
    'read_neutral_row',
    'write_neutral_terminator',
    'serialize_row_to_copytext',
    'open_spool_for_write',
    'open_spool_for_read',
    'cleanup_spool_paths',
    'cleanup_predecessor_spools',
    'prepare_copy_source',
]
