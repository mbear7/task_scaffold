# -*- coding: utf-8 -*-
"""COPY loader subsystem: bounded spool preparation and DBAPI transport.

Layering (ADR 0011 §Implementation sequence):

    db_publish  ->  db_copy  ->  db_values

`db_copy` is one level *below* `db_publish`. It knows nothing about live-table
publication, advisory locks, transaction boundaries, or the `DbPublisher`
class. It cannot import `db_publish`, begin/commit/roll back transactions, or
create an engine or connection. Its only database operation opens a cursor
on the SQLAlchemy connection supplied by the publisher and
streams a prepared spool through psycopg2 `copy_expert()` into an already
created staging table.

The module is deliberately name-clean of the forbidden transaction and engine
operations so the architecture tests can enforce that ownership boundary.

Selected module surface:

- `CopyLoadPolicy`               - config dataclass, moved here from
                                    db_publish in 0.6.4 so its home
                                    matches its layer
- `SpoolFormatError`             - malformed or unowned spool
- `MAGIC`, `FORMAT_VERSION`      - internal header constants
- `SPOOL_STAGES`                 - supported spool-stage names
- `SPOOL_FILENAME_RE`            - exact portable grammar
- `compose_ownership_token`      - digest of the five ownership ingredients
- `compose_spool_filename`       - `task_core-copy-<token>-<stage>.spool`
- `parse_spool_filename`         - inverse; `None` on any deviation
- `write_spool_header` /
  `read_spool_header`            - the versioned internal header
- `resolve_spool_directory`      - best-effort creation with 0o700
- `cleanup_default_spool_directory` - best-effort removal of the empty
                                      framework-owned default directory

ADR 0011 §Spool ownership and cleanup requires *both* an exact filename
grammar and an internal header for positive ownership before predecessor
cleanup deletes anything. This module provides both primitives;
``DbPublisher.begin_run()`` invokes predecessor cleanup under the task
advisory lock.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import errno
import hashlib
import io
import json
import logging
import os
import re
import secrets
import struct
import tempfile
import time
from pathlib import Path
from typing import Any, BinaryIO, Callable

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from task_core.db_values import (
    DbPublishError,
    ResolvedColumn,
    _declared_type_family,
    _declared_value_error,
    _InferenceStreamState,
    _is_aware_datetime,
    _normalize_value,
    _resolve_override,
    _validate_declared_value,
    _validate_numeric_value,
)
from task_core.types import find_duplicates


log = logging.getLogger(__name__)


# --- Config ------------------------------------------------------------

@dataclass(frozen=True, kw_only=True)
class CopyLoadPolicy:
    """Where and how the COPY loader spools rows before database transport.

    Three settings, all with defaults that keep every existing INSERT-path
    caller unchanged. Tasks using `db_loader='copy'` inherit these settings
    unless a task-level override is explicitly supplied.

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

    `encrypt_spools=True` protects both spool bodies with independently
    generated AES-256-GCM keys. The ownership header remains plaintext so
    a successor run can identify and delete abandoned files after the key
    has disappeared. A task may explicitly opt out through
    `PipelineSpec.db_copy_spool_encryption=False`; the outer container and
    cleanup rules remain the same.
    """

    spool_directory: Path | None = None
    buffer_bytes: int = 1_048_576
    encrypt_spools: bool = True

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
        if type(self.encrypt_spools) is not bool:
            raise DbPublishError(
                f'encrypt_spools must be bool, got {self.encrypt_spools!r}'
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


def cleanup_default_spool_directory(
    policy: CopyLoadPolicy | None,
    *,
    attempts: int = 3,
    retry_delay_seconds: float = 0.05,
) -> bool:
    """Best-effort removal of the empty framework-owned spool root.

    Only the implicit default directory is task_core-owned. A configured
    ``spool_directory`` belongs to the operator and is never removed, even
    when it is empty or resolves to the same path as the default root.

    ``Path.rmdir()`` is the race-safe primitive: it removes only an empty
    directory. A concurrent task or a preserved foreign file therefore makes
    the call fail with ``ENOTEMPTY``/``EEXIST`` and the directory remains.
    Missing directories count as success. Other transient failures receive
    bounded retries and a final warning, but never replace the task result.
    """
    if policy is None:
        policy = CopyLoadPolicy()
    if not isinstance(policy, CopyLoadPolicy):
        raise DbPublishError(
            f'policy must be a CopyLoadPolicy or None, got {type(policy).__name__}'
        )
    if type(attempts) is not int or attempts < 1:
        raise DbPublishError(f'attempts must be a positive integer, got {attempts!r}')
    if type(retry_delay_seconds) not in (int, float) or retry_delay_seconds < 0:
        raise DbPublishError(
            f'retry_delay_seconds must be non-negative, got {retry_delay_seconds!r}'
        )
    if policy.spool_directory is not None:
        return False

    target = (Path(tempfile.gettempdir()) / DEFAULT_SPOOL_SUBDIR).resolve(
        strict=False,
    )
    for attempt in range(1, attempts + 1):
        try:
            target.rmdir()
        except FileNotFoundError:
            return True
        except OSError as exc:
            if exc.errno in (errno.ENOTEMPTY, errno.EEXIST):
                return False
            if attempt == attempts:
                log.warning(
                    'could not remove empty default COPY spool directory %s '
                    'after %s attempt(s): %s',
                    target, attempts, exc,
                )
                return False
            if retry_delay_seconds:
                time.sleep(retry_delay_seconds)
        else:
            return True
    return False


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


def _serialize_float_text(value: float) -> bytes:
    if value != value:
        return b'NaN'
    if value == float('inf'):
        return b'Infinity'
    if value == float('-inf'):
        return b'-Infinity'
    return repr(value).encode('ascii')


def _serialize_value_copytext_family(value: Any, column, family: str) -> bytes:
    """Serialize one non-NULL declared value for a pre-resolved family."""
    if family == 'bool':
        return b't' if value else b'f'
    if family in {'smallint', 'integer', 'bigint'}:
        return str(value).encode('ascii')
    if family == 'numeric':
        return str(value).encode('ascii')
    if family == 'float':
        return _serialize_float_text(value)
    if family == 'text':
        return _escape_copytext_text(value)
    if family == 'bytes':
        payload = bytes(value) if isinstance(value, (bytearray, memoryview)) else value
        return b'\\\\x' + payload.hex().encode('ascii')
    if family == 'date':
        return value.isoformat().encode('ascii')
    if family == 'datetime':
        return value.isoformat(sep=' ').encode('ascii')
    raise DbPublishError(
        f'internal invariant violated -- unsupported family {family!r} in copy serializer'
    )


def _serialize_value_copytext(value: Any, column) -> bytes:
    """Serialize one value already validated against a declared schema."""
    family = _declared_type_family(column.type)
    return _serialize_value_copytext_family(value, column, family)


def _serialize_inferred_value_copytext_family(
    value: Any,
    column: ResolvedColumn,
    table_name: str,
    row_number: int,
    family: str,
) -> bytes:
    """Serialize one inferred/override value using PostgreSQL input syntax.

    Declared mode deliberately refuses semantic coercion. Inferred mode is
    different: its resolved type can represent several observed Python
    families (int+float -> NUMERIC, date+datetime -> TIMESTAMP, mixed scalar
    families -> TEXT). COPY must therefore render those observed values in
    the resolved target type instead of applying the stricter declared-mode
    validator that INSERT never applies to inferred payloads.
    """
    if value is None:
        if column.nullable:
            return b'\\N'
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} contains NULL in '
            f'non-nullable column {column.name!r}'
        )

    if family == 'bool':
        if type(value) is not bool:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: expected bool'
            )
        return b't' if value else b'f'

    if family in {'smallint', 'integer', 'bigint'}:
        if type(value) is not int:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: expected int'
            )
        # Reuse the declared range check; it is not coercion and catches a
        # value PostgreSQL would reject after the expensive spool pass.
        _validate_declared_value(table_name, column, row_number, value)
        return str(value).encode('ascii')

    if family == 'numeric':
        if type(value) is int or isinstance(value, Decimal):
            return str(value).encode('ascii')
        if type(value) is float:
            return _serialize_float_text(value)
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} column {column.name!r} '
            f'is incompatible with inferred/overridden type {column.type}: '
            'expected int, float, or Decimal'
        )

    if family == 'float':
        if type(value) is float:
            return _serialize_float_text(value)
        if type(value) is int or isinstance(value, Decimal):
            return str(value).encode('ascii')
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} column {column.name!r} '
            f'is incompatible with inferred/overridden type {column.type}: '
            'expected int, float, or Decimal'
        )

    if family == 'text':
        if isinstance(value, str):
            text = value
        elif type(value) is bool:
            text = 'true' if value else 'false'
        elif type(value) in {int, float} or isinstance(value, Decimal):
            if type(value) is float and not (value == value and abs(value) != float('inf')):
                text = _serialize_float_text(value).decode('ascii')
            else:
                text = str(value)
        elif isinstance(value, datetime):
            text = value.isoformat(sep=' ')
        elif type(value) is date:
            text = value.isoformat()
        elif isinstance(value, (bytes, bytearray, memoryview)):
            # There is no unambiguous bytea-to-text coercion. Failing here is
            # safer than inventing a representation that INSERT may not use.
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                'cannot render bytes-like data into an inferred TEXT column'
            )
        else:
            text = str(value)
        if '\x00' in text:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                'contains NUL, which PostgreSQL text does not support'
            )
        return _escape_copytext_text(text)

    if family == 'bytes':
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: '
                'expected bytes-like value'
            )
        payload = bytes(value)
        return b'\\\\x' + payload.hex().encode('ascii')

    if family == 'date':
        if type(value) is not date:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: expected date'
            )
        return value.isoformat().encode('ascii')

    if family == 'datetime':
        if isinstance(value, datetime):
            return value.isoformat(sep=' ').encode('ascii')
        if type(value) is date:
            # PostgreSQL TIMESTAMP input accepts a date as midnight. This is
            # the widening represented by the shared date+datetime inference
            # rule, not a declared-mode convenience conversion.
            return value.isoformat().encode('ascii')
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} column {column.name!r} '
            f'is incompatible with inferred/overridden type {column.type}: '
            'expected date or datetime'
        )

    raise DbPublishError(
        f'internal invariant violated -- unsupported family {family!r} in inferred copy serializer'
    )


def _serialize_inferred_value_copytext(
    value: Any,
    column: ResolvedColumn,
    table_name: str,
    row_number: int,
) -> bytes:
    family = _declared_type_family(column.type)
    return _serialize_inferred_value_copytext_family(
        value,
        column,
        table_name,
        row_number,
        family,
    )


_CompiledInferredFieldSerializer = tuple[int, Callable[[Any, int], bytes]]
_CompiledDeclaredFieldWriter = tuple[int, Callable[[Any, int, bytearray], None]]


_DECLARED_INTEGER_RANGES = {
    'smallint': (-(2 ** 15), (2 ** 15) - 1),
    'integer': (-(2 ** 31), (2 ** 31) - 1),
    'bigint': (-(2 ** 63), (2 ** 63) - 1),
}


def _write_declared_null(
    *,
    table_name: str,
    column: ResolvedColumn,
    row_number: int,
    buffer: bytearray,
) -> None:
    if not column.nullable:
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} contains NULL in '
            f'non-nullable column {column.name!r}'
        )
    buffer.extend(b'\\N')


def _compile_declared_copy_field_writers(
    source_columns: Sequence[str],
    resolved_columns: Sequence[ResolvedColumn],
    table_name: str,
) -> tuple[_CompiledDeclaredFieldWriter, ...]:
    """Compile direct declared-value writers once per COPY spool.

    Common native Python values stay entirely on a family-specific hot path:
    missing handling, type validation, declared constraints, and COPY-text
    encoding happen in one callable. `_normalize_value()` remains the exact
    compatibility fallback for pandas, NumPy, and other scalar wrappers, but
    ordinary task rows no longer pay its generic `pd.isna()` and duck-typing
    cost for every cell.
    """
    source_index = {name: index for index, name in enumerate(source_columns)}
    compiled: list[_CompiledDeclaredFieldWriter] = []

    for column in resolved_columns:
        index = source_index[column.name]
        family = _declared_type_family(column.type)

        if family == 'bool':
            def write_bool(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if type(value) is bool:
                    buffer.append(0x74 if value else 0x66)
                    return
                if value is not None:
                    value = _normalize_value(value)
                if value is None:
                    _write_declared_null(
                        table_name=table_name,
                        column=column,
                        row_number=row_number,
                        buffer=buffer,
                    )
                    return
                if type(value) is not bool:
                    _declared_value_error(
                        table_name, column, row_number, 'expected bool',
                    )
                buffer.append(0x74 if value else 0x66)

            writer = write_bool

        elif family in _DECLARED_INTEGER_RANGES:
            lower, upper = _DECLARED_INTEGER_RANGES[family]

            def write_integer(
                value,
                row_number,
                buffer,
                *,
                column=column,
                family=family,
                lower=lower,
                upper=upper,
            ):
                if type(value) is not int:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if type(value) is not int:
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected int, not bool or another numeric family',
                        )
                if not lower <= value <= upper:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        f'value is outside {family} range',
                    )
                buffer.extend(str(value).encode('ascii'))

            writer = write_integer

        elif family == 'numeric':
            def write_numeric(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                native = type(value) is int or isinstance(value, Decimal)
                if not native:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                elif isinstance(value, Decimal) and value.is_nan():
                    value = None
                    _write_declared_null(
                        table_name=table_name,
                        column=column,
                        row_number=row_number,
                        buffer=buffer,
                    )
                    return
                _validate_numeric_value(
                    table_name,
                    column,
                    row_number,
                    value,
                )
                buffer.extend(str(value).encode('ascii'))

            writer = write_numeric

        elif family == 'float':
            def write_float(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if type(value) is float:
                    if value != value:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                else:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if type(value) is not float:
                        _declared_value_error(
                            table_name, column, row_number, 'expected float',
                        )
                buffer.extend(_serialize_float_text(value))

            writer = write_float

        elif family == 'text':
            max_length = column.type.length

            def write_text(
                value,
                row_number,
                buffer,
                *,
                column=column,
                max_length=max_length,
            ):
                if type(value) is not str:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if not isinstance(value, str):
                        _declared_value_error(
                            table_name, column, row_number, 'expected str',
                        )
                if '\x00' in value:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        'NUL character is not supported in PostgreSQL text',
                    )
                if max_length is not None and len(value) > max_length:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        f'text length exceeds VARCHAR({max_length})',
                    )
                buffer.extend(_escape_copytext_text(value))

            writer = write_text

        elif family == 'bytes':
            def write_bytes(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if not isinstance(value, (bytes, bytearray, memoryview)):
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if not isinstance(value, (bytes, bytearray, memoryview)):
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected bytes-like value',
                        )
                payload = (
                    bytes(value)
                    if isinstance(value, (bytearray, memoryview))
                    else value
                )
                buffer.extend(b'\\\\x')
                buffer.extend(payload.hex().encode('ascii'))

            writer = write_bytes

        elif family == 'date':
            def write_date(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if type(value) is not date:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if type(value) is not date:
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected date; datetime-to-DATE conversion is not implicit',
                        )
                buffer.extend(value.isoformat().encode('ascii'))

            writer = write_date

        elif family == 'datetime':
            wants_timezone = bool(column.type.timezone)

            def write_datetime(
                value,
                row_number,
                buffer,
                *,
                column=column,
                wants_timezone=wants_timezone,
            ):
                if type(value) is not datetime:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if not isinstance(value, datetime):
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected datetime',
                        )
                aware = _is_aware_datetime(value)
                if wants_timezone and not aware:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        'timezone-aware datetime required',
                    )
                if not wants_timezone and aware:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        'timezone-aware datetime cannot be published to timestamp without time zone',
                    )
                buffer.extend(value.isoformat(sep=' ').encode('ascii'))

            writer = write_datetime

        else:
            raise DbPublishError(
                f'internal invariant violated -- unsupported family '
                f'{family!r} in declared COPY compiler'
            )

        compiled.append((index, writer))

    return tuple(compiled)


def _compile_inferred_copy_field_serializers(
    source_columns: Sequence[str],
    resolved_columns: Sequence[ResolvedColumn],
    table_name: str,
) -> tuple[_CompiledInferredFieldSerializer, ...]:
    """Compile inferred source positions and scalar families once per spool."""
    source_index = {name: index for index, name in enumerate(source_columns)}
    compiled: list[_CompiledInferredFieldSerializer] = []
    for column in resolved_columns:
        index = source_index[column.name]
        family = _declared_type_family(column.type)

        def render_inferred(
            value,
            row_number,
            *,
            column=column,
            family=family,
        ):
            return _serialize_inferred_value_copytext_family(
                value,
                column,
                table_name,
                row_number,
                family,
            )

        compiled.append((index, render_inferred))
    return tuple(compiled)


def _write_compiled_declared_copytext_row(
    fp: BinaryIO,
    row: Sequence[Any],
    serializers: Sequence[_CompiledDeclaredFieldWriter],
    row_number: int,
    buffer: bytearray,
    *,
    expected_width: int,
) -> None:
    """Validate and serialize one raw declared row without a normalized tuple."""
    if type(row) not in (tuple, list):
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
            raise DbPublishError(
                f'row source yielded non-sequence {type(row).__name__}'
            )
    if len(row) != expected_width:
        raise DbPublishError(
            f'row width {len(row)} does not match column count {expected_width}'
        )

    buffer.clear()
    for field_number, (source_index, write_field) in enumerate(serializers):
        if field_number:
            buffer.append(0x09)
        write_field(row[source_index], row_number, buffer)
    buffer.append(0x0A)
    fp.write(buffer)


def _write_compiled_inferred_copytext_row(
    fp: BinaryIO,
    values: Sequence[Any],
    serializers: Sequence[_CompiledInferredFieldSerializer],
    row_number: int,
    buffer: bytearray,
) -> None:
    """Serialize one normalized inferred row into a reusable output buffer."""
    buffer.clear()
    for field_number, (source_index, render) in enumerate(serializers):
        if field_number:
            buffer.append(0x09)
        buffer.extend(render(values[source_index], row_number))
    buffer.append(0x0A)
    fp.write(buffer)

def serialize_row_to_copytext(
    row: Mapping[str, Any],
    columns: Sequence,
    table_name: str,
    row_number: int,
    *,
    declared: bool = True,
) -> bytes:
    """Convert one normalized row to its COPY text wire representation.

    Declared mode applies `_validate_declared_value`, the same strict kernel
    used by INSERT. Inferred mode instead renders values through the resolved
    widening family because INSERT does not apply declared coercion rules to
    inferred payloads. Both paths reject unexpected NULLs. Escaping exists
    exactly once in the codebase, inside `_escape_copytext_text`.

    Returns the wire bytes for the row: fields joined by TAB, terminated
    by LF. `columns` is the resolved schema in wire order (framework
    columns already appended); `row` must supply exactly those keys, but
    iteration order is taken from `columns` so the wire order is stable.
    """
    fields: list[bytes] = []
    for column in columns:
        value = row[column.name]
        if declared:
            _validate_declared_value(table_name, column, row_number, value)
            if value is None:
                fields.append(b'\\N')
            else:
                fields.append(_serialize_value_copytext(value, column))
        else:
            fields.append(_serialize_inferred_value_copytext(
                value, column, table_name, row_number,
            ))
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
class PreparedCopySource:
    """Immutable COPY preparation result.

    The exact row count is captured during the one permitted source
    traversal. ``spool_bytes`` is the final on-disk COPY-text spool size,
    including the ownership header. The caller owns ``path`` after return.
    """

    path: Path
    columns: tuple[ResolvedColumn, ...]
    row_count: int
    spool_bytes: int
    identity: SpoolIdentity
    buffer_bytes: int
    protection: str
    _key: bytes | None = field(repr=False, compare=False, default=None)

    def open_reader(self) -> BinaryIO:
        """Return a bounded plaintext reader over the final spool body.

        For encrypted spools the key stays on this in-memory result object;
        no decrypted temporary file is created.
        """
        return open_spool_for_read(
            self.path,
            identity=self.identity,
            stage='copytext',
            buffer_bytes=self.buffer_bytes,
            key=self._key,
        )

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


@dataclass(frozen=True)
class SpoolWriteHandle:
    stream: BinaryIO
    path: Path
    key: bytes | None
    protection: str


def _write_all(fp: BinaryIO, data: bytes) -> None:
    """Write every byte or fail; do not treat a short raw write as success."""
    view = memoryview(data)
    while view:
        written = fp.write(view)
        if written is None or written <= 0:
            raise OSError('short write while writing COPY spool')
        view = view[written:]


@contextmanager
def _close_preserving_primary(stream: BinaryIO, *, description: str):
    """Close on every path without letting cleanup replace a primary error.

    A close/finalization failure on the success path is fatal because the
    spool is incomplete. When the body is already failing, the close error is
    logged and the original exception remains primary.
    """
    try:
        yield stream
    except BaseException:
        try:
            stream.close()
        except BaseException:
            log.exception(
                'secondary error while closing %s; preserving primary exception',
                description,
            )
        raise
    else:
        stream.close()


class _AesGcmEncryptingRawWriter(io.RawIOBase):
    def __init__(self, raw: BinaryIO, *, key: bytes, nonce: bytes, aad: bytes):
        super().__init__()
        self._raw = raw
        self._encryptor = Cipher(
            algorithms.AES(key), modes.GCM(nonce),
        ).encryptor()
        self._encryptor.authenticate_additional_data(aad)
        self._finalized = False

    def writable(self):
        return True

    def write(self, data):
        if self.closed:
            raise ValueError('write to closed spool')
        payload = bytes(data)
        encrypted = self._encryptor.update(payload)
        if encrypted:
            _write_all(self._raw, encrypted)
        return len(payload)

    def flush(self):
        if not self.closed:
            self._raw.flush()

    def close(self):
        if self.closed:
            return
        try:
            if not self._finalized:
                tail = self._encryptor.finalize()
                if tail:
                    _write_all(self._raw, tail)
                _write_all(self._raw, _GCM_FOOTER.pack(
                    _GCM_FOOTER_MAGIC, self._encryptor.tag,
                ))
                self._raw.flush()
                self._finalized = True
        finally:
            try:
                super().close()
            finally:
                self._raw.close()


class _AesGcmDecryptingRawReader(io.RawIOBase):
    def __init__(
        self,
        raw: BinaryIO,
        *,
        key: bytes,
        nonce: bytes,
        tag: bytes,
        aad: bytes,
        ciphertext_bytes: int,
    ):
        super().__init__()
        self._raw = raw
        self._remaining = ciphertext_bytes
        self._decryptor = Cipher(
            algorithms.AES(key), modes.GCM(nonce, tag),
        ).decryptor()
        self._decryptor.authenticate_additional_data(aad)
        self._pending = bytearray()
        self._authenticated = False

    def readable(self):
        return True

    def _fill(self, minimum: int) -> None:
        while len(self._pending) < minimum and self._remaining > 0:
            chunk = self._raw.read(min(max(minimum - len(self._pending), 64 * 1024), self._remaining))
            if not chunk:
                raise SpoolFormatError(
                    f'encrypted spool ended with {self._remaining} ciphertext bytes missing'
                )
            self._remaining -= len(chunk)
            self._pending.extend(self._decryptor.update(chunk))
        if self._remaining == 0 and not self._authenticated:
            try:
                self._pending.extend(self._decryptor.finalize())
            except InvalidTag as exc:
                raise SpoolFormatError(
                    'encrypted spool authentication failed (wrong key, corruption, or truncation)'
                ) from exc
            self._authenticated = True

    def readinto(self, buffer):
        if self.closed:
            return 0
        target = memoryview(buffer).cast('B')
        if not target:
            return 0
        self._fill(len(target))
        if not self._pending:
            return 0
        count = min(len(target), len(self._pending))
        target[:count] = self._pending[:count]
        del self._pending[:count]
        return count

    def close(self):
        if self.closed:
            return
        try:
            super().close()
        finally:
            self._raw.close()


def _validate_spool_key(key: bytes | None, *, required: bool) -> bytes | None:
    if key is None:
        if required:
            raise DbPublishError('encrypted spool requires its in-memory session key')
        return None
    if not isinstance(key, bytes) or len(key) != _AES_KEY_BYTES:
        raise DbPublishError(
            f'spool encryption key must be {_AES_KEY_BYTES} bytes'
        )
    return key


def open_spool_for_write(
    directory: Path,
    *,
    stage: str,
    identity: SpoolIdentity,
    buffer_bytes: int = 1_048_576,
    encrypt: bool = True,
    key: bytes | None = None,
) -> SpoolWriteHandle:
    """Create one owned spool and return a plaintext write stream.

    By default the stream encrypts every body byte using a fresh
    AES-256-GCM nonce. The key is generated when omitted and returned only on
    the in-memory handle; it is never written to disk. With encryption
    explicitly disabled the same versioned ownership container is used, but
    the body is plaintext.
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
    if type(encrypt) is not bool:
        raise DbPublishError(f'encrypt must be bool, got {encrypt!r}')

    protection = PROTECTION_AES256_GCM if encrypt else PROTECTION_NONE
    if encrypt:
        key = _validate_spool_key(
            key if key is not None else secrets.token_bytes(_AES_KEY_BYTES),
            required=True,
        )
        nonce = secrets.token_bytes(_GCM_NONCE_BYTES)
    else:
        if key is not None:
            raise DbPublishError('plaintext spool must not be given an encryption key')
        nonce = None

    filename = compose_spool_filename(token=identity.token, stage=stage)
    path = directory / filename
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0)
    try:
        fd = os.open(path, flags, 0o600)
    except FileNotFoundError:
        # Another task may remove the shared empty default root after this
        # task resolved it but before file creation. Recreate the directory
        # once and retry the exclusive open. The same behavior is safe for
        # configured directories because resolve_spool_directory() already
        # creates them when absent; this only closes the race between those
        # two operations.
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(path, flags, 0o600)
    try:
        raw = os.fdopen(fd, 'wb', buffering=0)
    except BaseException:
        try:
            try:
                os.close(fd)
            except OSError:
                log.exception(
                    'secondary error while closing COPY spool descriptor %s; '
                    'preserving creation exception',
                    path,
                )
        finally:
            cleanup_spool_paths([path])
        raise

    stream: BinaryIO | None = None
    try:
        header_bytes = write_spool_header(
            raw,
            task=identity.task,
            target_schema=identity.target_schema,
            target_table=identity.target_table,
            run_start_utc=identity.run_start_utc,
            pid=identity.pid,
            token=identity.token,
            stage=stage,
            protection=protection,
            nonce=nonce,
        )
        if encrypt:
            protected_raw = _AesGcmEncryptingRawWriter(
                raw, key=key, nonce=nonce, aad=header_bytes,
            )
            stream = io.BufferedWriter(protected_raw, buffer_size=buffer_bytes)
        else:
            stream = io.BufferedWriter(raw, buffer_size=buffer_bytes)
    except BaseException:
        try:
            try:
                if stream is not None:
                    stream.close()
                else:
                    raw.close()
            except BaseException:
                log.exception(
                    'secondary error while closing incomplete COPY spool %s; '
                    'preserving creation exception',
                    path,
                )
        finally:
            cleanup_spool_paths([path])
        raise

    return SpoolWriteHandle(
        stream=stream,
        path=path,
        key=key,
        protection=protection,
    )


def open_spool_for_read(
    path: Path,
    *,
    identity: SpoolIdentity,
    stage: str,
    buffer_bytes: int = 1_048_576,
    key: bytes | None = None,
) -> BinaryIO:
    """Open an owned spool and expose only its plaintext body.

    Encrypted bodies are authenticated at EOF. A wrong key, corrupted body,
    missing footer, or truncated ciphertext raises ``SpoolFormatError``.
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

    raw = open(path, 'rb', buffering=0)
    try:
        header, header_bytes = _read_spool_header_with_bytes(raw)
        if header['token'] != identity.token:
            raise DbPublishError(
                f'spool at {path} header token {header["token"]!r} does not match '
                f'identity token {identity.token!r}'
            )
        if header['stage'] != stage:
            raise DbPublishError(
                f'spool at {path} header stage {header["stage"]!r} does not match '
                f'expected stage {stage!r}'
            )

        protection = header['protection']
        if protection == PROTECTION_NONE:
            if key is not None:
                raise DbPublishError('plaintext spool must not be read with an encryption key')
            return io.BufferedReader(raw, buffer_size=buffer_bytes)

        key = _validate_spool_key(key, required=True)
        nonce = bytes.fromhex(header['nonce'])
        body_start = raw.tell()
        end = raw.seek(0, os.SEEK_END)
        if end < body_start + _GCM_FOOTER.size:
            raise SpoolFormatError('encrypted spool is missing its authentication footer')
        raw.seek(end - _GCM_FOOTER.size)
        footer = raw.read(_GCM_FOOTER.size)
        if len(footer) != _GCM_FOOTER.size:
            raise SpoolFormatError('short read on encrypted spool footer')
        footer_magic, tag = _GCM_FOOTER.unpack(footer)
        if footer_magic != _GCM_FOOTER_MAGIC:
            raise SpoolFormatError(
                f'wrong encrypted spool footer magic: {footer_magic!r}'
            )
        ciphertext_bytes = end - body_start - _GCM_FOOTER.size
        raw.seek(body_start)
        decrypting_raw = _AesGcmDecryptingRawReader(
            raw,
            key=key,
            nonce=nonce,
            tag=tag,
            aad=header_bytes,
            ciphertext_bytes=ciphertext_bytes,
        )
        return io.BufferedReader(decrypting_raw, buffer_size=buffer_bytes)
    except BaseException:
        raw.close()
        raise


def cleanup_spool_paths(
    paths: Sequence[Path],
    *,
    attempts: int = 3,
    retry_delay_seconds: float = 0.05,
) -> list[Path]:
    """Best-effort current-run cleanup with bounded retries and logging.

    Already-missing paths count as success. Every residual path is returned
    and logged exactly; callers decide whether residue is fatal at that point
    in the lifecycle.
    """
    if type(attempts) is not int or attempts < 1:
        raise DbPublishError(f'attempts must be a positive integer, got {attempts!r}')
    if not isinstance(retry_delay_seconds, (int, float)) or retry_delay_seconds < 0:
        raise DbPublishError(
            f'retry_delay_seconds must be non-negative, got {retry_delay_seconds!r}'
        )

    failed: list[Path] = []
    for path in paths:
        removed = False
        for attempt in range(1, attempts + 1):
            try:
                path.unlink()
            except FileNotFoundError:
                removed = True
                break
            except OSError as exc:
                if attempt == attempts:
                    log.warning(
                        'could not remove COPY spool %s after %s attempt(s): %s',
                        path, attempts, exc,
                    )
                    break
                if retry_delay_seconds:
                    time.sleep(retry_delay_seconds)
            else:
                removed = True
                break
        if not removed:
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

    Returns (deleted, preserved) as sorted lists of Paths. Failure to
    remove a positively identified predecessor spool after bounded retries
    is fatal: continuing would knowingly leave task data behind and allow
    another spool to be created beside it.

    The directory need not exist; a first-run task has nothing to clean.
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

    owned: list[Path] = []
    preserved: list[Path] = []
    for entry in sorted(directory.iterdir()):
        if not entry.is_file():
            # Subdirectory, symlink to a directory, device node, ...
            # never ours.
            preserved.append(entry)
            continue
        parsed_name = parse_spool_filename(entry.name)
        if parsed_name is None:
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
        if (
            header.get('token') != parsed_name['token']
            or header.get('stage') != parsed_name['stage']
        ):
            # A valid header attached to a filename for a different run or
            # stage is not positive ownership. Preserve rather than guess.
            preserved.append(entry)
            continue
        if header.get('task') != task:
            # Foreign task. Preserve.
            preserved.append(entry)
            continue
        owned.append(entry)

    failed = cleanup_spool_paths(owned)
    failed_set = set(failed)
    deleted = [path for path in owned if path not in failed_set]
    if failed:
        residual = ', '.join(str(path) for path in failed)
        raise DbPublishError(
            f'could not remove predecessor COPY spool(s) owned by task '
            f'{task!r}: {residual}. Refusing to continue while known task '
            f'data remains in the spool directory.'
        )
    return (deleted, preserved)


# --- DBAPI COPY transport ----------------------------------------------


def _build_copy_sql(conn, staging_table, columns: Sequence[ResolvedColumn]) -> str:
    """Build one defensively quoted PostgreSQL COPY FROM STDIN statement.

    The serializer writes text rows with tab delimiters and ``\\N`` as the
    NULL marker. The SQL must describe that exact grammar; changing one side
    without the other would silently corrupt values rather than merely reduce
    performance.
    """
    dialect = getattr(conn, 'dialect', None)
    if dialect is None or getattr(dialect, 'name', None) != 'postgresql':
        raise DbPublishError(
            'COPY loader requires a SQLAlchemy PostgreSQL connection'
        )
    if getattr(dialect, 'driver', None) not in (None, 'psycopg2'):
        raise DbPublishError(
            f"COPY loader requires SQLAlchemy's psycopg2 dialect, got "
            f"{getattr(dialect, 'driver', None)!r}"
        )

    preparer = dialect.identifier_preparer
    table_sql = preparer.format_table(staging_table)
    column_sql = ', '.join(preparer.quote(column.name) for column in columns)
    # SQL text E'\\\\N' represents the two-character COPY NULL marker
    # backslash + N. The input serializer emits that marker only for None;
    # literal text ``\\N`` is escaped as ``\\\\N`` in the row body.
    return (
        f'COPY {table_sql} ({column_sql}) FROM STDIN '
        "WITH (FORMAT text, DELIMITER E'\\t', NULL E'\\\\N', ENCODING 'UTF8')"
    )


def _get_driver_connection(conn):
    """Return the existing psycopg2 connection behind SQLAlchemy.

    No engine or second connection is created. SQLAlchemy 2.x exposes the
    DBAPI connection as ``Connection.connection.driver_connection``. The
    legacy ``.connection`` fallback keeps the helper usable with older test
    doubles and SQLAlchemy 2.0 proxy shapes without importing psycopg2.
    """
    proxy = getattr(conn, 'connection', None)
    if proxy is None:
        raise DbPublishError(
            'COPY loader could not access the SQLAlchemy DBAPI connection'
        )
    raw = getattr(proxy, 'driver_connection', None)
    if raw is None:
        raw = getattr(proxy, 'connection', None)
    if raw is None or not callable(getattr(raw, 'cursor', None)):
        raise DbPublishError(
            'COPY loader could not access the existing psycopg2 connection'
        )
    return raw


def load_copy_into_staging(conn, staging_table, prepared, _chunk_size=None) -> int:
    """Stream one prepared COPY-text spool into an existing staging table.

    The caller owns the SQLAlchemy transaction, staging DDL, verification,
    commit/rollback and spool cleanup. This function opens only a cursor on
    the existing DBAPI connection and returns the exact logical row count
    captured during spool preparation.
    """
    if not isinstance(prepared, PreparedCopySource):
        raise DbPublishError(
            f'COPY loader requires PreparedCopySource, got '
            f'{type(prepared).__name__}'
        )
    copy_sql = _build_copy_sql(conn, staging_table, prepared.columns)
    raw = _get_driver_connection(conn)
    cursor = raw.cursor()
    primary_error = None
    try:
        with prepared.open_reader() as reader:
            # psycopg2 copy_expert() pulls from the file-like object in bounded
            # chunks. Reading through authenticated EOF is what verifies the
            # final AES-GCM tag; no decrypted temporary file is created.
            cursor.copy_expert(copy_sql, reader, size=prepared.buffer_bytes)
        return prepared.row_count
    except BaseException as exc:
        primary_error = exc
        # Bypassing SQLAlchemy's execute() layer means a psycopg2 exception is
        # not automatically classified by the dialect or used to invalidate
        # the SQLAlchemy Connection. Ask the dialect as well as checking the
        # driver's closed flag: psycopg2 does not set ``closed`` for every
        # disconnect shape. Invalidating here preserves DbPublisher's fatal
        # no-reconnect rule for the advisory-lock-owning session.
        disconnected = bool(getattr(raw, 'closed', 0))
        if not disconnected and isinstance(exc, Exception):
            is_disconnect = getattr(conn.dialect, 'is_disconnect', None)
            if callable(is_disconnect):
                try:
                    disconnected = bool(is_disconnect(exc, raw, cursor))
                except BaseException:
                    log.exception(
                        'secondary error while classifying a COPY connection '
                        'failure; preserving primary exception'
                    )
        if disconnected:
            invalidate = getattr(conn, 'invalidate', None)
            if callable(invalidate):
                try:
                    invalidate(exc)
                except BaseException:
                    log.exception(
                        'secondary error while invalidating a lost COPY '
                        'connection; preserving primary exception'
                    )
        raise
    finally:
        try:
            cursor.close()
        except BaseException:
            if primary_error is None:
                raise
            log.exception(
                'secondary error while closing psycopg2 COPY cursor; '
                'preserving primary exception'
            )


def _normalize_copy_row(
    row: Sequence[Any],
    *,
    expected_width: int,
) -> tuple[Any, ...]:
    if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
        raise DbPublishError(
            f'row source yielded non-sequence {type(row).__name__}'
        )
    normalized = tuple(_normalize_value(value) for value in row)
    if len(normalized) != expected_width:
        raise DbPublishError(
            f'row width {len(normalized)} does not match column '
            f'count {expected_width}'
        )
    return normalized


def _prepare_declared_copy_source_one_pass(
    *,
    row_source: Iterable[Sequence[Any]],
    source_columns: tuple[str, ...],
    resolved_columns: tuple[ResolvedColumn, ...],
    identity: SpoolIdentity,
    directory: Path,
    policy: CopyLoadPolicy,
) -> PreparedCopySource:
    """Validate and serialize a declared COPY payload in one traversal.

    A declared schema already supplies the target types and wire order, so a
    type-neutral spool would only add a full write/read cycle. The final
    COPY-text spool is therefore written directly while each normalized row
    is validated against the shared declared-value kernel.
    """
    copytext_path: Path | None = None
    try:
        serializers = _compile_declared_copy_field_writers(
            source_columns,
            resolved_columns,
            identity.target_table,
        )
        handle = open_spool_for_write(
            directory,
            stage='copytext',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            encrypt=policy.encrypt_spools,
        )
        copytext_path = handle.path
        row_count = 0
        output_buffer = bytearray()
        with _close_preserving_primary(
            handle.stream,
            description=f'COPY-text spool {copytext_path}',
        ) as copytext_fp:
            for row in row_source:
                row_count += 1
                _write_compiled_declared_copytext_row(
                    copytext_fp,
                    row,
                    serializers,
                    row_count,
                    output_buffer,
                    expected_width=len(source_columns),
                )
        return PreparedCopySource(
            path=copytext_path,
            columns=resolved_columns,
            row_count=row_count,
            spool_bytes=copytext_path.stat().st_size,
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            protection=handle.protection,
            _key=handle.key,
        )
    except BaseException:
        if copytext_path is not None:
            cleanup_spool_paths([copytext_path])
        cleanup_default_spool_directory(policy)
        raise


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
    type_overrides: Mapping[str, Any] | None = None,
    not_null_columns: Sequence[str] = (),
) -> PreparedCopySource:
    """Prepare a final COPY-text spool from a positional row source.

    Declared mode knows the target schema before traversal and therefore
    validates and serializes directly into the final spool in one pass.
    Inferred mode writes a type-neutral spool while accumulating schema state,
    then replays it once through the resolved target-aware serializer.

    `row_source` must yield sequences whose positional order matches
    `columns`. Each source value is normalized once through the same kernel as
    INSERT. Declared and source column sets must match, but their order may
    differ; a positional serializer compiled once before the row loop emits
    fields in resolved-schema order without rebuilding per-row dictionaries.

    `framework_columns` pins the resolved type and nullability of technical
    columns whose value is caller-supplied and constant (e.g.
    `etl_updated_at`) after inference completes. Inferred user columns now
    distinguish naive from timezone-aware datetimes directly; framework
    columns remain pinned because their contract is framework-owned rather
    than inferred from task data. In declared mode this override is a no-op
    (declared columns already carry their pinned type), but the same
    framework tuple is accepted and validated for symmetry with the caller.

    On success: returns an immutable `PreparedCopySource` carrying the final
    spool, resolved columns, exact row count and on-disk byte count. An
    inferred-mode neutral spool is removed before success is returned. On any
    exception, deletion of every current-run spool is attempted with bounded
    retries; a cleanup failure is logged without replacing the primary
    exception and may be reaped by a later positively-owned cleanup pass.

    Since 0.6.6 this preparation result is consumed by the same-connection
    psycopg2 COPY transport. The caller still owns transaction boundaries,
    staging DDL, publication and final spool cleanup.
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
    # Duplicate names would make source-position lookup ambiguous and could
    # route a resolved output column to the wrong positional value. The
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
        declared_duplicates = find_duplicates(declared_names)
        if declared_duplicates:
            raise DbPublishError(
                f'declared_schema contains duplicate column names: {declared_duplicates!r}'
            )
        missing = [name for name in declared_names if name not in columns_tuple]
        unexpected = [name for name in columns_tuple if name not in set(declared_names)]
        if missing or unexpected:
            raise DbPublishError(
                f'declared_schema columns do not match source columns; '
                f'missing={missing!r}, unexpected={unexpected!r}'
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
    if declared_schema is not None and type_overrides is not None:
        raise DbPublishError('declared_schema cannot be combined with type_overrides')
    if declared_schema is not None and tuple(not_null_columns):
        raise DbPublishError('declared_schema cannot be combined with not_null_columns')

    if type_overrides is not None and not isinstance(type_overrides, Mapping):
        raise DbPublishError(
            f'type_overrides must be a mapping or None, got {type(type_overrides).__name__}'
        )
    overrides = dict(type_overrides or {})
    unknown_overrides = [name for name in overrides if name not in columns_tuple]
    if unknown_overrides:
        raise DbPublishError(
            f'type_overrides contains column(s) not present in the output: {unknown_overrides!r}'
        )

    if not isinstance(not_null_columns, Sequence) or isinstance(
        not_null_columns, (str, bytes)
    ):
        raise DbPublishError('not_null_columns must be a sequence of strings')
    not_null = tuple(not_null_columns)
    if not all(isinstance(name, str) for name in not_null):
        raise DbPublishError('not_null_columns must contain only strings')
    not_null_duplicates = find_duplicates(not_null)
    if not_null_duplicates:
        raise DbPublishError(
            f'not_null_columns contains duplicate column(s): {not_null_duplicates!r}'
        )
    unknown_not_null = [name for name in not_null if name not in columns_tuple]
    if unknown_not_null:
        raise DbPublishError(
            f'not_null_columns contains column(s) not present in the output: {unknown_not_null!r}'
        )

    if row_source is None:
        raise DbPublishError('row_source must not be None')

    # --- Execute the selected preparation path -----------------------

    if declared_schema is not None:
        return _prepare_declared_copy_source_one_pass(
            row_source=row_source,
            source_columns=columns_tuple,
            resolved_columns=declared_tuple,
            identity=identity,
            directory=directory,
            policy=policy,
        )

    # Inferred mode still needs two passes: the target schema is unknown
    # until every sampled/observed family has been resolved. Pass 1 writes
    # normalized values to the neutral spool while feeding inference. Pass 2
    # uses a positional serializer compiled once from the resolved schema.
    neutral_path: Path | None = None
    copytext_path: Path | None = None
    try:
        state = _InferenceStreamState(len(columns_tuple))
        source_row_count = 0
        neutral_handle = open_spool_for_write(
            directory,
            stage='neutral',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            encrypt=policy.encrypt_spools,
        )
        neutral_path = neutral_handle.path
        with _close_preserving_primary(
            neutral_handle.stream,
            description=f'neutral COPY spool {neutral_path}',
        ) as neutral_fp:
            write_neutral_preamble(neutral_fp, columns=columns_tuple)
            for row in row_source:
                values = _normalize_copy_row(
                    row,
                    expected_width=len(columns_tuple),
                )
                source_row_count += 1
                state.feed_row(values)
                write_neutral_row(
                    neutral_fp,
                    values,
                    expected_width=len(columns_tuple),
                )
            write_neutral_terminator(neutral_fp)

        resolved_types = state.resolve()
        framework_by_name = {c.name: c for c in framework_tuple}
        resolved_list = []
        for index, name in enumerate(columns_tuple):
            framework = framework_by_name.get(name)
            if framework is not None:
                resolved_list.append(framework)
                continue
            override = _resolve_override(overrides.get(name))
            resolved_list.append(ResolvedColumn(
                name=name,
                type=override if override is not None else resolved_types[index],
                nullable=name not in not_null,
            ))
        resolved_columns = tuple(resolved_list)
        serializers = _compile_inferred_copy_field_serializers(
            columns_tuple,
            resolved_columns,
            identity.target_table,
        )

        copytext_handle = open_spool_for_write(
            directory,
            stage='copytext',
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            encrypt=policy.encrypt_spools,
        )
        copytext_path = copytext_handle.path
        output_buffer = bytearray()
        with _close_preserving_primary(
            copytext_handle.stream,
            description=f'COPY-text spool {copytext_path}',
        ) as copytext_fp:
            neutral_read = open_spool_for_read(
                neutral_path,
                identity=identity,
                stage='neutral',
                buffer_bytes=policy.buffer_bytes,
                key=neutral_handle.key,
            )
            with _close_preserving_primary(
                neutral_read,
                description=f'neutral COPY spool reader {neutral_path}',
            ):
                preamble = read_neutral_preamble(neutral_read)
                if preamble != columns_tuple:
                    raise DbPublishError(
                        f'neutral spool preamble columns {preamble!r} do '
                        f'not match expected {columns_tuple!r}'
                    )
                row_number = 0
                while True:
                    values = read_neutral_row(
                        neutral_read,
                        len(columns_tuple),
                    )
                    if values is None:
                        break
                    row_number += 1
                    _write_compiled_inferred_copytext_row(
                        copytext_fp,
                        values,
                        serializers,
                        row_number,
                        output_buffer,
                    )

        failed_neutral_cleanup = cleanup_spool_paths([neutral_path])
        if failed_neutral_cleanup:
            raise DbPublishError(
                f'could not remove completed neutral COPY spool(s): '
                f'{failed_neutral_cleanup!r}'
            )
        return PreparedCopySource(
            path=copytext_path,
            columns=resolved_columns,
            row_count=source_row_count,
            spool_bytes=copytext_path.stat().st_size,
            identity=identity,
            buffer_bytes=policy.buffer_bytes,
            protection=copytext_handle.protection,
            _key=copytext_handle.key,
        )
    except BaseException:
        to_cleanup = [p for p in (neutral_path, copytext_path) if p is not None]
        if to_cleanup:
            cleanup_spool_paths(to_cleanup)
        cleanup_default_spool_directory(policy)
        raise


__all__ = [
    'CopyLoadPolicy',
    'SpoolFormatError',
    'MAGIC',
    'FORMAT_VERSION',
    'SPOOL_STAGES',
    'PROTECTION_NONE',
    'PROTECTION_AES256_GCM',
    'SPOOL_PROTECTIONS',
    'SPOOL_FILENAME_RE',
    'DEFAULT_SPOOL_SUBDIR',
    'PreparedCopySource',
    'SpoolIdentity',
    'compose_ownership_token',
    'compose_spool_filename',
    'parse_spool_filename',
    'write_spool_header',
    'read_spool_header',
    'resolve_spool_directory',
    'cleanup_default_spool_directory',
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
    'load_copy_into_staging',
    'prepare_copy_source',
]
