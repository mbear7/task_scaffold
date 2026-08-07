"""Spool file lifecycle: handles, encryption, reading and cleanup.

Split out of copy.py in 0.7.4. Everything that touches the filesystem is
here -- exclusive creation, the AES-256-GCM streams that protect spool
bodies by default, the bounded decrypting reader COPY pulls from, and the
cleanup that removes current-run and positively-owned predecessor spools.

Filesystem failures keep their native type; see ADR 0011 §Filesystem
failures keep their own type. The one deliberate exception is predecessor
cleanup, which converts a failed unlink into a returned residual path and
then refuses to continue -- a decision, not a restatement of the errno.
"""

from __future__ import annotations

import io
import logging
import os
import secrets
import time
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from task_core.db.spool_format import (
    _AES_KEY_BYTES,
    _GCM_FOOTER,
    _GCM_FOOTER_MAGIC,
    _GCM_NONCE_BYTES,
    PROTECTION_AES256_GCM,
    PROTECTION_NONE,
    SPOOL_STAGES,
    SpoolFormatError,
    _read_spool_header_with_bytes,
    _write_all,
    compose_ownership_token,
    compose_spool_filename,
    parse_spool_filename,
    read_spool_header,
    write_spool_header,
)
from task_core.db.values import DbPublishError, ResolvedColumn

log = logging.getLogger(__name__)

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
        # The spool root is gone between resolution and creation. Something
        # outside task_core removed it -- a tmp reaper, an operator, an
        # unrelated cleanup script -- because task_core does not remove that
        # root. One recreate is enough against that: a loop would only turn a
        # fast failure into a slow one if something is deleting continuously.
        #
        # Nothing is caught around the retry. Filesystem failures keep their
        # native type; see ADR 0011 §Filesystem failures keep their own type.
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
