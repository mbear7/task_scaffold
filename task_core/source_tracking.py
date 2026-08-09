"""
Level 1: source-change-tracking dataclasses and helpers. Depends only
on task_core.types (for SourceCheckError).
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from task_core.types import SourceCheckError


@dataclass(frozen=True)
class SourceFileMeta:
    relative_path: str
    full_path: str
    size_bytes: int
    modified_at_utc: datetime

    def to_signature_dict(self):
        # Deliberately excludes full_path: signatures should be based on
        # stable relative paths inside the source root (see SMB/DFS support
        # notes), so a DFS root/mount change doesn't look like a source change.
        return {
            'relative_path': self.relative_path,
            'size_bytes': self.size_bytes,
            'modified_at_utc': self.modified_at_utc.isoformat(),
        }

    def to_snapshot_dict(self):
        # Full metadata, including full_path, for storage/debugging in
        # source_snapshot. Not used for signature calculation.
        return {**self.to_signature_dict(), 'full_path': self.full_path}


@dataclass(frozen=True)
class SourceFingerprint:
    source_key: str
    source_kind: str
    root_path: str | None
    include_mask: str | None
    recursive: bool
    file_count: int
    total_size_bytes: int
    max_modified_at_utc: datetime | None
    source_signature: str
    source_snapshot: list[dict[str, Any]] | dict[str, Any]
    # Per-fingerprint say in whether its source_snapshot may be persisted,
    # on top of (not instead of) SourceChangeCheckConfig.store_snapshot's
    # global kill switch -- see TrackedDbQuerySource for why db_query
    # snapshots default this to False while file-based ones default True.
    store_snapshot: bool = True


# MD5 is acceptable here: this is a non-security change-detection digest
# over file metadata, not a security hash.
def file_meta_from_selected(selected_file):
    """SourceFileMeta from a SelectedFile.

    Duck-typed on the three attributes it reads rather than importing
    file_access.SelectedFile: that module is the same level as this one,
    and this module deliberately depends on nothing but types.py.
    """
    return SourceFileMeta(
        relative_path=selected_file.relative_path,
        full_path=selected_file.path,
        size_bytes=selected_file.stat_result.st_size,
        modified_at_utc=datetime.fromtimestamp(
            selected_file.stat_result.st_mtime, tz=timezone.utc,
        ),
    )


def single_file_fingerprint(
    source_key, *, source_kind, root_path, include_mask, recursive,
    selected_file,
):
    """The fingerprint of exactly one selected file.

    Shared by every single-file resource -- workbooks and CSV alike -- so
    that the three of them cannot drift into describing the same selection
    differently. The expression was already duplicated once between
    resources/excel.py and its latest-file builder before 0.7.7; a third
    copy for CSV is what this exists to prevent.
    """
    meta = file_meta_from_selected(selected_file)
    return SourceFingerprint(
        source_key=source_key,
        source_kind=source_kind,
        root_path=root_path,
        include_mask=include_mask,
        recursive=recursive,
        file_count=1,
        total_size_bytes=meta.size_bytes,
        max_modified_at_utc=meta.modified_at_utc,
        source_signature=make_source_signature([meta.to_signature_dict()]),
        source_snapshot=[meta.to_snapshot_dict()],
        # File metadata, not query results -- fine to persist.
        store_snapshot=True,
    )


def make_source_signature(payload):
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()


@dataclass(frozen=True, kw_only=True)
class SourceChangeCheckConfig:
    enabled: bool = False
    schema: str = 'bsr'
    table: str = 'task_scaffold_meta'
    create_if_missing: bool = True
    store_snapshot: bool = True

    def __post_init__(self):
        if not isinstance(self.enabled, bool):
            raise TypeError('enabled must be bool')
        if not isinstance(self.schema, str) or not self.schema:
            raise TypeError('schema must be a non-empty str')
        if not isinstance(self.table, str) or not self.table:
            raise TypeError('table must be a non-empty str')
        if not isinstance(self.create_if_missing, bool):
            raise TypeError('create_if_missing must be bool')
        if not isinstance(self.store_snapshot, bool):
            raise TypeError('store_snapshot must be bool')


# Example:
#
# SOURCE_CHANGE_CHECK = SourceChangeCheckConfig(
#     enabled=True,
#     schema='bsr',
#     table='task_scaffold_meta',
# )
#
# run_pipelines(
#     ...
#     source_change_check=SOURCE_CHANGE_CHECK,
# )


@dataclass(frozen=True)
class TrackedResourceSource:
    # One tracked resource source maps to one task_context loader key.
    # resource_key doubles as the metadata table's
    # source_key. TrackedDbQuerySource (below) separates the two, for
    # sources fingerprinted via a DB query rather than a resource object.
    resource_key: str

    @property
    def source_key(self):
        return self.resource_key


@dataclass(frozen=True)
class TrackedDbQuerySource:
    # Not wired into any task yet -- this exists so the mechanism is
    # available once a real fingerprint query (and, ideally, a
    # supporting index) is agreed with the owner of the source view.
    # Query design guidance: prefer a max(updated_at)-style watermark
    # (a single scalar reflecting the most recent change) over selecting
    # and hashing the full result set, since a watermark is cheap to
    # compute repeatedly and doesn't require deciding what "the data"
    # even means for a large or frequently-changing view.
    source_key: str
    resource_key: str
    query: str
    # Unlike file metadata (filename/size/mtime -- essentially never
    # sensitive), a DB query's result is your actual data, which may or may
    # not be safe to persist in a technical scratch table. Default to
    # hash-only (signature stored, source_snapshot NULL) unless a task
    # explicitly opts a specific query in with store_snapshot=True, e.g.
    # because it knows the result is just an innocuous timestamp.
    store_snapshot: bool | None = None

    def __post_init__(self):
        if not isinstance(self.source_key, str) or not self.source_key:
            raise TypeError('source_key must be a non-empty str')
        if not isinstance(self.resource_key, str) or not self.resource_key:
            raise TypeError('resource_key must be a non-empty str')
        if not isinstance(self.query, str) or not self.query.strip():
            raise TypeError('query must be a non-empty str')
        if self.store_snapshot is not None and not isinstance(self.store_snapshot, bool):
            raise TypeError('store_snapshot must be bool or None')


def _json_safe_scalar(value, *, context):
    # The query contract asks for JSON-serializable scalars, but the most
    # natural fingerprint query (max(updated_at)) returns a datetime/date,
    # and numeric aggregates can come back as Decimal -- neither is directly
    # JSON-serializable. Handle those common DB scalar types here instead of
    # pushing every task author to cast them in SQL; anything else fails
    # clearly rather than being silently coerced.
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        # str(), not float(): a signature must not lose precision that
        # could hide a real change.
        return str(value)
    raise SourceCheckError(
        f'{context}: query result value of type {type(value).__name__!r} is not JSON-serializable; '
        'adjust the query to return text/numeric/boolean/timestamp scalars'
    )
