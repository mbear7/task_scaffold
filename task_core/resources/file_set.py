# -*- coding: utf-8 -*-
"""
Level 2: multi-file/folder resource. Depends on file_access.py (level 1)
and source_tracking.py (level 1) for fingerprint construction, same
layering as resources/excel.py.

select_file_infos() already returns every matching file in a folder --
this module does not duplicate that selection logic, it wraps it: a
public resource wrapper, deterministic sorting (filesystem iteration
order isn't stable across runs), and aggregate fingerprint generation.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PureWindowsPath

from task_core.excel_metadata import read_excel_row_metadata as _read_excel_row_metadata

from task_core.types import SourceCheckError
from task_core.source_tracking import (
    SourceFileMeta,
    SourceFingerprint,
    make_source_signature,
)
from task_core.file_access import NoMatchingFilesError, SelectedFile, _resolve_source_access


@dataclass(frozen=True)
class _FileSetSelection:
    # tuple, not list -- matching file_set_resource.files's own
    # immutability. A frozen dataclass whose one real field is a mutable
    # list isn't actually frozen where it matters.
    root_path: str
    include_mask: str
    recursive: bool
    selected_files: tuple[SelectedFile, ...]


class file_set_resource:
    def __init__(self, files, *, source_access, buffered=False, selection=None):
        self.files = tuple(files)   # immutable -- a pipeline holding this
                                     # reference can't accidentally reorder
                                     # or mutate it after construction
        self._source_access = _resolve_source_access(source_access)
        self._buffered = buffered
        self._selection = selection
        self._row_metadata_cache = {}

    def open_file(self, selected_file):
        if selected_file not in self.files:
            raise ValueError(
                f'{selected_file!r} is not one of this file_set_resource\'s '
                'own selected files -- did it come from a different resource?'
            )
        return self._source_access.open_binary(selected_file.path, buffered=self._buffered)

    def read_excel_row_metadata(self, selected_file, *, sheet=0, mode='outline', column=None):
        # Same shape and same explicit-opt-in reasoning as
        # excel_resource.read_excel_row_metadata() -- this is the half of
        # phase 2 that actually serves a file-set pipeline (ssch), which
        # never constructs an excel_resource at all.
        #
        # Cached by (selected_file, sheet, mode, column) -- selected_file
        # confirmed directly to be hashable (a frozen dataclass) and safe
        # as a dict key -- for the same reason excel_resource's own
        # version is cached: every file in self.files is immutable for
        # this resource's whole lifetime, so the same key always means
        # the same answer, removing a real, easy-to-forget cost -- a
        # second full network read on every call otherwise, not just
        # the first.
        key = (selected_file, sheet, mode, column)
        if key not in self._row_metadata_cache:
            with self.open_file(selected_file) as src:
                self._row_metadata_cache[key] = _read_excel_row_metadata(src, sheet=sheet, mode=mode, column=column)
        # A copy, not the internal cache dict directly -- same fix,
        # same reason, as excel_resource's own version above.
        return dict(self._row_metadata_cache[key])

    def source_fingerprint(self, source_key):
        if self._selection is None:
            raise SourceCheckError(
                'file_set_resource was not built with source-selection metadata; '
                'source_fingerprint() is only supported for resources built via '
                'build_file_set_resource()'
            )

        sel = self._selection
        # Reads sel.selected_files directly -- the exact same tuple this
        # resource's own .files is built from, not a second,
        # independently-computed selection. If fingerprinting re-queried
        # the folder separately, a file added or removed between the two
        # calls would mean the fingerprint no longer actually describes
        # what the pipeline processed.
        file_metas = [
            SourceFileMeta(
                relative_path=f.relative_path,
                full_path=f.path,
                size_bytes=f.stat_result.st_size,
                modified_at_utc=datetime.fromtimestamp(f.stat_result.st_mtime, tz=timezone.utc),
            )
            for f in sel.selected_files
        ]

        return SourceFingerprint(
            source_key=source_key,
            source_kind='file_set',
            root_path=sel.root_path,
            include_mask=sel.include_mask,
            recursive=sel.recursive,
            file_count=len(file_metas),
            total_size_bytes=sum(m.size_bytes for m in file_metas),
            max_modified_at_utc=max((m.modified_at_utc for m in file_metas), default=None),
            # A list of per-file dicts, in the same order as sel.selected_files
            # (item 2's deterministic sort) -- make_source_signature's
            # json.dumps(sort_keys=True) stabilizes dict-key order within
            # each entry but never reorders the list itself, so an unstable
            # file order here means an unstable signature and spurious
            # "source changed" reruns, independent of whatever
            # reproducible-combining reason motivated the sort in the
            # first place.
            source_signature=make_source_signature([m.to_signature_dict() for m in file_metas]),
            source_snapshot=[m.to_snapshot_dict() for m in file_metas],
            store_snapshot=True,  # file metadata, not query results -- fine to persist
        )


def build_file_set_resource(
    folder_path,
    pattern='*',
    *,
    include_hidden=False,
    include_system=False,
    include_temp=False,
    min_age_seconds=None,
    recursive=False,
    source_access=None,
    buffered=False,
    on_empty='raise',
):
    if on_empty not in ('raise', 'empty'):
        raise ValueError(f"on_empty must be 'raise' or 'empty', got {on_empty!r}")

    source_access = _resolve_source_access(source_access)

    try:
        file_infos = source_access.select_file_infos(
            folder_path,
            pattern=pattern,
            include_hidden=include_hidden,
            include_system=include_system,
            include_temp=include_temp,
            min_age_seconds=min_age_seconds,
            recursive=recursive,
        )
    except NoMatchingFilesError:
        # 'raise' (default) needs no special handling at all -- this is
        # already select_file_infos()'s own behavior, so just let it
        # propagate. Only 'empty' converts it. A plain FileNotFoundError
        # (the folder itself doesn't exist) is a different exception
        # class and is NOT caught here -- it always propagates,
        # regardless of on_empty, since that's a misconfiguration
        # 'empty' was never meant to tolerate.
        if on_empty == 'empty':
            file_infos = []
        else:
            raise

    # Sort key: PureWindowsPath for how the actual filesystem treats file
    # identity (case-insensitive on Windows/SMB, matching how
    # SelectedFile.relative_path is always backslash-joined for the SMB
    # branch regardless of client OS), with a raw-string tie-breaker --
    # two files differing only by case compare equal under
    # case-insensitive PureWindowsPath, and Python's stable sort would
    # otherwise fall back to non-deterministic filesystem discovery order
    # for that pair.
    sorted_files = tuple(
        sorted(file_infos, key=lambda f: (PureWindowsPath(f.relative_path), f.relative_path))
    )

    selection = _FileSetSelection(
        root_path=str(folder_path),
        include_mask=pattern,
        recursive=recursive,
        selected_files=sorted_files,
    )

    return file_set_resource(
        sorted_files,
        source_access=source_access,
        buffered=buffered,
        selection=selection,
    )
