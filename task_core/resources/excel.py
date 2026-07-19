# -*- coding: utf-8 -*-
"""
Level 2: Excel resource. Depends on file_access.py (level 1) and
source_tracking.py (level 1) for fingerprint construction, and
openpyxl_compat.py (level 1) for warning suppression.
"""

from datetime import datetime, timezone

from openpyxl.utils.cell import range_boundaries

import petl as etl

from task_core.excel_metadata import read_excel_row_metadata as _read_excel_row_metadata

from task_core.types import SourceCheckError
from task_core.source_tracking import (
    SourceFileMeta,
    SourceFingerprint,
    make_source_signature,
)
from task_core.file_access import _resolve_source_access, _ResourceSelection
from task_core.openpyxl_compat import suppress_openpyxl_data_validation_warning


def load_table(wb, table):
    """Extract an Excel Table's data range as a petl table. Confirmed
    correct against the real, original implementation (previously lived
    in petl_util, which task_core no longer depends on at all -- this is
    a verbatim port, not a reconstruction)."""
    ws = wb[table['sheet']]
    min_col, min_row, max_col, max_row = range_boundaries(table['range_string'])
    rows = list(ws.iter_rows(
        min_row=min_row, max_row=max_row,
        min_col=min_col, max_col=max_col,
        values_only=True,
    ))
    header = rows[0]
    data = rows[1:]
    return etl.wrap([header, *data])


def tbl2dict(wb, table, cols=(0, 1)):
    """Build a dict from two columns of an Excel Table's data range,
    keyed by the first, deduplicated (first occurrence wins). Confirmed
    correct against the real, original implementation, same provenance
    as load_table() above."""
    ws = wb[table['sheet']]
    min_col, min_row, max_col, max_row = range_boundaries(table['range_string'])
    rows = ws.iter_rows(
        min_row=min_row + 1, max_row=max_row,  # +1: skip the header row
        min_col=min_col, max_col=max_col,
        values_only=True,
    )
    c_from, c_to = cols
    seen = set()
    return {r[c_from]: r[c_to] for r in rows if r[c_from] not in seen and not seen.add(r[c_from])}


class excel_resource:
    def __init__(self, file_path, sheets, tables, *, source_access=None, excel_buffered=False, selection=None):
        self.file_path = str(file_path)
        self.sheets = sheets
        self.tables = tables
        self._source_access = _resolve_source_access(source_access)
        self._excel_buffered = excel_buffered
        self._selection = selection
        self._workbook_cm = None
        self._wb = None
        self._table_cache = {}
        self._map_cache = {}
        self._range_cache = {}
        self._sheet_cache = {}

    def source_fingerprint(self, source_key):
        if self._selection is None:
            raise SourceCheckError(
                f'excel_resource for {self.file_path!r} was not built with source-selection '
                'metadata; source_fingerprint() is only supported for resources built via '
                'build_latest_xlsx_resource()'
            )

        sel = self._selection
        f = sel.selected_file
        file_meta = SourceFileMeta(
            relative_path=f.relative_path,
            full_path=f.path,
            size_bytes=f.stat_result.st_size,
            modified_at_utc=datetime.fromtimestamp(f.stat_result.st_mtime, tz=timezone.utc),
        )

        return SourceFingerprint(
            source_key=source_key,
            source_kind=sel.source_kind,
            root_path=sel.root_path,
            include_mask=sel.include_mask,
            recursive=sel.recursive,
            file_count=1,
            total_size_bytes=file_meta.size_bytes,
            max_modified_at_utc=file_meta.modified_at_utc,
            source_signature=make_source_signature([file_meta.to_signature_dict()]),
            source_snapshot=[file_meta.to_snapshot_dict()],
            store_snapshot=True,  # file metadata, not query results -- fine to persist
        )

    def read_excel_row_metadata(self, sheet=0, mode='outline', column=None):
        # Explicit opt-in only -- never called automatically, not part of
        # build_excel_resource() or get_table(), not inferred from
        # anything. A pipeline calls this itself, from inside its own
        # .run(ctx), the same way TrackedDbQuerySource's query is the
        # task author's decision, not scaffold inference. Returns the
        # raw {row_number: value} mapping; alignment against a specific
        # materialized table's row range (task_core.align_row_metadata,
        # in excel_metadata.py) stays the pipeline author's own call,
        # since only the pipeline knows how its own table's rows
        # correspond to XLSX row numbers after any filtering it does of
        # its own.
        #
        # Opens its own handle via open_binary(), independent of whatever
        # handle this resource may already be holding open for its normal
        # data reads (in SMB non-buffered mode, that handle stays open
        # for the resource's whole lifetime). SMB read-sharing means this
        # is safe, but it's a second full network read per call, not
        # free -- call once per sheet and cache the result if the
        # pipeline needs it more than once.
        with self._source_access.open_binary(self.file_path, buffered=self._excel_buffered) as src:
            return _read_excel_row_metadata(src, sheet=sheet, mode=mode, column=column)

    def _ensure_workbook(self):
        if self._wb is None:
            # We intentionally keep the workbook context open across the whole
            # excel_resource lifetime so repeated get_table/get_map/get_range/
            # get_sheet_rows calls reuse one workbook instance and one set of
            # caches.
            #
            # In SMB non-buffered mode this also means the remote file stream
            # stays open until excel_resource.close(). In buffered mode the SMB
            # stream is closed immediately after reading into memory; only the
            # in-memory buffer remains open for the workbook lifetime.
            with suppress_openpyxl_data_validation_warning():
                self._workbook_cm = self._source_access.open_workbook(
                    self.file_path,
                    read_only=True,
                    data_only=True,
                    buffered=self._excel_buffered,
                )
                self._wb = self._workbook_cm.__enter__()
        return self._wb

    def get_table(self, name):
        if name not in self._table_cache:
            with suppress_openpyxl_data_validation_warning():
                wb = self._ensure_workbook()
                self._table_cache[name] = load_table(wb, self.tables[name])
        return self._table_cache[name]

    def get_map(self, name, cols=(0, 1)):
        key = (name, cols)
        if key not in self._map_cache:
            with suppress_openpyxl_data_validation_warning():
                wb = self._ensure_workbook()
                self._map_cache[key] = tbl2dict(wb, self.tables[name], cols=cols)
        return self._map_cache[key]

    def get_range(self, sheet, range_string, header=True):
        key = (sheet, range_string, header)
        if key not in self._range_cache:
            with suppress_openpyxl_data_validation_warning():
                wb = self._ensure_workbook()
                ws = wb[sheet]
                min_col, min_row, max_col, max_row = range_boundaries(range_string)
                rows = list(
                    ws.iter_rows(
                        min_row=min_row,
                        max_row=max_row,
                        min_col=min_col,
                        max_col=max_col,
                        values_only=True,
                    )
                )
                if not rows:
                    self._range_cache[key] = etl.empty()
                elif header:
                    self._range_cache[key] = etl.wrap([rows[0], *rows[1:]])
                else:
                    self._range_cache[key] = etl.wrap(rows)
        return self._range_cache[key]

    def get_sheet_rows(self, sheet, header=True):
        key = (sheet, header)
        if key not in self._sheet_cache:
            with suppress_openpyxl_data_validation_warning():
                wb = self._ensure_workbook()
                ws = wb[sheet]
                rows = list(ws.values)
                if not rows:
                    self._sheet_cache[key] = etl.empty()
                elif header:
                    self._sheet_cache[key] = etl.wrap([rows[0], *rows[1:]])
                else:
                    self._sheet_cache[key] = etl.wrap(rows)
        return self._sheet_cache[key]

    def close(self):
        try:
            if self._wb is not None:
                close_fn = getattr(self._wb, 'close', None)
                if close_fn is not None:
                    close_fn()
        finally:
            if self._workbook_cm is not None:
                self._workbook_cm.__exit__(None, None, None)
            self._workbook_cm = None
            self._wb = None
            self._table_cache.clear()
            self._map_cache.clear()
            self._range_cache.clear()
            self._sheet_cache.clear()



def build_excel_resource(file_path, *, source_access=None, excel_buffered=False, selection=None):
    source_access = _resolve_source_access(source_access)
    file_path = source_access.select_fixed_file(file_path)

    with suppress_openpyxl_data_validation_warning():
        sheets, tables = source_access.xlsx_info(file_path, buffered=excel_buffered)

    return excel_resource(
        file_path=file_path,
        sheets=sheets,
        tables=tables,
        source_access=source_access,
        excel_buffered=excel_buffered,
        selection=selection,
    )



def build_latest_xlsx_resource(
    folder_path,
    pattern='*.xlsx',
    *,
    include_hidden=False,
    include_system=False,
    include_temp=False,
    min_age_seconds=None,
    recursive=False,
    source_access=None,
    excel_buffered=False,
):
    source_access = _resolve_source_access(source_access)

    # Select via select_file_infos() (not select_latest_file()) so the
    # SelectedFile/stat_result used for the source_fingerprint() below is the
    # same one that picks the file to open -- no second directory listing or
    # re-stat.
    file_infos = source_access.select_file_infos(
        folder_path,
        pattern=pattern,
        include_hidden=include_hidden,
        include_system=include_system,
        include_temp=include_temp,
        min_age_seconds=min_age_seconds,
        recursive=recursive,
    )
    latest = max(file_infos, key=lambda item: (item.stat_result.st_mtime, item.path))

    selection = _ResourceSelection(
        source_kind='latest_file',
        root_path=str(folder_path),
        include_mask=pattern,
        recursive=recursive,
        selected_file=latest,
    )

    return build_excel_resource(
        latest.path,
        source_access=source_access,
        excel_buffered=excel_buffered,
        selection=selection,
    )
