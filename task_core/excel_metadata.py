# -*- coding: utf-8 -*-
"""
Level 1 leaf: Excel structural metadata (outline level, indent, style)
extracted directly from a workbook's raw XML, via zipfile + lxml -- not
openpyxl, since openpyxl doesn't expose outline levels/indent/style
through its own object model at the fidelity this needs.

Native to task_core -- not sourced from any external utility module.
task_core depends on no external petl_util-style module for anything;
task files (hr_task.py, ops_task.py, hr_petl_task.py) may have their own,
separate needs (month-name parsing, calendar tables, petl-table
transformation helpers), but everything Excel-metadata-related, including
aligning that metadata to a materialized table's rows, lives here.

The extraction logic itself (strip_ns/get_xmlinfo/get_styleinfo) predates
this module -- carried over verbatim from where it was already real and
already verified, not reimplemented from scratch.
"""

import posixpath
import re
import zipfile as _zipfile_module

from lxml import etree


def strip_ns(root):
    for elem in root.iter():
        if not isinstance(elem.tag, str):
            continue
        elem.tag = etree.QName(elem).localname
    etree.cleanup_namespaces(root)
    return root


def _parse_xml_from_zip(zf, name):
    with zf.open(name) as f:
        return strip_ns(etree.fromstring(f.read()))


def _normalize_sheet_target(target):
    target = target.replace('\\', '/')
    if target.startswith('/'):
        return target.lstrip('/')
    return posixpath.normpath(posixpath.join('xl', target))


def _get_attr_local(el, name):
    for k, v in el.attrib.items():
        if k == name or k.endswith('}' + name):
            return v
    return None


def _get_workbook_sheet_entries(zf):
    wb = _parse_xml_from_zip(zf, 'xl/workbook.xml')
    rels = _parse_xml_from_zip(zf, 'xl/_rels/workbook.xml.rels')
    rel_map = {}
    for rel in rels.xpath('//Relationships/Relationship'):
        rel_id = _get_attr_local(rel, 'Id')
        target = _get_attr_local(rel, 'Target')
        if rel_id and target:
            rel_map[rel_id] = _normalize_sheet_target(target)
    entries = []
    for idx, el in enumerate(wb.xpath('//workbook/sheets/*')):
        rel_id = _get_attr_local(el, 'id')
        entries.append({'index': idx, 'name': _get_attr_local(el, 'name'), 'r:id': rel_id, 'path': rel_map.get(rel_id)})
    return entries


def get_sheets(zf):
    return [x['name'] for x in _get_workbook_sheet_entries(zf)]


def _resolve_sheet_entry(zf, sheet=0):
    entries = _get_workbook_sheet_entries(zf)
    if isinstance(sheet, str):
        for entry in entries:
            if entry['name'] == sheet:
                return entry
        raise KeyError(f'Sheet name not found: {sheet!r}')
    if isinstance(sheet, int):
        try:
            return entries[sheet]
        except IndexError as exc:
            raise IndexError(f'Sheet index out of range: {sheet}') from exc
    raise TypeError(f'Invalid sheet spec: {sheet!r}')


def _excel_col_letters(column):
    if isinstance(column, str):
        match = re.match(r'([A-Za-z]+)', column.strip())
        if not match:
            raise ValueError(f'Invalid Excel column spec: {column!r}')
        return match.group(1).upper()
    if isinstance(column, int):
        if column < 0:
            raise ValueError('column index must be >= 0')
        n = column + 1
        out = []
        while n:
            n, rem = divmod(n - 1, 26)
            out.append(chr(65 + rem))
        return ''.join(reversed(out))
    raise TypeError(f'Invalid column spec: {column!r}')


def _cell_ref_col(cell_ref):
    if not cell_ref:
        return None
    match = re.match(r'([A-Za-z]+)', cell_ref)
    return match.group(1).upper() if match else None


def get_xmlinfo(zf, sheet=0, mode='outline', column=0):
    entry = _resolve_sheet_entry(zf, sheet=sheet)
    path = entry.get('path')
    if not path:
        raise ValueError(f'Worksheet target not resolved for sheet {sheet!r}')
    row_dicts = []
    cell_dicts = []
    want_outline = (mode == 'outline')
    target_col = None if want_outline else _excel_col_letters(column)
    xml = _parse_xml_from_zip(zf, path)
    for row in xml.xpath('//worksheet/sheetData/*'):
        row_data = dict(row.attrib)
        row_dicts.append(row_data)
        if not want_outline:
            picked = {}
            for cell in row:
                if etree.QName(cell).localname != 'c':
                    continue
                if _cell_ref_col(cell.get('r')) == target_col:
                    picked = dict(cell.attrib)
                    break
            cell_dicts.append(picked)
    row_nums = [int(x['r']) if x.get('r') is not None else None for x in row_dicts]
    return row_nums, row_dicts if want_outline else cell_dicts


def get_styleinfo(zf, dic):
    style = []
    align = []
    xml = _parse_xml_from_zip(zf, 'xl/styles.xml')
    for el in xml.xpath('//styleSheet/cellXfs/*'):
        style.append(dict(el.attrib))
        alignment = el.find('alignment')
        indent = alignment.get('indent') if alignment is not None else None
        align.append(int(indent) if indent is not None else None)
    aligns = [align[int(x['s'])] if x.get('s') is not None else None for x in dic]
    styles = [style[int(x['s'])] if x.get('s') is not None else None for x in dic]
    return aligns, styles


def read_excel_row_metadata(source, *, sheet=0, mode='outline', column=None):
    if mode == 'outline':
        if column is not None:
            raise ValueError("mode='outline' is row-level metadata; column must be None")
    elif mode in ('indent', 'style'):
        if column is None:
            raise ValueError(f"mode={mode!r} is cell-level metadata; column is required")
    else:
        raise ValueError(f'unsupported mode: {mode!r}')

    with _zipfile_module.ZipFile(source) as zf:
        nms, dics = get_xmlinfo(zf, sheet=sheet, mode=mode, column=column if column is not None else 0)
        if mode == 'outline':
            values = [int(x['outlineLevel']) if 'outlineLevel' in x else 0 for x in dics]
        elif mode == 'indent':
            values, _ = get_styleinfo(zf, dics)
        else:  # style
            _, values = get_styleinfo(zf, dics)

    return dict(zip(nms, values))


def align_row_metadata(metadata, *, first_row, n_rows):
    """Turn a {row_number: value} mapping (as returned by
    read_excel_row_metadata) into a plain positional list of length
    n_rows, assuming a separately materialized table's row 0 corresponds
    to XLSX row `first_row` and rows are otherwise contiguous.

    A row missing from `metadata` (an XML gap) lands as None at that one
    position; every other row keeps its own correct value rather than
    everything after the gap silently shifting by one.
    """
    return [metadata.get(first_row + i) for i in range(n_rows)]
