# -*- coding: utf-8 -*-


"""Level 1 leaf: openpyxl warning suppression only."""

from contextlib import contextmanager
import warnings


@contextmanager
def suppress_openpyxl_data_validation_warning():
    # Name kept as-is to avoid touching all ten call sites across
    # resources/excel.py, table_adapters.py, file_access.py -- but this now
    # also covers two more openpyxl "extension not supported" warnings hit in
    # real source files: Slicer List (table/PivotTable slicers) and the
    # generic Unknown-extension case. All three are the same underlying
    # situation -- an OOXML <extLst> element openpyxl doesn't model, dropped
    # only if the loaded workbook were saved back out, which this codebase
    # never does to a source file.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message='Data Validation extension is not supported and will be removed',
            category=UserWarning,
        )
        warnings.filterwarnings(
            'ignore',
            message='Slicer List extension is not supported and will be removed',
            category=UserWarning,
        )
        warnings.filterwarnings(
            'ignore',
            message='Unknown extension is not supported and will be removed',
            category=UserWarning,
        )
        yield
