# -*- coding: utf-8 -*-
"""
Public facade for task_core. Pure re-exports only -- no function or class
bodies of its own, no matter how small. This is a named, standing rule,
not an incidental property of the current contents.

This is the acceptance test for the whole package migration: every name
below must resolve exactly as it did when task_core was a single flat
file, so that existing *_task.py files (ops_task.py and friends) need zero
source changes.

One recorded, deliberate behavior exception to that guarantee (v0.2.0):
select_file_infos() given a *file* path now raises ValueError directing
callers to select_fixed_file(), instead of silently returning a
single-file selection with every filter argument ignored. The name still
resolves; this one behavior intentionally does not. See README
("Dependencies" section's changelog note) and file_access.py's own error
message for the rationale.
"""

__version__ = '0.6.10'

from task_core.db_publish import (
    CopyLoadPolicy,
    DbPublishError,
    DbPublishInvariantError,
    IdentifierPolicy,
    PublicationLockPolicy,
    PublisherConfig,
)
from task_core.types import (
    DbRunResult,
    PipelineContractError,
    PipelineError,
    PipelineSpec,
    OutputColumn,
    RunResult,
    SourceCheckError,
    get_pipeline_spec,
)

from task_core.source_tracking import (
    SourceChangeCheckConfig,
    SourceFileMeta,
    SourceFingerprint,
    TrackedDbQuerySource,
    TrackedResourceSource,
    make_source_signature,
)

from task_core.file_access import (
    LOCAL_FILE_ACCESS,
    NoMatchingFilesError,
    SelectedFile,
    build_source_access,
    source_access,
    is_excel_temp_file,
    is_hidden_file,
    is_system_file,
    select_file_infos,
    select_files,
    select_fixed_file,
    select_latest_file,
)

from task_core.resources.db import build_db_resource, db_resource

from task_core.excel_metadata import align_row_metadata

from task_core.resources.excel import (
    build_excel_resource,
    build_latest_xlsx_resource,
    excel_resource,
)

from task_core.resources.file_set import build_file_set_resource, file_set_resource

from task_core.binding import (
    PipelineBinding,
    ResourceEnvironment,
    ResourceSpec,
    bind,
    build_resource_context,
    compute_resource_wiring,
    validate_bindings,
)

from task_core.resources.factories import latest_xlsx, resource, xlsx_file_set

from task_core.context import task_context

from task_core.export import build_db_payload, export_excel

from task_core.table_adapters import normalize_for_excel

from task_core.runner import run_pipelines, validate_pipeline_class, validate_pipeline_classes

from task_core.logging_setup import setup_logging
