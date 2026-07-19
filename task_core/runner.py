# -*- coding: utf-8 -*-
"""
Level 3: run_pipelines() orchestration. Sits above everything else in
the package. context.py and source_tracking.py are TYPE_CHECKING-only
imports (run_pipelines() only ever duck-types ctx/source_change_check);
db_publish.py, source_state.py, export.py, table_adapters.py, and
types.py are real runtime imports.

Deliberately engine-neutral: no petl_util or pandas import here, and no
openpyxl_compat either -- both live entirely inside table_adapters.py
now, reached only through the adapter's uniform five-method interface
(validate/nrows/display/to_excel/to_db_payload). This module never
branches on which engine a pipeline uses.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import TYPE_CHECKING

from task_core.db_publish import DbPublisher

from task_core.types import (
    DbRunResult,
    PipelineContractError,
    PipelineError,
    RunResult,
    SourceCheckError,
    get_pipeline_spec,
)
from task_core.source_state import build_source_state_store, update_source_state
from task_core.export import _build_db_payload_with_spec, _export_excel_with_spec
from task_core.table_adapters import get_table_adapter
from task_core.binding import PipelineBinding

if TYPE_CHECKING:
    from task_core.context import task_context
    from task_core.source_tracking import SourceChangeCheckConfig


def _underlying_pipeline_class(entry):
    # A PIPELINES entry may be a plain pipeline class (existing convention)
    # or a PipelineBinding (task_core/binding.py) wrapping one with its
    # resource bindings. Everywhere this module needs .spec, .__name__, or
    # .run itself, it needs the underlying class, not the wrapper.
    from task_core.binding import PipelineBinding
    if isinstance(entry, PipelineBinding):
        return entry.pipeline
    return entry


def validate_pipeline_class(task_cls, *, pipeline_name=None):
    task_cls = _underlying_pipeline_class(task_cls)
    name = pipeline_name or getattr(task_cls, '__name__', repr(task_cls))

    spec = get_pipeline_spec(task_cls)

    if not callable(getattr(task_cls, 'run', None)):
        raise PipelineContractError(f'{name}: missing callable run(ctx) method')

    return spec


def validate_pipeline_classes(pipelines, run_sequence):
    missing = [name for name in run_sequence if name not in pipelines]
    if missing:
        raise PipelineContractError(f'run_sequence contains unknown pipeline(s): {missing}')

    return {
        name: validate_pipeline_class(pipelines[name], pipeline_name=name)
        for name in run_sequence
    }


def _build_db_run_result(*, output_db, has_db_outputs, publisher=None):
    if publisher is None:
        return DbRunResult(
            requested=bool(output_db),
            had_outputs=bool(has_db_outputs),
            committed=False,
            committed_tables=[],
            published_tables=[],
            row_counts={},
        )

    return DbRunResult(
        requested=bool(output_db),
        had_outputs=bool(has_db_outputs),
        committed=publisher.committed,
        committed_tables=publisher.committed_tables,
        published_tables=publisher.written_tables,
        row_counts=publisher.table_rows,
    )


def run_pipelines(
    task_name,
    build_context,
    pipelines,
    run_sequence,
    output_excel=True,
    output_db=False,
    creds=None,
    pg_schema='bsr',
    source_change_check=None,
    force_run=False,
):
    log = logging.getLogger(task_name)
    specs = validate_pipeline_classes(pipelines, run_sequence)
    ctx = build_context()
    publisher = None
    pipeline_rows = {}
    excel_outputs = []

    try:
        has_db_outputs = output_db and any(
            specs[name].db_table
            for name in run_sequence
        )

        source_check_enabled = (
            output_db
            and source_change_check is not None
            and source_change_check.enabled
        )

        if not output_db and source_change_check is not None and source_change_check.enabled:
            log.info('source check ignored because output_db=False')

        if source_check_enabled and not ctx.tracked_sources:
            raise SourceCheckError(
                f'{ctx.task_name}: source change check is enabled, but no tracked_sources are configured'
            )

        # A DbPublisher is needed either to publish real output tables, or to
        # read/write the technical source-state table -- source-change
        # checking must not open a second, independent DB connection.
        if has_db_outputs or source_check_enabled:
            publisher = DbPublisher(creds=creds, schema=pg_schema, logger=log)

        current_fingerprints = []
        source_changed = None

        if source_check_enabled:
            log.info(
                'source check enabled; sources=%s',
                [source.source_key for source in ctx.tracked_sources],
            )
            current_fingerprints = ctx.collect_source_fingerprints()
            log.info('source fingerprints collected, count=%s', len(current_fingerprints))

            store = build_source_state_store(
                publisher,
                schema=source_change_check.schema,
                table=source_change_check.table,
            )
            if source_change_check.create_if_missing:
                store.ensure_table()

            unchanged = store.sources_unchanged(ctx.task_name, current_fingerprints)
            # This read may have auto-begun an implicit transaction on the
            # connection (see DbPublisher.discard_pending_read()); reset it
            # before anything below tries to start an explicit transaction.
            publisher.discard_pending_read()

            source_changed = not unchanged

            if unchanged and not force_run:
                log.info('sources unchanged, skipping pipeline execution')
                publisher.rollback()
                return RunResult(
                    task_name=ctx.task_name,
                    pipeline_rows={},
                    excel_outputs=[],
                    db=DbRunResult(
                        requested=bool(output_db),
                        had_outputs=False,
                        committed=False,
                        committed_tables=[],
                        published_tables=[],
                        row_counts={},
                    ),
                    skipped=True,
                    skip_reason='sources_unchanged',
                    source_check_enabled=True,
                    source_changed=False,
                    source_fingerprints=current_fingerprints,
                )

            if unchanged and force_run:
                log.info('force_run=True, running despite unchanged sources')
            else:
                log.info('source changed, running full task')

        run_started_at = datetime.now(timezone.utc)

        for pipeline_name in run_sequence:
            entry = pipelines[pipeline_name]
            pipeline_cls = _underlying_pipeline_class(entry)
            spec = specs[pipeline_name]
            adapter = get_table_adapter(spec.table_adapter)
            log.info('starting pipeline %s', pipeline_name)

            try:
                if isinstance(entry, PipelineBinding):
                    kwargs = {
                        alias: ctx.get_resource(ctx.resource_keys_by_spec_id[id(resource_spec)])
                        for alias, resource_spec in entry.resources.items()
                    }
                    out_tbl = pipeline_cls.run(ctx, **kwargs)
                elif callable(getattr(pipeline_cls, 'run', None)):
                    out_tbl = pipeline_cls.run(ctx)
                else:
                    raise PipelineError(ctx.task_name, pipeline_name, 'run', 'pipeline has no run(ctx) method')
            except PipelineError:
                raise
            except Exception as e:
                raise PipelineError(ctx.task_name, pipeline_name, 'run', 'failed during pipeline execution') from e

            adapter.validate(out_tbl)

            rows = adapter.nrows(out_tbl)
            pipeline_rows[pipeline_name] = rows
            log.info('pipeline %s finished, rows=%s', pipeline_name, rows)

            if spec.publish_result:
                ctx.set_result(pipeline_name, out_tbl)

            if spec.debug_display:
                adapter.display(out_tbl)

            if output_excel and spec.excel_name:
                excel_name = _export_excel_with_spec(out_tbl, spec)
                if excel_name is not None:
                    excel_outputs.append(excel_name)

            if output_db and spec.db_table:
                payload = _build_db_payload_with_spec(pipeline_cls, out_tbl, spec, pg_schema, run_started_at=run_started_at)
                publisher.publish(payload)

        if publisher is not None:
            if source_check_enabled:
                update_source_state(
                    publisher,
                    task_name=ctx.task_name,
                    fingerprints=current_fingerprints,
                    config=source_change_check,
                )
                log.info('source state updated, source_count=%s', len(current_fingerprints))
            publisher.commit()

        db_result = _build_db_run_result(
            output_db=output_db,
            has_db_outputs=has_db_outputs,
            publisher=publisher,
        )
        return RunResult(
            task_name=ctx.task_name,
            pipeline_rows=pipeline_rows,
            excel_outputs=excel_outputs,
            db=db_result,
            skipped=False,
            skip_reason=None,
            source_check_enabled=source_check_enabled,
            source_changed=source_changed,
            source_fingerprints=current_fingerprints,
        )

    except Exception:
        if publisher is not None:
            publisher.rollback()
        log.exception('task %s failed', ctx.task_name)
        raise

    finally:
        if publisher is not None:
            publisher.close()
        ctx.close()
