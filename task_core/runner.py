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


def _suppress_context_recursively(e):
    # A grouped cleanup failure (BaseExceptionGroup/ExceptionGroup) has
    # its own __context__ chained by Python the same way a single
    # exception does, but so does *each exception nested inside it* --
    # found by external review, confirmed directly: setting
    # __suppress_context__ only on the outer group left every nested
    # exception still showing its own, full chain back to primary_error,
    # so the diagnostic-duplication problem this was meant to fix
    # remained for any multi-error cleanup failure. Recurses through
    # .exceptions for nested groups, arbitrarily deep, since one group's
    # own sub-exceptions could themselves be groups.
    #
    # This mutates e (and everything nested inside it) permanently, not
    # scoped to a single log call in any literal sense despite only
    # affecting this function's own logging of it -- also found by
    # external review, a fair correction to this comment's own, earlier
    # wording. In practice this has no real consequence beyond that:
    # every exception this touches came from cleanup_errors, discarded
    # once run_pipelines() returns or raises, never reused or re-raised
    # as itself anywhere else.
    e.__suppress_context__ = True
    if isinstance(e, BaseExceptionGroup):
        for sub in e.exceptions:
            _suppress_context_recursively(sub)


def _underlying_pipeline_class(entry):
    # A PIPELINES entry may be a plain pipeline class (existing convention)
    # or a PipelineBinding (task_core/binding.py) wrapping one with its
    # resource bindings. Everywhere this module needs .spec, .__name__, or
    # .run itself, it needs the underlying class, not the wrapper.
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

    seen = set()
    duplicates = []
    for name in run_sequence:
        if name in seen and name not in duplicates:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        raise PipelineContractError(f'run_sequence contains duplicate pipeline(s): {duplicates}')

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
    publisher_factory=DbPublisher,
):
    """publisher_factory: the DbPublisher constructor to use, real by
    default. Pass a fake here for tests -- e.g. tc.run_pipelines(...,
    publisher_factory=FakeDbPublisher) -- rather than monkeypatching
    tc.runner.DbPublisher, which this default parameter value no longer
    responds to: like any default argument, it's bound once, to whatever
    DbPublisher was at the time this function was defined (module import
    time), not re-read from the module namespace on every call."""
    log = logging.getLogger(task_name)
    specs = validate_pipeline_classes(pipelines, run_sequence)
    ctx = build_context()
    publisher = None
    pipeline_rows = {}
    excel_outputs = []

    # Explicit, not inferred from sys.exc_info() -- confirmed directly
    # that interpreter exception state is not a reliable signal of
    # whether *this task* has a primary failure. A caller of
    # run_pipelines() sitting inside its own, unrelated except: block
    # made sys.exc_info() non-None for this call's entire duration, even
    # when the task itself succeeded -- a resource cleanup failure during
    # that genuinely-successful task incorrectly looked like it had
    # something ambient to avoid masking, and got logged instead of
    # raised, silently hiding a real, leaked resource. Only this
    # function's own try/except genuinely knows whether this task failed.
    primary_error = None
    cleanup_errors = []
    rollback_attempted = False

    def try_step(fn, description):
        try:
            fn()
        except BaseException as e:
            # BaseException, not Exception -- same reasoning as
            # cleanup.py's attempt_all_cleanup(): a KeyboardInterrupt
            # raised by publisher.close()/rollback()/ctx.close() itself
            # must not stop the remaining cleanup steps in this finally:
            # block, or replace what's already propagating.
            e.add_note(description)
            cleanup_errors.append(e)

    def try_rollback():
        # Defensive, not currently load-bearing through any reachable
        # path: try_step() below already catches rollback()'s own
        # failure rather than letting it propagate, so the skip path's
        # `try_rollback(); return skipped_result` always completes
        # normally and the outer except: block never triggers from this
        # specific scenario. This guard protects against a real
        # possibility all the same -- some future change adding code
        # between try_rollback() and that return which itself raises,
        # which would trigger the outer except: and could otherwise
        # attempt a second rollback() on the same publisher.
        nonlocal rollback_attempted
        if rollback_attempted or publisher is None:
            return
        rollback_attempted = True
        try_step(publisher.rollback, 'while rolling back')

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
            publisher = publisher_factory(creds=creds, schema=pg_schema, logger=log)

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
                try_rollback()
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
                    kwargs = {}
                    for alias, resource_spec in entry.resources.items():
                        spec_id = id(resource_spec)
                        if spec_id not in ctx.resource_keys_by_spec_id:
                            raise PipelineContractError(
                                f"{ctx.task_name}: pipeline '{pipeline_name}' binding '{alias}' -- "
                                f"ctx.resource_keys_by_spec_id has no entry for this resource. "
                                f"task_context was built without the binding key map: pass "
                                f"compute_resource_wiring()'s key_by_spec_id through as "
                                f"resource_keys_by_spec_id=... when hand-building task_context() "
                                f"for an incremental migration, or use build_resource_context() "
                                f"instead once every pipeline is migrated."
                            )
                        kwargs[alias] = ctx.get_resource(ctx.resource_keys_by_spec_id[spec_id])
                    out_tbl = pipeline_cls.run(ctx, **kwargs)
                elif callable(getattr(pipeline_cls, 'run', None)):
                    out_tbl = pipeline_cls.run(ctx)
                else:
                    raise PipelineError(ctx.task_name, pipeline_name, 'run', 'pipeline has no run(ctx) method')
            except (PipelineError, PipelineContractError):
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

    except BaseException as e:
        # BaseException, not Exception: KeyboardInterrupt, SystemExit, and
        # GeneratorExit are BaseException subclasses but not Exception
        # ones, so an `except Exception:` here never caught them --
        # primary_error stayed None during a genuine interruption,
        # meaning a cleanup failure during that interruption looked like
        # the only failure there was to report and replaced the
        # interruption itself in what actually propagated. Confirmed
        # directly before fixing. This does not change how the inner
        # pipeline-loop wrapper behaves (it still only ever wraps
        # Exception subclasses into PipelineError) -- that wrapper was
        # already correct here, since KeyboardInterrupt isn't an
        # Exception subclass either, so it was never caught and wrapped
        # there in the first place; it needs no change, only this outer
        # boundary did.
        primary_error = e
        try_rollback()
        log.exception('task %s failed', ctx.task_name)
        raise

    finally:
        if publisher is not None:
            try_step(publisher.close, 'while closing publisher')
        try_step(ctx.close, 'while closing task context')

        if cleanup_errors:
            if primary_error is not None:
                # Logged, not raised: primary_error is already
                # propagating (or about to, from the except: block's own
                # `raise` above) -- these must never replace it. Each
                # error's own, correct traceback is attached explicitly
                # (not a plain log.exception() call here, which would
                # read sys.exc_info() at *this* point and log whatever's
                # currently ambient -- confirmed directly this was
                # primary_error's own traceback, not the cleanup
                # failure's, silently losing the actual cleanup error
                # from diagnostics).
                for e in cleanup_errors:
                    # See _suppress_context_recursively()'s own docstring
                    # for why this needs to recurse into a grouped
                    # cleanup failure's own nested exceptions, not just
                    # set this on the outer group. Confirmed directly,
                    # not assumed, that __suppress_context__ is respected
                    # by logging's own exc_info=(...) formatting, the
                    # same mechanism `raise ... from None` uses for
                    # uncaught tracebacks -- not a plain log.exception()
                    # call here either, which would read sys.exc_info()
                    # at *this* point and log whatever's currently
                    # ambient, confirmed directly this was primary_error's
                    # own traceback, not the cleanup failure's, silently
                    # losing the actual cleanup error from diagnostics.
                    _suppress_context_recursively(e)
                    log.error('cleanup error during run_pipelines()', exc_info=(type(e), e, e.__traceback__))
            elif len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            else:
                raise BaseExceptionGroup('multiple cleanup steps failed', cleanup_errors)
