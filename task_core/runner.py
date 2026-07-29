# -*- coding: utf-8 -*-
"""
Level 3: run_pipelines() orchestration. Sits above everything else in
the package. context.py and source_tracking.py are TYPE_CHECKING-only
imports (run_pipelines() only ever duck-types ctx/source_change_check);
db_publish.py, source_state.py, export.py, table_adapters.py, and
types.py are real runtime imports.

Deliberately engine-neutral: no petl or pandas import here, and no
openpyxl_compat either -- all of them live entirely inside
table_adapters.py, reached only through the adapter's uniform interface
(validate/nrows/display/to_excel/to_db_payload). This module never
branches on which engine a pipeline uses.
"""

from __future__ import annotations

from datetime import datetime, timezone
import functools
import logging
import os
from typing import TYPE_CHECKING

from task_core.db_publish import (
    DbPublisher,
    PublicationPlan,
    PublisherConfig,
)

from task_core.types import (
    DbRunResult,
    PipelineContractError,
    PipelineError,
    RunResult,
    SourceCheckError,
    find_duplicates,
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

    if spec.output_schema is not None and callable(
        getattr(task_cls, 'get_dynamic_db_contract', None)
    ):
        raise PipelineContractError(
            f'{name}: output_schema cannot be combined with '
            f'get_dynamic_db_contract(); declared schemas require a static final '
            f'column contract'
        )

    if not callable(getattr(task_cls, 'run', None)):
        raise PipelineContractError(f'{name}: missing callable run(ctx) method')

    return spec


def _normalized_output_target(kind, value):
    """The form two declared targets are compared in to decide whether they
    are the same physical output.

    casefold() on both kinds, deliberately, and deliberately NOT
    os.path.normcase() -- which is a no-op on POSIX and lowercases on
    Windows, and would therefore make this validation give different
    answers depending on where it happened to run. A task that validates
    clean on a developer's machine and then collides on the server is
    worse than a rule that is merely strict. Casefolding everywhere is
    deterministic, and it is the correct answer for both targets anyway:
    Windows filesystems treat Report.xlsx and report.xlsx as one file, and
    PostgreSQL folds unquoted identifiers to lower case, so Sales and
    sales are one table.

    The cost is rejecting two names that genuinely differ only by case on
    a case-sensitive filesystem. That combination is already broken on the
    platform this ships to, so refusing it everywhere loses nothing real.

    normpath() additionally collapses './out.xlsx' and 'out.xlsx', which
    are unambiguously the same file on every platform.
    """
    if kind == 'excel_name':
        # Windows filesystems genuinely treat Report.xlsx and report.xlsx as
        # one file, so casefolding is correct here and stays.
        # abspath, not just normpath: to_excel() writes relative to the
        # process working directory, so 'out.xlsx' and
        # '/cwd/out.xlsx' are the same physical file while normpath alone
        # keeps them as different keys -- confirmed directly, leaving the
        # original silent-overwrite class reachable. (Symlink aliases would
        # need realpath(); not handled here.)
        return os.path.abspath(os.path.normpath(value)).casefold()

    # Database identifiers are already required to satisfy the lower-case
    # portable contract. Preserve the declared value exactly; backend
    # preflight rejects anything outside that contract.
    return value


def _reject_duplicate_output_targets(specs):
    """Two active pipelines writing the same output silently destroyed each
    other's work. Confirmed directly, both kinds:

    excel_name -- to_excel() writes a whole workbook via toxlsx(path);
    PipelineSpec has no sheet field, so there is no same-file-different-
    sheet arrangement to protect. Two pipelines declaring 'same.xlsx' both
    ran, both reported success, excel_outputs listed the name twice, and
    only the second pipeline's rows existed on disk.

    db_table -- worse, because it happens inside the committed
    transaction. publish() does DROP + CREATE, so the second pipeline
    dropped the table the first had just filled. Reproduced against a real
    SQLAlchemy engine: three rows after pipeline one, one row after
    pipeline two, one row after commit. row_counts is keyed by table name,
    so it reported {'same_table': 1} and the three lost rows left no trace
    in the RunResult at all.

    Checked here, in validate_pipeline_classes(), because this is the last
    point before build_context() -- so a task with colliding targets fails
    before any resource is constructed, any remote file is opened, or any
    connection is made, rather than partway through a run that has already
    done real work.

    Checked unconditionally rather than only when output_excel/output_db
    are on. A duplicate declaration is a defect in the task definition,
    not a property of one invocation; gating it on this run's flags would
    let it hide until the first run that happens to enable that output.

    find_duplicates() (types.py) is deliberately not reused: it returns the
    duplicated values, and the useful part of this error is WHICH pipelines
    collide, which needs the owners kept alongside them.
    """
    owners = {}
    for name, spec in specs.items():
        for kind in ('excel_name', 'db_table'):
            value = getattr(spec, kind)
            if not value:
                continue
            key = (kind, _normalized_output_target(kind, value))
            owners.setdefault(key, []).append((name, value))

    collisions = [
        (kind, pipelines_and_values)
        for (kind, _), pipelines_and_values in owners.items()
        if len(pipelines_and_values) > 1
    ]
    if not collisions:
        return

    described = '; '.join(
        '{} -> {}'.format(
            kind,
            ', '.join(f'{name}={value!r}' for name, value in pipelines_and_values),
        )
        for kind, pipelines_and_values in collisions
    )
    raise PipelineContractError(
        f'pipelines in run_sequence declare the same output target, which would '
        f'silently overwrite each other: {described}'
    )


def _skipped_result(ctx, output_db, *, reason, source_check_enabled,
                    source_changed=None, fingerprints=None):
    """A run that did no pipeline work. Both skip paths -- sources
    unchanged, and another run holding the advisory lock -- return this
    shape, so a caller distinguishes them by skip_reason rather than by
    which fields happen to be populated.
    """
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
        skip_reason=reason,
        source_check_enabled=source_check_enabled,
        source_changed=source_changed,
        source_fingerprints=fingerprints or [],
    )


def validate_pipeline_classes(pipelines, run_sequence):
    missing = [name for name in run_sequence if name not in pipelines]
    if missing:
        raise PipelineContractError(f'run_sequence contains unknown pipeline(s): {missing}')

    duplicates = find_duplicates(run_sequence)
    if duplicates:
        raise PipelineContractError(f'run_sequence contains duplicate pipeline(s): {duplicates}')

    specs = {
        name: validate_pipeline_class(pipelines[name], pipeline_name=name)
        for name in run_sequence
    }
    _reject_duplicate_output_targets(specs)
    return specs


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
    # Both outputs default OFF. Excel defaulted on until 0.3.6, which sat
    # badly against decisions/0007: an aid you opt into should not be
    # produced by a task that never asked for it. It also made the two
    # outputs inconsistent for no reason -- output_db has always defaulted
    # off.
    #
    # The failure mode of this change is silent: a caller that relied on
    # the default stops getting workbooks with no error. Every call site in
    # this repository passes it explicitly (checked: 57 of 57), so nothing
    # here changes behaviour, but an external caller may need the keyword
    # added.
    output_excel=False,
    output_db=False,
    creds=None,
    pg_schema='bsr',
    source_change_check=None,
    force_run=False,
    publisher_config=None,
):
    """publisher_config: a frozen PublisherConfig holding publisher_factory,
    identifier_policy and publication_lock_policy. Defaults to
    PublisherConfig(). Resolved once here and read from thereafter, so
    there is no second defaulting point that could diverge from what
    preflight validated against.
    """
    log = logging.getLogger(task_name)

    # Resolved ONCE. Every later use reads from this object, so there is no
    # second defaulting point that could silently diverge -- which is the
    # failure IdentifierPolicy was created to stop, now prevented for the
    # whole publisher configuration rather than one integer.
    config = publisher_config if publisher_config is not None else PublisherConfig()
    publisher_factory = config.resolved_factory()
    identifier_policy = config.identifier_policy
    publication_plan = PublicationPlan()

    specs = validate_pipeline_classes(pipelines, run_sequence)

    # Backend-specific preflight, invoked by an engine-neutral runner. The
    # knowledge of what a valid PostgreSQL identifier is stays behind
    # publisher_factory; this module only knows there is a backend to ask,
    # and that here -- after structural validation, before build_context()
    # -- is when to ask it. A classmethod, so nothing is constructed that
    # would need closing if build_context() raises next.
    #
    # Deliberately NOT gated on output_db: an unpublishable declared name
    # is a defect in the task, not a property of one invocation. The hook
    # performs no backend I/O, so a run with DB output disabled still
    # touches nothing. It no-ops when no spec declares db_table.
    # Resolved from the factory when it provides one, falling back to the
    # REAL backend policy -- never to a no-op. publisher_factory is
    # duck-typed and is legitimately a plain callable in places
    # (lambda **kw: FakePublisher(**kw, close_error=...) is a useful test
    # idiom), so a callable with no classmethod to ask must still get
    # validated rather than quietly skipped. Skipping on absence is exactly
    # how an optional hook ends up with zero coverage for years.
    preflight = getattr(publisher_factory, 'preflight', DbPublisher.preflight)
    # The source-state table is passed in as a reserved target: a
    # source-check-enabled run creates and writes it, so its identifiers
    # need the same byte validation as any business table, and no pipeline
    # may publish over it.
    source_state_target = None
    if source_change_check is not None and getattr(source_change_check, 'enabled', False):
        source_state_target = (
            getattr(source_change_check, 'schema', None),
            getattr(source_change_check, 'table', None),
        )

    preflight(
        specs,
        schema=pg_schema,
        source_state_target=source_state_target,
        identifier_policy=identifier_policy,
    )

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
        has_db_outputs = output_db and any(specs[name].db_table for name in run_sequence)

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
            publisher = publisher_factory(
                creds=creds,
                schema=pg_schema,
                logger=log,
                identifier_policy=identifier_policy,
                publication_lock_policy=config.publication_lock_policy,
                publication_plan=publication_plan,
                task_name=ctx.task_name,
            )

            # The task advisory lock, before anything else touches the
            # database. Acquired ahead of the source-state read because
            # fingerprinting is the expensive part on a remote share and
            # there is no point paying for it only to lose the race at the
            # first publish -- and because update_source_state() WRITES, so
            # two runs past the read are both intending to write the same
            # rows.
            #
            # try, not the blocking form: a schedule that fires faster than
            # the task runs would otherwise build an unbounded queue, which
            # is worse than the collision.
            if not publisher.begin_run():
                log.warning(
                    'another run of %s is already in progress; skipping this one',
                    ctx.task_name,
                )
                return _skipped_result(
                    ctx, output_db,
                    reason='task_already_running',
                    source_check_enabled=source_check_enabled,
                )

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
            source_changed = not unchanged

            if unchanged and not force_run:
                log.info('sources unchanged, skipping pipeline execution')
                try_rollback()
                return _skipped_result(
                    ctx, output_db,
                    reason='sources_unchanged',
                    source_check_enabled=True,
                    source_changed=False,
                    fingerprints=current_fingerprints,
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

            # Repeated traversal of out_tbl is expected whenever anything
            # beyond the always-run nrows() call below will also traverse
            # it -- confirmed directly, not assumed, that this must happen
            # before nrows(), the first traversal: stabilize() has zero
            # effect unless applied before whatever traversal is first,
            # since that traversal is what populates the caching it
            # relies on. For a lazy petl transformation chain, every
            # later traversal otherwise re-runs the entire chain from
            # scratch; for a db_resource-backed table specifically, each
            # traversal re-issues the underlying SQL query, which is a
            # correctness risk, not just a performance one -- a changing
            # source table could produce different row counts between
            # nrows() and whatever publishes afterward.
            needs_stabilization = (
                spec.publish_result
                or spec.debug_display
                or (output_excel and spec.excel_name)
                or (output_db and spec.db_table)
            )
            if needs_stabilization:
                out_tbl = adapter.stabilize(out_tbl, repeated=True)

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
                # QUEUED, not executed. The source-state write must land in
                # the same transaction as the swaps or a failed publication
                # could still advance the stored fingerprints. Queuing it
                # keeps commit()'s signature and the publisher protocol
                # unchanged -- both were expanded by accident once already.
                publication_plan.add(
                    f'update source state ({len(current_fingerprints)} sources)',
                    functools.partial(
                        update_source_state,
                        publisher,
                        task_name=ctx.task_name,
                        fingerprints=current_fingerprints,
                        config=source_change_check,
                    ),
                )
            # The publication phase: verify prepared artifacts, run queued
            # steps, swap every staging table, commit. One short
            # transaction, deliberately not a separate call here -- doing
            # the swap from the runner made publish()+commit() silently
            # incorrect for any direct caller.
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
                # Logged, not raised: primary_error is already propagating
                # (or about to, from the except: block's own `raise` above)
                # -- these must never replace it. Each error's own, correct
                # traceback is attached explicitly via exc_info=(...): a
                # plain log.exception() here would read sys.exc_info() at
                # *this* point and log whatever's currently ambient --
                # confirmed directly that was primary_error's own traceback,
                # not the cleanup failure's, silently losing the actual
                # cleanup error from diagnostics. __suppress_context__ is
                # respected by that same exc_info formatting (the `raise
                # ... from None` mechanism); see
                # _suppress_context_recursively()'s docstring for why it
                # must recurse into a grouped failure's nested exceptions
                # rather than mark only the outer group.
                for e in cleanup_errors:
                    _suppress_context_recursively(e)
                    log.error('cleanup error during run_pipelines()', exc_info=(type(e), e, e.__traceback__))
            elif len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            else:
                raise BaseExceptionGroup('multiple cleanup steps failed', cleanup_errors)
