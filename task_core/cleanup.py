# -*- coding: utf-8 -*-
"""
Level 1 leaf: attempt_all_cleanup(), used by task_context.close()
(context.py) for its own multi-resource loop. Kept here, not in
context.py, specifically so runner.py's existing, deliberate boundary
(it duck-types ctx, and never imports context.py at runtime, only under
TYPE_CHECKING) doesn't have to be broken just to use this too -- though
runner.py's own cleanup steps (rollback/publisher.close()) don't go
through this helper at all now; see its own module docstring for why.

This module previously decided whether to log or raise a cleanup failure
by checking sys.exc_info() -- whether Python currently has an exception
being handled, anywhere up the call stack. Found by external review,
confirmed directly: that is not a reliable signal that *this task* has a
primary failure. A caller of run_pipelines() sitting inside its own,
unrelated except: block (e.g. `except ValueError: run_pipelines(...)`)
makes sys.exc_info() non-None for the whole duration of that call, even
if the task itself completes with no error at all -- a resource cleanup
failure during that genuinely-successful task would incorrectly look
like it had something ambient to avoid masking, and get logged instead
of raised, silently hiding a real, leaked resource.

There is no reliable way to infer "does *this task* have a primary
failure" from interpreter state -- it has to be tracked explicitly, by
the one piece of code that actually knows: run_pipelines()'s own
try/except. attempt_all_cleanup() below reflects that: it no longer
takes any suppress/log parameters at all. It always attempts every item
and always raises at the end if anything failed -- a single exception if
only one item failed, an ExceptionGroup if more than one did. The
decision of whether to let that raise propagate or catch and log it
(with the failure's own, correct traceback -- attached via add_note(),
not a separate exc_info=True log call made outside the except: block
that actually caught it, which was a second, related bug: exc_info=True
reads sys.exc_info() at the time of the *logging* call, which by then no
longer reflects the resource's own exception, so the wrong traceback
ended up in the log) belongs entirely to the caller that has an actual
primary-failure signal to consult -- run_pipelines() itself.
"""


def attempt_all_cleanup(items, close_fn, *, describe):
    """items: an iterable of things to attempt closing.
    close_fn(item): performs one cleanup step.
    describe(item): a human-readable description of that item, attached
    to its exception (via add_note()) if it fails, so the failure is
    still identifiable even after being collected, re-raised, and
    possibly grouped with others.

    Every item is attempted regardless of an earlier one failing. Always
    raises at the end if anything failed; never logs, never suppresses --
    callers with an actual primary-failure signal to consult (i.e.
    run_pipelines()) decide what to do with whatever this raises.
    """
    errors = []
    for item in items:
        try:
            close_fn(item)
        except BaseException as e:
            # BaseException, not Exception: a KeyboardInterrupt/SystemExit
            # raised by one item's own close() must not stop every
            # subsequent item from getting its own close attempt, or
            # replace whatever this whole function's caller is already
            # propagating -- confirmed directly before fixing, the same
            # class of gap found in run_pipelines()'s own outer except:
            # clause. add_note() works identically on BaseException-only
            # types, verified directly, not assumed.
            e.add_note(describe(item))
            errors.append(e)

    if not errors:
        return
    if len(errors) == 1:
        raise errors[0]
    # BaseExceptionGroup, not ExceptionGroup: constructing ExceptionGroup
    # directly with a genuine BaseException-only member (KeyboardInterrupt
    # etc.) raises TypeError -- confirmed directly. BaseExceptionGroup
    # accepts any mix and automatically becomes a plain ExceptionGroup
    # instance when every member happens to be an Exception subclass
    # (confirmed directly, not assumed), so this one call correctly
    # covers both cases without needing to inspect errors first.
    raise BaseExceptionGroup('multiple cleanup steps failed', errors)
