# 0003 — Releasing a workbook requires gc.collect()

Status: accepted

## Problem

Workbooks are read over SMB/DFS. Closing one with `wb.close()` was not
enough to release the underlying file handle on the remote share: the
handle stayed open, and a later run or a later process could not replace
the file.

## Decision

Every code path that opens a workbook explicitly drops its reference and
calls `gc.collect()` after closing, before the underlying stream's context
exits.

## Why

openpyxl builds reference cycles between the workbook, its worksheets and
their parents. Ordinary reference counting cannot collect a cycle, so the
`ZipFile` — and therefore the SMB handle — stays alive until the cyclic
collector runs. Left to its own schedule, that can be much later, or after
the process has already tried to reopen the file.

This is a production finding, not a theoretical one. It was found by files
failing to be replaced on the share.

## Consequences

- Both workbook paths carry the same treatment: the short-lived one in
  `xlsx_info()`, and the long-lived one in `open_workbook()` that
  `excel_resource` retains for its whole lifetime. Applying it only to the
  first — as was originally the case — protected the path where the bug
  was found rather than the path where the risk concentrates, since the
  retained workbook can hold a remote stream open for the entire run.
- **Dropping the reference is as important as the collect.** `del wb` sits
  in its own inner `finally`, so a raising `wb.close()` cannot skip it, and
  `excel_resource.close()` clears `self._wb` *before* triggering the
  collect — a live attribute at that moment defeats it entirely.
- **This is not a guarantee that the workbook is collectible**, only that
  nothing of ours still refers to it. On the path where `wb.close()`
  itself raises, the live exception traceback holds the failing method's
  frame, whose `self` is the workbook. Removing that would mean discarding
  the traceback, which costs more in diagnostics than it gains. The tests
  assert only that `wb` is gone as a local.
- `gc.collect()` is not free. It is called once per workbook open, not per
  sheet or per read.
- A test run emits occasional `ResourceWarning: unclosed file` originating
  at this `gc.collect()` call. That is the mechanism working, not a leak:
  the cycle collector is releasing a handle that reference counting did
  not, which is the entire reason the call exists. Suppressing the warning
  would hide the only visible evidence of it.
