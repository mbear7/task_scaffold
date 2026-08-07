# -*- coding: utf-8 -*-
"""Declarative publication policy: what to do, and how long to wait.

Split out of publish.py in 0.7.4. These are frozen dataclasses a task or
runner hands to the publisher; none of them opens a connection, and none
imports the publisher. Keeping them below it is what lets the layering test
catch a policy that starts reaching upward into publication mechanics.

PublisherConfig stays in publish.py, because its resolved_factory() defaults
to DbPublisher and the coupling is real rather than incidental.
"""

from dataclasses import dataclass
import math

from pathlib import Path

from task_core.db.identifiers import MAX_IDENTIFIER_BYTES
from task_core.db.values import DbPublishError


class PublicationPlan:
    """Work the runner needs performed inside the publication transaction.

    Source-state writing belongs to the runner and source_state.py, but it
    must land in the same transaction as the table swaps or a failed run
    could still advance the stored fingerprints. Queuing it here keeps
    commit()'s signature and the publisher protocol unchanged -- both of
    which were expanded by accident once already.
    """

    def __init__(self):
        self._steps = []

    def add(self, description, action):
        self._steps.append((description, action))

    def run(self, log):
        for description, action in self._steps:
            log.info('publication step: %s', description)
            action()

    def clear(self):
        self._steps = []

    def __len__(self):
        return len(self._steps)


@dataclass(frozen=True, kw_only=True)
class IdentifierPolicy:
    """The single source of truth for identifier rules, shared by
    class-level preflight and the publisher that will do the work.

    Previously the limit reached preflight through run_pipelines() and the
    publisher through its own constructor default, so
    db_max_identifier_bytes=40 validated declared names against 40 and
    everything discovered at runtime against 63. Two independently
    configured integers for one rule.

    Frozen, and deliberately does NOT hold the server-verified limit: that
    is resolved per connection and can only tighten this value. The policy
    is authoritative for static validation; the effective limit at DDL time
    is min(policy, server) and the publisher owns that derivation. Two
    policy objects in flight would be worse than one policy plus a
    documented derivation.
    """

    max_identifier_bytes: int = MAX_IDENTIFIER_BYTES

    def __post_init__(self):
        # `type(...) is int`, not isinstance: bool subclasses int, so
        # IdentifierPolicy(max_identifier_bytes=True) would otherwise
        # produce an effective one-byte limit rather than reject the config.
        if type(self.max_identifier_bytes) is not int or self.max_identifier_bytes < 1:
            raise DbPublishError(
                f'max_identifier_bytes must be a positive integer, '
                f'got {self.max_identifier_bytes!r}'
            )


DEFAULT_IDENTIFIER_POLICY = IdentifierPolicy()

@dataclass(frozen=True, kw_only=True)
class PublicationLockPolicy:
    """How long publication may wait for ACCESS EXCLUSIVE on its targets.

    PostgreSQL queues new ACCESS SHARE requests behind a waiting ACCESS
    EXCLUSIVE, so a publisher waiting on one long reader blocks every
    subsequent reader too. Bounding the wait is what stops one slow query
    turning a publication into a read outage.

    `retry_horizon_seconds` is the primary bound and gates COMPLETION of
    the lock phase, not merely permission to start another attempt --
    otherwise an attempt begun just inside the horizon could run well past
    it and the horizon would be a hint rather than a limit. The per-attempt
    timeouts are therefore ceilings: each attempt gets
    min(configured, time remaining), so a final attempt may run with far
    less than the configured budget.

    `max_attempts` is a defensive ceiling only, not the policy. Under these
    defaults it is unreachable -- a 1s minimum delay inside a 60s horizon
    admits far fewer -- and it exists to stop a runaway if someone
    configures a sub-second delay.
    """

    # Two different things, deliberately:
    #
    #   lock_timeout_ms       the PER-CONFLICT limit -- how long to wait
    #                         for any one target
    #   acquisition_timeout_ms the AGGREGATE multi-target budget: how long
    #                         the statement may spend waiting for the
    #                         complete lock set, and therefore how long an
    #                         already-acquired target blocks its own
    #                         readers while the statement waits for the
    #                         next one
    #
    # acquisition_timeout_ms is NOT the total reader-impact ceiling.
    # Acquired locks are held through the swap and commit that follow, and
    # both timeouts are reset once the set is complete, so the earliest
    # acquired target is blocked for acquisition + publication. The
    # aggregate bounds the WAITING half only; the critical section is
    # unbounded by this policy.
    #
    # Sizing, with:
    #
    #   L = per-conflict lock timeout
    #   A = complete lock-acquisition timeout
    #   M = execution and timeout-ordering margin
    #   n = actual existing targets in the LOCK TABLE statement
    #   P = post-acquisition publication duration
    #   B = accepted total reader-blocking budget
    #
    # Retry classification requires the hard runtime invariant:
    #
    #     n * L + M <= A
    #
    # Total reader blocking must separately satisfy:
    #
    #     A + P <= B
    #
    # A bounds acquisition waiting only. Locks already acquired remain held
    # through replacement or refill and commit. Replacement P is normally
    # catalog-time; explicit refill P is row- and index-dependent.
    #
    # The defaults support at most (5000 - 50) // 500 = 9 existing targets
    # in one publication. _lock_publication_targets() rejects a larger actual
    # lock set before requesting any live-target lock.
    lock_timeout_ms: int = 500
    acquisition_timeout_ms: int = 5_000
    retry_horizon_seconds: float = 60.0
    retry_delay_min_seconds: float = 1.0
    retry_delay_max_seconds: float = 5.0
    max_attempts: int = 100

    # Engineering margin reserved after the sum of all possible sequential
    # per-target waits. A single-wait ordering check alone is insufficient for
    # one LOCK TABLE statement containing multiple relations.
    TIMEOUT_MARGIN_MS = 50

    def __post_init__(self):
        for name in ('lock_timeout_ms', 'acquisition_timeout_ms', 'max_attempts'):
            value = getattr(self, name)
            # type(...) is int, not isinstance: bool subclasses int.
            if type(value) is not int or value < 1:
                raise DbPublishError(f'{name} must be a positive integer, got {value!r}')
        for name in ('retry_horizon_seconds', 'retry_delay_min_seconds',
                     'retry_delay_max_seconds'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise DbPublishError(f'{name} must be a number, got {value!r}')
            # isfinite: NaN and inf passed a type-and-sign check and then
            # failed later inside int(), random.uniform() or sleep(), far
            # from the configuration that caused them.
            if not math.isfinite(value) or value < 0:
                raise DbPublishError(f'{name} must be a finite non-negative number, got {value!r}')
        if self.retry_delay_min_seconds > self.retry_delay_max_seconds:
            raise DbPublishError(
                f'retry_delay_min_seconds ({self.retry_delay_min_seconds}) exceeds '
                f'retry_delay_max_seconds ({self.retry_delay_max_seconds})'
            )
        # PostgreSQL documents that a nonzero lock_timeout is pointless
        # once it reaches statement_timeout, because the statement timeout
        # fires first. Here that is not merely pointless but harmful: it
        # converts retryable 55P03 lock_not_available into terminal 57014
        # query_canceled, so ordinary contention would end the run instead
        # of being retried.
        if self.acquisition_timeout_ms < self.lock_timeout_ms + self.TIMEOUT_MARGIN_MS:
            raise DbPublishError(
                f'acquisition_timeout_ms ({self.acquisition_timeout_ms}) must exceed '
                f'lock_timeout_ms ({self.lock_timeout_ms}) by at least '
                f'{self.TIMEOUT_MARGIN_MS}ms. Otherwise statement_timeout fires first '
                f'and retryable lock contention (55P03) arrives as terminal '
                f'cancellation (57014).'
            )

    def attempt_budgets_ms(self, remaining_seconds, *, target_count=1):
        """Return ``(statement_timeout_ms, lock_timeout_ms)`` for one attempt.

        ``statement_timeout`` covers the complete multi-target statement while
        ``lock_timeout`` applies to each sequential acquisition. On a shortened
        final attempt the effective per-target timeout is reduced so the actual
        budgets still satisfy ``A >= n * L + M``.
        """
        if type(target_count) is not int or target_count < 1:
            raise DbPublishError(
                f'target_count must be a positive integer, got {target_count!r}'
            )

        remaining_ms = int(remaining_seconds * 1000)
        if remaining_ms <= 0:
            return None

        statement_ms = min(self.acquisition_timeout_ms, remaining_ms)
        available_for_waits = statement_ms - self.TIMEOUT_MARGIN_MS
        if available_for_waits < target_count:
            return None

        lock_ms = min(self.lock_timeout_ms, available_for_waits // target_count)
        if lock_ms < 1:
            return None
        return statement_ms, lock_ms


@dataclass(frozen=True, kw_only=True)
class CopyLoadPolicy:
    """Where and how the COPY loader spools rows before database transport.

    Three settings, all with defaults that keep every existing INSERT-path
    caller unchanged. Tasks using `db_loader='copy'` inherit these settings
    unless a task-level override is explicitly supplied.

    `spool_directory=None` means "resolve at consumption time via the
    platform tempdir". Nothing here creates the directory or touches
    the filesystem: construction stays pure so a configuration error is
    visible before any resource is built. Directory creation with
    mode 0o700 belongs in `resolve_spool_directory`.

    `buffer_bytes=1 MiB` sizes the I/O buffers on both spool write and
    replay. The bounded-memory contract (ADR 0011 §Local spool design)
    is "proportional to columns + one row + bounded I/O buffers, not
    to row count". This is the "bounded I/O buffers" number.

    A Path -- not str -- for spool_directory: PathLike is easy to
    accept and hard to be strict about later, and the boundary between
    "the config value" and "a filesystem path" is worth keeping crisp.
    A caller with a string can spell Path(s) themselves.

    `encrypt_spools=True` protects both spool bodies with independently
    generated AES-256-GCM keys. The ownership header remains plaintext so
    a successor run can identify and delete abandoned files after the key
    has disappeared. A task may explicitly opt out through
    `PipelineSpec.db_copy_spool_encryption=False`; the outer container and
    cleanup rules remain the same.
    """

    spool_directory: Path | None = None
    buffer_bytes: int = 1_048_576
    encrypt_spools: bool = True

    def __post_init__(self):
        if self.spool_directory is not None and not isinstance(self.spool_directory, Path):
            raise DbPublishError(
                f'spool_directory must be a pathlib.Path or None, '
                f'got {type(self.spool_directory).__name__}'
            )
        # `type(...) is int`, not isinstance: bool subclasses int, so
        # CopyLoadPolicy(buffer_bytes=True) would silently produce a
        # one-byte buffer. Same guard IdentifierPolicy uses on its own
        # int field, for the same reason.
        if type(self.buffer_bytes) is not int or self.buffer_bytes < 1:
            raise DbPublishError(
                f'buffer_bytes must be a positive integer, got {self.buffer_bytes!r}'
            )
        if type(self.encrypt_spools) is not bool:
            raise DbPublishError(
                f'encrypt_spools must be bool, got {self.encrypt_spools!r}'
            )
