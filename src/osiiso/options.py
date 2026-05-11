"""TaskOptions — reusable, immutable configuration bundle for submitted tasks.

Create a :class:`TaskOptions` instance once and reuse it across many
``submit()`` calls, or pass option fields directly as keyword arguments to
``submit()``.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TaskOptions:
    """Reusable, immutable configuration for task submission.

    Create once and reuse across many ``submit()`` calls, or pass individual
    fields as keyword arguments to ``submit()`` directly.

    Attributes:
        priority: Scheduling priority; lower values run first. Default ``3``.
        must_complete: If ``True``, the task bypasses cancellation on shutdown.
        timeout: Per-task execution time limit in seconds.  ``None`` = no limit.
        retries: Number of additional attempts after an initial failure.
        retry_delay: Seconds to wait before the first retry.
        backoff: Multiplier applied to *retry_delay* on each subsequent retry.
        delay: Schedule the task *delay* seconds from now.  Mutually
            exclusive with *run_at*.
        run_at: Absolute ``time.time()`` timestamp at which to run the task.
            Mutually exclusive with *delay*.
        name: Override the auto-derived task name used in results and logs.
        group_id: Associate this task with a named group.
        detached: If ``True``, the task's result is excluded from ``run()``.

    Example::

        retry_opts = TaskOptions(retries=3, retry_delay=1.0, backoff=2.0)
        q.submit(fetch, url1, opts=retry_opts)
        q.submit(fetch, url2, opts=retry_opts)

        # Or inline — same effect:
        q.submit(fetch, url1, retries=3, retry_delay=1.0, backoff=2.0)
    """

    priority: int = 3
    must_complete: bool = False
    timeout: float | None = None
    retries: int = 0
    retry_delay: float = 0.0
    backoff: float = 1.0
    delay: float | None = None
    run_at: float | None = None
    name: str | None = None
    group_id: str | None = None
    detached: bool = False

    def __post_init__(self) -> None:
        """Validate field values after construction.

        Raises:
            ValueError: If any field value is outside the allowed range, or
                if both *delay* and *run_at* are specified simultaneously.
        """
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        if self.retries < 0:
            raise ValueError("retries must be >= 0")
        if self.retry_delay < 0:
            raise ValueError("retry_delay must be >= 0")
        if self.backoff <= 0:
            raise ValueError("backoff must be > 0")
        if self.delay is not None and self.delay < 0:
            raise ValueError("delay must be >= 0")
        if self.delay is not None and self.run_at is not None:
            raise ValueError("delay and run_at are mutually exclusive")

    def replace(self, **overrides: object) -> TaskOptions:
        """Return a copy with the given fields replaced."""
        return dataclasses.replace(self, **overrides)


# Set of valid field names, used to reject unknown kwargs passed to submit().
OPTION_FIELDS: frozenset[str] = frozenset(f.name for f in dataclasses.fields(TaskOptions))


def resolve_opts(opts: TaskOptions | None, overrides: dict[str, object]) -> TaskOptions:
    """Merge an optional base :class:`TaskOptions` with keyword overrides.

    Internal helper used by ``submit()``, ``map()``, and ``group()``.

    Args:
        opts: Optional base options.  ``None`` is treated as ``TaskOptions()``.
        overrides: Keyword arguments that override fields in *opts*.

    Returns:
        A resolved :class:`TaskOptions` with all overrides applied.

    Raises:
        TypeError: If any key in *overrides* is not a recognised
            :class:`TaskOptions` field name.
    """
    unknown = set(overrides) - OPTION_FIELDS
    if unknown:
        raise TypeError(f"Unknown task option(s): {', '.join(sorted(unknown))}")
    if not overrides:
        return opts or TaskOptions()
    if opts is None:
        return TaskOptions(**overrides)
    return dataclasses.replace(opts, **overrides)
