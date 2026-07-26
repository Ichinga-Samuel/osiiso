"""TaskOptions — reusable, immutable configuration bundle for submitted tasks."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TaskOptions:
    """Immutable per-task configuration.

    Create once and reuse across many ``submit()`` calls, or pass individual
    fields as keyword arguments to ``submit()`` / ``map()`` / ``group()``.

    Attributes:
        priority: Scheduling priority; lower values run first. Default ``3``.
        must_complete: If ``True``, the task survives timeouts, ``fail_first``
            aborts, and graceful cancellation. Only ``shutdown(force=True)``
            or an explicit ``handle.cancel()`` stops it.
        timeout: Per-attempt execution time limit in seconds (``None`` = no limit).
        retries: Additional attempts after the initial failure.
        retry_delay: Seconds to wait before the first retry.
        backoff: Multiplier applied to *retry_delay* after each retry.
        delay: Run the task *delay* seconds from now (exclusive with *run_at*).
        run_at: Absolute ``time.time()`` timestamp to run at (exclusive with *delay*).
        name: Override the auto-derived task name.
        group_id: Associate the task with a named group.
        detached: If ``True``, the task still runs (and ``run()`` waits for it)
            but its result is excluded from the :class:`~osiiso.RunSummary`.
            Observe it through its handle instead.
        metadata: Arbitrary user data carried onto the handle and result.

    Example::

        retry_opts = TaskOptions(retries=3, retry_delay=1.0, backoff=2.0)
        q.submit(fetch, url1, opts=retry_opts)
        q.submit(fetch, url2, retries=3, retry_delay=1.0)  # inline, same effect
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
    metadata: Any = None

    def __post_init__(self) -> None:
        """Validate the field ranges and the ``delay``/``run_at`` exclusivity.

        Raises:
            ValueError: If a field is out of range or both scheduling fields are set.
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

    def replace(self, **overrides: Any) -> TaskOptions:
        """Return a copy with the given fields replaced.

        Args:
            **overrides: Field values to change; everything else is copied as is.

        Returns:
            A new, validated :class:`TaskOptions`.
        """
        return dataclasses.replace(self, **overrides)


OPTION_FIELDS: frozenset[str] = frozenset(f.name for f in dataclasses.fields(TaskOptions))

_DEFAULT = TaskOptions()


def resolve_opts(opts: TaskOptions | None, overrides: dict[str, Any]) -> TaskOptions:
    """Merge a base :class:`TaskOptions` with keyword overrides (overrides win).

    Args:
        opts: The base options, or ``None`` to start from the defaults.
        overrides: Option field names mapped to their overriding values.

    Returns:
        The effective options for a submission.

    Raises:
        TypeError: If *overrides* contains a name that is not a TaskOptions field.
    """
    unknown = set(overrides) - OPTION_FIELDS
    if unknown:
        raise TypeError(f"Unknown task option(s): {', '.join(sorted(unknown))}")
    if not overrides:
        return opts or _DEFAULT
    if opts is None:
        return TaskOptions(**overrides)
    return dataclasses.replace(opts, **overrides)
