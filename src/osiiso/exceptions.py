"""Public exception hierarchy — everything osiiso raises descends from :class:`OsiisoError`."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .result import TaskResult


class OsiisoError(Exception):
    """Base exception for the osiiso package."""


class ClosedError(OsiisoError):
    """Raised when a task is submitted to a queue that is closed or shutting down."""


class QueueFullError(OsiisoError):
    """Raised by :meth:`AsyncQueue.submit` when a bounded queue (``size > 0``) is full.

    Sync queues (:class:`~osiiso.ThreadQueue`, :class:`~osiiso.ProcessQueue`)
    block on submit instead of raising.
    """


class ExecutionError(OsiisoError):
    """Raised when one or more tasks fail (or are cancelled) during execution.

    Attributes:
        results: Tuple of the offending :class:`~osiiso.TaskResult` objects.
    """

    def __init__(self, results: list[TaskResult] | tuple[TaskResult, ...]):
        """Store the offending results and summarise their counts in the message.

        Args:
            results: The failed and/or cancelled results to report.
        """
        self.results = tuple(results)
        failed = sum(1 for r in self.results if r.status == "failed")
        cancelled = len(self.results) - failed
        parts = []
        if failed:
            parts.append(f"{failed} task(s) failed")
        if cancelled:
            parts.append(f"{cancelled} task(s) cancelled")
        super().__init__("; ".join(parts) or "0 task(s) failed")
