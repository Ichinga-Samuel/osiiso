"""Task groups — structured handles over a batch of submitted tasks.

This module provides :class:`TaskGroup` (for :class:`~osiiso.AsyncQueue`) and
:class:`SyncTaskGroup` (for :class:`~osiiso.ThreadQueue` /
:class:`~osiiso.ProcessQueue`).  Both classes let callers wait on, cancel, or
collect the results of an entire batch through a single handle.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

from .handle import SyncTaskHandle, TaskHandle
from .result import RunSummary, TaskResult


class TaskGroup:
    """Async group handle returned by :meth:`AsyncQueue.group`.

    Holds a snapshot of :class:`~osiiso.TaskHandle` objects submitted as a
    logical unit.  Use :meth:`wait` to await all handles or :meth:`values` to
    get their return values (raising on any failure).

    Attributes:
        group_id: Unique identifier for this group.
        handles: Immutable tuple of the constituent :class:`~osiiso.TaskHandle` objects.
    """

    __slots__ = ("group_id", "handles")

    def __init__(self, group_id: str, handles: Iterable[TaskHandle]) -> None:
        """Initialize the group.

        Args:
            group_id: A string identifier for this group (auto-generated if
                not supplied by the caller).
            handles: The :class:`~osiiso.TaskHandle` objects that belong to
                this group.
        """
        self.group_id = group_id
        self.handles = tuple(handles)

    def __iter__(self):
        return iter(self.handles)

    def __len__(self) -> int:
        return len(self.handles)

    def __repr__(self) -> str:
        return f"TaskGroup({self.group_id!r}, tasks={len(self)})"

    def cancel(self) -> int:
        """Request cancellation of every handle in the group.

        Returns:
            The number of handles that were successfully cancelled
            (already-finished handles count as 0).
        """
        return sum(1 for h in self.handles if h.cancel())

    async def wait(self) -> RunSummary:
        """Await all handles and return a :class:`~osiiso.RunSummary`.

        Returns:
            A :class:`~osiiso.RunSummary` aggregating the results of every
            task in the group.
        """
        start = time.perf_counter()
        results = [await h.wait() for h in self.handles]
        return RunSummary.from_results(results, run_start=start, timed_out=False)

    async def values(self) -> tuple[Any, ...]:
        """Await all handles and return only their return values.

        Returns:
            A tuple of return values from every succeeded task, in submission
            order.

        Raises:
            ~osiiso.ExecutionError: If any task in the group failed.
        """
        summary = await self.wait()
        summary.raise_for_errors()
        return summary.values


class SyncTaskGroup:
    """Blocking group handle returned by :meth:`ThreadQueue.group` / :meth:`ProcessQueue.group`.

    Holds a snapshot of :class:`~osiiso.SyncTaskHandle` objects submitted as a
    logical unit.  All methods block the calling thread.

    Attributes:
        group_id: Unique identifier for this group.
        handles: Immutable tuple of the constituent
            :class:`~osiiso.SyncTaskHandle` objects.
    """

    __slots__ = ("group_id", "handles")

    def __init__(self, group_id: str, handles: Iterable[SyncTaskHandle]) -> None:
        """Initialize the group.

        Args:
            group_id: A string identifier for this group.
            handles: The :class:`~osiiso.SyncTaskHandle` objects that belong
                to this group.
        """
        self.group_id = group_id
        self.handles = tuple(handles)

    def __iter__(self):
        return iter(self.handles)

    def __len__(self) -> int:
        return len(self.handles)

    def __repr__(self) -> str:
        return f"SyncTaskGroup({self.group_id!r}, tasks={len(self)})"

    def cancel(self) -> int:
        """Request cancellation of every handle in the group.

        Returns:
            The number of handles that were successfully cancelled.
        """
        return sum(1 for h in self.handles if h.cancel())

    def wait(self, timeout: float | None = None) -> RunSummary:
        """Block until all handles finish and return a :class:`~osiiso.RunSummary`.

        The *timeout* budget is shared across all handles — each handle gets
        the remaining wall-clock time from the original budget.

        Args:
            timeout: Maximum seconds to wait for the entire group.  ``None``
                means wait indefinitely.

        Returns:
            A :class:`~osiiso.RunSummary` aggregating the results of every
            task in the group.

        Raises:
            TimeoutError: If a handle's remaining time budget is exhausted
                before its task finishes.
        """
        start = time.perf_counter()
        deadline = None if timeout is None else time.perf_counter() + timeout
        results: list[TaskResult] = []
        for h in self.handles:
            remaining = None if deadline is None else max(0.0, deadline - time.perf_counter())
            results.append(h.wait(timeout=remaining))
        return RunSummary.from_results(results, run_start=start, timed_out=False)

    def values(self, timeout: float | None = None) -> tuple[Any, ...]:
        """Block until all handles finish and return only their return values.

        Args:
            timeout: Maximum seconds to wait for the entire group.

        Returns:
            A tuple of return values from every succeeded task, in submission
            order.

        Raises:
            ~osiiso.ExecutionError: If any task in the group failed.
            TimeoutError: If the timeout expires before all tasks complete.
        """
        summary = self.wait(timeout=timeout)
        summary.raise_for_errors()
        return summary.values
