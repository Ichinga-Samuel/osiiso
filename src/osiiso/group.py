"""Task groups and completion-order iteration over handles."""

from __future__ import annotations

import asyncio
import queue as queue_mod
import time
from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any

from .handle import SyncTaskHandle, TaskHandle
from .result import RunSummary, TaskResult


async def as_completed(handles: Iterable[TaskHandle]) -> AsyncIterator[TaskHandle]:
    """Yield async handles in completion order, fastest first.

    Args:
        handles: The handles to await.

    Yields:
        Each :class:`~osiiso.TaskHandle` as it finishes.
    """
    pending = {asyncio.ensure_future(h.wait()): h for h in handles}
    try:
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                yield pending.pop(fut)
    finally:
        for fut in pending:
            fut.cancel()


def iter_completed(handles: Iterable[SyncTaskHandle], timeout: float | None = None) -> Iterator[SyncTaskHandle]:
    """Yield sync handles in completion order, blocking the calling thread.

    Args:
        handles: The handles to wait on.
        timeout: Overall budget in seconds for the whole iteration.

    Yields:
        Each :class:`~osiiso.SyncTaskHandle` as it finishes.

    Raises:
        TimeoutError: If *timeout* elapses before every handle finishes.
    """
    items = list(handles)
    done_q: queue_mod.Queue[SyncTaskHandle] = queue_mod.Queue()
    for h in items:
        h.add_done_callback(done_q.put)
    deadline = None if timeout is None else time.perf_counter() + timeout
    for _ in items:
        remaining = None if deadline is None else max(0.0, deadline - time.perf_counter())
        try:
            yield done_q.get(timeout=remaining)
        except queue_mod.Empty:
            raise TimeoutError("handles did not complete within timeout") from None


class _GroupBase:
    """State shared by both group flavours.

    Attributes:
        group_id: Unique identifier for this group.
        handles: Immutable tuple of the constituent handles, in submission order.
    """

    __slots__ = ("group_id", "handles")

    def __init__(self, group_id: str, handles: Iterable[Any]) -> None:
        """Store *group_id* and freeze *handles* into a tuple."""
        self.group_id = group_id
        self.handles = tuple(handles)

    def __iter__(self):
        """Iterate over the group's handles in submission order."""
        return iter(self.handles)

    def __len__(self) -> int:
        """Number of handles in the group."""
        return len(self.handles)

    def __repr__(self) -> str:
        """Return ``Group('group_id', tasks=N)``."""
        return f"{type(self).__name__}({self.group_id!r}, tasks={len(self)})"

    def cancel(self) -> int:
        """Request cancellation of every handle; returns how many were accepted."""
        return sum(1 for h in self.handles if h.cancel())


class TaskGroup(_GroupBase):
    """Async group handle returned by :meth:`AsyncQueue.group`."""

    async def wait(self) -> RunSummary:
        """Await all handles; returns a :class:`~osiiso.RunSummary` in submission order."""
        start = time.perf_counter()
        results = [await h.wait() for h in self.handles]
        return RunSummary.from_results(results, run_start=start, timed_out=False)

    async def values(self) -> tuple[Any, ...]:
        """Await all handles and return their values in submission order.

        Raises:
            ~osiiso.ExecutionError: If any task failed or was cancelled.
        """
        summary = await self.wait()
        summary.raise_for_errors(include_cancelled=True)
        return summary.values

    def as_completed(self) -> AsyncIterator[TaskHandle]:
        """Yield this group's handles in completion order."""
        return as_completed(self.handles)


class SyncTaskGroup(_GroupBase):
    """Blocking group handle returned by :meth:`ThreadQueue.group` / :meth:`ProcessQueue.group`."""

    def wait(self, timeout: float | None = None) -> RunSummary:
        """Block until all handles finish; *timeout* is a shared budget for the whole group.

        Args:
            timeout: Seconds allowed for the whole group, or ``None`` for no limit.

        Returns:
            A :class:`~osiiso.RunSummary` in submission order.

        Raises:
            TimeoutError: If the budget is exhausted first.
        """
        start = time.perf_counter()
        deadline = None if timeout is None else start + timeout
        results: list[TaskResult] = []
        for h in self.handles:
            remaining = None if deadline is None else max(0.0, deadline - time.perf_counter())
            results.append(h.wait(timeout=remaining))
        return RunSummary.from_results(results, run_start=start, timed_out=False)

    def values(self, timeout: float | None = None) -> tuple[Any, ...]:
        """Block until done and return values in submission order.

        Args:
            timeout: Seconds allowed for the whole group, or ``None`` for no limit.

        Raises:
            ~osiiso.ExecutionError: If any task failed or was cancelled.
            TimeoutError: If *timeout* elapses first.
        """
        summary = self.wait(timeout=timeout)
        summary.raise_for_errors(include_cancelled=True)
        return summary.values

    def as_completed(self, timeout: float | None = None) -> Iterator[SyncTaskHandle]:
        """Yield this group's handles in completion order, blocking the calling thread.

        Args:
            timeout: Overall budget in seconds; :class:`TimeoutError` if it elapses.
        """
        return iter_completed(self.handles, timeout=timeout)
