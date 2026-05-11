"""Task handles — lightweight objects for awaiting, inspecting, and cancelling tasks.

This module defines two handle classes:

* :class:`TaskHandle` — returned by :meth:`~osiiso.AsyncQueue.submit`;
  awaitable from asyncio coroutines.
* :class:`SyncTaskHandle` — returned by :meth:`~osiiso.ThreadQueue.submit`
  and :meth:`~osiiso.ProcessQueue.submit`; blocks the calling thread.

Handles are the primary way callers observe the lifecycle of a submitted task.
"""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import CancelledError
from typing import Any, Literal

from .result import TaskResult

TaskStatus = Literal["pending", "running", "retrying", "succeeded", "failed", "cancelled"]


class TaskHandle:
    """Async handle returned by :meth:`~osiiso.AsyncQueue.submit`.

    Awaitable — ``await handle`` is equivalent to ``await handle.wait()``.
    Thread-safe: the result can be resolved by a background thread while
    an asyncio task awaits it.

    Attributes:
        task_id: Unique hex identifier for this task.
        name: Human-readable name derived from the callable.
        priority: Scheduling priority (lower number = higher priority).
        must_complete: If ``True`` the task is not cancelled during shutdown.
        created_at: ``perf_counter`` timestamp of submission.
        group_id: Group this task belongs to, or ``None``.
        detached: Whether the result is excluded from ``run()`` aggregation.
        scheduled_for: Absolute ``perf_counter`` target start time, or ``None``.
    """

    __slots__ = (
        "task_id",
        "name",
        "priority",
        "must_complete",
        "created_at",
        "group_id",
        "detached",
        "scheduled_for",
        "_attempts",
        "_cancel_fn",
        "_waiters",
        "_lock",
        "_result",
        "_status",
        "_started_at",
    )

    def __init__(
        self,
        *,
        task_id: str,
        name: str,
        priority: int,
        must_complete: bool,
        created_at: float,
        cancel_fn: Any,
        group_id: str | None = None,
        detached: bool = False,
        scheduled_for: float | None = None,
    ) -> None:
        """Create a handle.  Called internally by the queue.

        Args:
            task_id: UUID hex string for the task.
            name: Human-readable task name.
            priority: Scheduling priority integer.
            must_complete: Whether to protect the task from cancellation.
            created_at: ``perf_counter`` submission timestamp.
            cancel_fn: Callable ``(task_id) -> bool`` used by :meth:`cancel`.
            group_id: Optional group this task belongs to.
            detached: If ``True``, excluded from ``run()`` aggregation.
            scheduled_for: Absolute ``perf_counter`` start time, or ``None``.
        """
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.must_complete = must_complete
        self.created_at = created_at
        self.group_id = group_id
        self.detached = detached
        self.scheduled_for = scheduled_for
        self._attempts = 0
        self._cancel_fn = cancel_fn
        self._waiters: set[asyncio.Future[TaskResult]] = set()
        self._lock = threading.Lock()
        self._result: TaskResult | None = None
        self._status: TaskStatus = "pending"
        self._started_at: float | None = None

    def __repr__(self) -> str:
        return f"TaskHandle({self.name!r}, status={self.status!r}, id={self.task_id[:8]})"

    def __await__(self):
        return self.wait().__await__()

    @property
    def status(self) -> TaskStatus:
        """Current lifecycle status of the task."""
        return self._status

    @property
    def attempts(self) -> int:
        """Number of execution attempts made so far (including the current one)."""
        return self._attempts

    def done(self) -> bool:
        """Return ``True`` if the task has a final result (succeeded, failed, or cancelled)."""
        return self._result is not None

    def cancelled(self) -> bool:
        """Return ``True`` if the task was cancelled."""
        r = self._result
        return r is not None and r.status == "cancelled"

    def exception(self) -> BaseException | None:
        """Return the exception from a failed task, or ``None`` on success.

        Raises:
            asyncio.InvalidStateError: If the task has not finished yet.
        """
        return self.result().exception

    def result(self) -> TaskResult:
        """Return the :class:`~osiiso.TaskResult` for this task.

        Raises:
            asyncio.InvalidStateError: If the task has not finished yet.
        """
        if self._result is None:
            raise asyncio.InvalidStateError("result not ready")
        return self._result

    def value(self) -> Any:
        """Return the callable's return value, re-raising on failure or cancellation.

        Returns:
            The value returned by the task callable.

        Raises:
            asyncio.InvalidStateError: If the task has not finished yet.
            asyncio.CancelledError: If the task was cancelled.
            Exception: The exception originally raised by the task.
        """
        r = self.result()
        if r.status == "cancelled":
            raise asyncio.CancelledError(r.message)
        if r.exception is not None:
            raise r.exception
        return r.value

    async def wait(self) -> TaskResult:
        """Await the task and return its :class:`~osiiso.TaskResult`.

        Returns immediately if the task is already done.  Safe to call from
        multiple coroutines concurrently.

        Returns:
            The final :class:`~osiiso.TaskResult` for this task.
        """
        if self._result is not None:
            return self._result
        waiter = asyncio.get_running_loop().create_future()
        with self._lock:
            if self._result is not None:
                return self._result
            self._waiters.add(waiter)
        try:
            return await waiter
        finally:
            with self._lock:
                self._waiters.discard(waiter)

    def cancel(self) -> bool:
        """Request cancellation of this task.

        Returns:
            ``True`` if the cancellation request was accepted, ``False`` if
            the task was already done.
        """
        if self.done():
            return False
        return bool(self._cancel_fn(self.task_id))

    def _mark_running(self) -> None:
        self._attempts += 1
        self._status = "running"
        if self._started_at is None:
            self._started_at = time.perf_counter()

    def _mark_retrying(self) -> None:
        self._status = "retrying"

    def _mark_finished(self, result: TaskResult) -> None:
        with self._lock:
            if self._result is not None:
                return
            self._result = result
            self._status = result.status
            waiters = tuple(self._waiters)
            self._waiters.clear()
        for w in waiters:
            loop = w.get_loop()
            if w.done() or loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(self._resolve, w, result)
            except RuntimeError:
                continue

    @staticmethod
    def _resolve(waiter: asyncio.Future[TaskResult], result: TaskResult) -> None:
        if not waiter.done():
            waiter.set_result(result)


class SyncTaskHandle:
    """Blocking handle returned by :meth:`~osiiso.ThreadQueue.submit` and
    :meth:`~osiiso.ProcessQueue.submit`.

    All methods block the calling thread.  Thread-safe.

    Attributes:
        task_id: Unique hex identifier for this task.
        name: Human-readable name derived from the callable.
        priority: Scheduling priority (lower number = higher priority).
        must_complete: If ``True`` the task is not cancelled during shutdown.
        created_at: ``perf_counter`` timestamp of submission.
        group_id: Group this task belongs to, or ``None``.
        detached: Whether the result is excluded from ``run()`` aggregation.
        scheduled_for: Absolute ``perf_counter`` target start time, or ``None``.
    """

    __slots__ = (
        "task_id",
        "name",
        "priority",
        "must_complete",
        "created_at",
        "group_id",
        "detached",
        "scheduled_for",
        "_attempts",
        "_cancel_fn",
        "_condition",
        "_result",
        "_status",
        "_started_at",
    )

    def __init__(
        self,
        *,
        task_id: str,
        name: str,
        priority: int,
        must_complete: bool,
        created_at: float,
        cancel_fn: Any,
        group_id: str | None = None,
        detached: bool = False,
        scheduled_for: float | None = None,
    ) -> None:
        """Create a handle.  Called internally by the queue.

        Args:
            task_id: UUID hex string for the task.
            name: Human-readable task name.
            priority: Scheduling priority integer.
            must_complete: Whether to protect the task from cancellation.
            created_at: ``perf_counter`` submission timestamp.
            cancel_fn: Callable ``(task_id) -> bool`` used by :meth:`cancel`.
            group_id: Optional group this task belongs to.
            detached: If ``True``, excluded from ``run()`` aggregation.
            scheduled_for: Absolute ``perf_counter`` start time, or ``None``.
        """
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.must_complete = must_complete
        self.created_at = created_at
        self.group_id = group_id
        self.detached = detached
        self.scheduled_for = scheduled_for
        self._attempts = 0
        self._cancel_fn = cancel_fn
        self._condition = threading.Condition()
        self._result: TaskResult | None = None
        self._status: TaskStatus = "pending"
        self._started_at: float | None = None

    def __repr__(self) -> str:
        return f"SyncTaskHandle({self.name!r}, status={self.status!r}, id={self.task_id[:8]})"

    @property
    def status(self) -> TaskStatus:
        """Current lifecycle status of the task."""
        return self._status

    @property
    def attempts(self) -> int:
        """Number of execution attempts made so far."""
        return self._attempts

    def done(self) -> bool:
        """Return ``True`` if the task has a final result."""
        with self._condition:
            return self._result is not None

    def cancelled(self) -> bool:
        """Return ``True`` if the task was cancelled."""
        with self._condition:
            return self._result is not None and self._result.status == "cancelled"

    def exception(self) -> BaseException | None:
        """Return the exception from a failed task, or ``None``.

        Raises:
            RuntimeError: If the task has not finished yet.
        """
        return self.result().exception

    def result(self) -> TaskResult:
        """Return the :class:`~osiiso.TaskResult` for this task.

        Raises:
            RuntimeError: If the task has not finished yet.
        """
        with self._condition:
            if self._result is None:
                raise RuntimeError("result not ready")
            return self._result

    def value(self) -> Any:
        """Return the callable's return value, re-raising on failure or cancellation.

        Returns:
            The value returned by the task callable.

        Raises:
            RuntimeError: If the task has not finished yet.
            concurrent.futures.CancelledError: If the task was cancelled.
            Exception: The exception originally raised by the task.
        """
        r = self.result()
        if r.status == "cancelled":
            raise CancelledError(r.message)
        if r.exception is not None:
            raise r.exception
        return r.value

    def wait(self, timeout: float | None = None) -> TaskResult:
        """Block until the task finishes and return its :class:`~osiiso.TaskResult`.

        Args:
            timeout: Maximum seconds to wait.  ``None`` means wait indefinitely.

        Returns:
            The final :class:`~osiiso.TaskResult` for this task.

        Raises:
            TimeoutError: If *timeout* expires before the task finishes.
        """
        with self._condition:
            if self._result is None:
                ok = self._condition.wait_for(lambda: self._result is not None, timeout=timeout)
                if not ok:
                    raise TimeoutError("result not ready")
            return self._result  # type: ignore[return-value]

    def cancel(self) -> bool:
        """Request cancellation of this task.

        Returns:
            ``True`` if the cancellation request was accepted, ``False`` if
            the task was already done.
        """
        if self.done():
            return False
        return bool(self._cancel_fn(self.task_id))

    def _mark_running(self) -> None:
        self._attempts += 1
        self._status = "running"
        if self._started_at is None:
            self._started_at = time.perf_counter()

    def _mark_retrying(self) -> None:
        self._status = "retrying"

    def _mark_finished(self, result: TaskResult) -> None:
        with self._condition:
            if self._result is not None:
                return
            self._result = result
            self._status = result.status
            self._condition.notify_all()
