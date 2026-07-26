"""Task handles — await, inspect, cancel, and observe submitted tasks.

* :class:`TaskHandle` — returned by :meth:`AsyncQueue.submit`; awaitable.
* :class:`SyncTaskHandle` — returned by :meth:`ThreadQueue.submit` /
  :meth:`ProcessQueue.submit`; blocks the calling thread.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from concurrent.futures import CancelledError
from logging import getLogger
from typing import Any, Literal

from .result import TaskResult

logger = getLogger(__name__)

TaskState = Literal["pending", "running", "retrying", "succeeded", "failed", "cancelled"]


class _BaseHandle:
    """State and behaviour shared by both handle flavours.

    Attributes:
        task_id: Unique hex identifier for this task.
        name: Human-readable name derived from the callable.
        priority: Scheduling priority (lower runs first).
        must_complete: ``True`` if the task is protected from cancellation.
        created_at: Monotonic timestamp of submission.
        group_id: Group this task belongs to, or ``None``.
        detached: Whether the result is excluded from ``run()`` summaries.
        scheduled_for: Absolute monotonic target start time, or ``None``.
        metadata: User data from :class:`~osiiso.TaskOptions.metadata`.
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
        "metadata",
        "_attempts",
        "_cancel_fn",
        "_lock",
        "_result",
        "_status",
        "_started_at",
        "_callbacks",
    )

    _pending_error: type[Exception] = RuntimeError
    _cancelled_error: type[BaseException] = asyncio.CancelledError

    def __init__(
        self,
        *,
        task_id: str,
        name: str,
        priority: int,
        must_complete: bool,
        created_at: float,
        cancel_fn: Callable[[str], bool],
        group_id: str | None = None,
        detached: bool = False,
        scheduled_for: float | None = None,
        metadata: Any = None,
    ) -> None:
        """Record the task's identity (see the class attributes) and initialise waiter state.

        Args:
            cancel_fn: Queue callback that cancels this task by id.
        """
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.must_complete = must_complete
        self.created_at = created_at
        self.group_id = group_id
        self.detached = detached
        self.scheduled_for = scheduled_for
        self.metadata = metadata
        self._attempts = 0
        self._cancel_fn = cancel_fn
        self._lock = threading.Lock()
        self._result: TaskResult | None = None
        self._status: TaskState = "pending"
        self._started_at: float | None = None
        self._callbacks: list[Callable[[Any], Any]] | None = None

    def __repr__(self) -> str:
        """Return ``Handle('name', status=..., id=...)`` with a shortened task id."""
        return f"{type(self).__name__}({self.name!r}, status={self._status!r}, id={self.task_id[:8]})"

    @property
    def status(self) -> TaskState:
        """Current lifecycle status of the task."""
        return self._status

    @property
    def attempts(self) -> int:
        """Number of execution attempts made so far."""
        return self._attempts

    def done(self) -> bool:
        """``True`` once the task has a final result."""
        return self._result is not None

    def cancelled(self) -> bool:
        """``True`` if the task was cancelled."""
        r = self._result
        return r is not None and r.status == "cancelled"

    def result(self) -> TaskResult:
        """Return the final :class:`~osiiso.TaskResult`.

        Raises:
            asyncio.InvalidStateError / RuntimeError: If not finished yet
                (async / sync handle respectively).
        """
        if self._result is None:
            raise self._pending_error("result not ready")
        return self._result

    def exception(self) -> BaseException | None:
        """Exception from a failed task, or ``None`` (raises if not done)."""
        return self.result().exception

    def value(self) -> Any:
        """The callable's return value; re-raises on failure or cancellation.

        Raises:
            asyncio.CancelledError / concurrent.futures.CancelledError: If the
                task was cancelled (async / sync handle respectively).
            Exception: The exception originally raised by the task.
        """
        r = self.result()
        if r.status == "cancelled":
            raise self._cancelled_error(r.message)
        if r.exception is not None:
            raise r.exception
        return r.value

    def cancel(self) -> bool:
        """Request cancellation; returns ``False`` if the task already finished."""
        if self.done():
            return False
        return bool(self._cancel_fn(self.task_id))

    def add_done_callback(self, fn: Callable[[Any], Any]) -> None:
        """Call ``fn(handle)`` when the task finishes (immediately if already done).

        Callbacks run in the thread (or event loop) that resolved the task;
        exceptions are logged and swallowed.
        """
        with self._lock:
            if self._result is None:
                if self._callbacks is None:
                    self._callbacks = []
                self._callbacks.append(fn)
                return
        self._invoke_callback(fn)

    def _invoke_callback(self, fn: Callable[[Any], Any]) -> None:
        """Call ``fn(self)``, logging (never propagating) exceptions."""
        try:
            fn(self)
        except Exception:
            logger.exception("done callback raised for task %s", self.name)

    def _mark_running(self) -> None:
        """Count a new attempt, switch to ``running``, and stamp the first start time."""
        self._attempts += 1
        self._status = "running"
        if self._started_at is None:
            self._started_at = time.perf_counter()

    def _mark_retrying(self) -> None:
        """Switch to ``retrying`` while waiting between attempts."""
        self._status = "retrying"

    def _mark_finished(self, result: TaskResult) -> bool:
        """Set the final result once; wake waiters and fire callbacks.

        Args:
            result: The outcome to publish.

        Returns:
            ``True`` if this call set the result, ``False`` if it was already set.
        """
        with self._lock:
            if self._result is not None:
                return False
            self._result = result
            self._status = result.status
            callbacks, self._callbacks = self._callbacks, None
            self._finish_locked()
        self._finish_unlocked()
        if callbacks:
            for cb in callbacks:
                self._invoke_callback(cb)
        return True

    def _finish_locked(self) -> None:
        """Subclass hook run while the handle lock is held."""

    def _finish_unlocked(self) -> None:
        """Subclass hook run after the handle lock is released."""


class TaskHandle(_BaseHandle):
    """Awaitable handle returned by :meth:`AsyncQueue.submit`.

    ``await handle`` is equivalent to ``await handle.wait()``.  Thread-safe:
    the result may be resolved from another thread.
    """

    __slots__ = ("_waiters", "_resolved")

    _pending_error = asyncio.InvalidStateError

    def __init__(self, **kwargs: Any) -> None:
        """Initialise the handle with an empty waiter set (see :class:`_BaseHandle`)."""
        super().__init__(**kwargs)
        self._waiters: set[asyncio.Future[TaskResult]] = set()
        self._resolved: tuple[asyncio.Future[TaskResult], ...] = ()

    def __await__(self):
        """Delegate to :meth:`wait`, so ``await handle`` yields the result."""
        return self.wait().__await__()

    async def wait(self) -> TaskResult:
        """Await completion and return the :class:`~osiiso.TaskResult`.

        Returns immediately if already done; safe to await from many coroutines.
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

    def _finish_locked(self) -> None:
        """Snapshot and clear the pending waiters (lock held)."""
        self._resolved = tuple(self._waiters)
        self._waiters.clear()

    def _finish_unlocked(self) -> None:
        """Resolve the snapshotted waiters on their own loops, skipping dead ones."""
        result = self._result
        waiters, self._resolved = self._resolved, ()
        for w in waiters:
            loop = w.get_loop()
            if w.done() or loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(_resolve, w, result)
            except RuntimeError:
                continue


def _resolve(waiter: asyncio.Future[TaskResult], result: TaskResult) -> None:
    """Set *result* on *waiter* unless it is already done (runs on the waiter's loop)."""
    if not waiter.done():
        waiter.set_result(result)


class SyncTaskHandle(_BaseHandle):
    """Blocking handle returned by :meth:`ThreadQueue.submit` / :meth:`ProcessQueue.submit`."""

    __slots__ = ("_cond",)

    _pending_error = RuntimeError
    _cancelled_error = CancelledError

    def __init__(self, **kwargs: Any) -> None:
        """Initialise the handle with a condition over the shared lock (see :class:`_BaseHandle`)."""
        super().__init__(**kwargs)
        self._cond = threading.Condition(self._lock)

    def wait(self, timeout: float | None = None) -> TaskResult:
        """Block until the task finishes and return its :class:`~osiiso.TaskResult`.

        Args:
            timeout: Seconds to wait, or ``None`` to wait indefinitely.

        Raises:
            TimeoutError: If *timeout* seconds elapse first.
        """
        with self._cond:
            if not self._cond.wait_for(lambda: self._result is not None, timeout=timeout):
                raise TimeoutError("result not ready")
            return self._result  # type: ignore[return-value]

    def _finish_locked(self) -> None:
        """Wake every thread blocked in :meth:`wait` (lock held)."""
        self._cond.notify_all()
