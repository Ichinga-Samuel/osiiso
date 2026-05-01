from __future__ import annotations

import asyncio
import itertools
import os
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from logging import getLogger
from threading import Lock, get_ident
from typing import Any, TypeVar

from ._base import (
    QueueMode,
    QueueRunSummary,
    ShutdownPolicy,
    TaskResult,
    TaskStatus,
    _make_result,
)
from .exceptions import QueueClosedError
from .queue_item import QueueItem

logger = getLogger(__name__)
T = TypeVar("T")


class TaskHandle:
    """A lightweight handle for awaiting, inspecting, or cancelling a queued task."""

    __slots__ = (
        "task_id",
        "name",
        "priority",
        "must_complete",
        "created_at",
        "_attempts",
        "_cancel_callback",
        "_waiters",
        "_state_lock",
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
        cancel_callback: Any,
    ) -> None:
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.must_complete = must_complete
        self.created_at = created_at
        self._attempts = 0
        self._cancel_callback = cancel_callback
        self._waiters: set[asyncio.Future[TaskResult]] = set()
        self._state_lock = Lock()
        self._result: TaskResult | None = None
        self._status: TaskStatus = "pending"
        self._started_at: float | None = None

    def __repr__(self) -> str:
        return (
            "TaskHandle("
            f"task_id={self.task_id!r}, name={self.name!r}, status={self.status!r}, "
            f"attempts={self.attempts}, priority={self.priority})"
        )

    def __await__(self):
        return self.wait().__await__()

    @property
    def status(self) -> TaskStatus:
        return self._status

    @property
    def attempts(self) -> int:
        return self._attempts

    def done(self) -> bool:
        return self._result is not None

    def cancelled(self) -> bool:
        return self._result is not None and self._result.status == "cancelled"

    def exception(self) -> BaseException | None:
        return self.result().exception

    def result(self) -> TaskResult:
        if self._result is None:
            raise asyncio.InvalidStateError("Task result is not ready yet")
        return self._result

    def value(self) -> Any:
        result = self.result()
        if result.status == "cancelled":
            raise asyncio.CancelledError(result.message)
        if result.exception is not None:
            raise result.exception
        return result.value

    async def wait(self) -> TaskResult:
        if self._result is not None:
            return self._result

        waiter = asyncio.get_running_loop().create_future()
        with self._state_lock:
            if self._result is not None:
                return self._result
            self._waiters.add(waiter)
        try:
            return await waiter
        finally:
            with self._state_lock:
                self._waiters.discard(waiter)

    def cancel(self) -> bool:
        if self.done():
            return False
        return bool(self._cancel_callback(self.task_id))

    def _mark_running(self) -> None:
        self._attempts += 1
        self._status = "running"
        if self._started_at is None:
            self._started_at = time.perf_counter()

    def _mark_retrying(self) -> None:
        self._status = "retrying"

    def _mark_finished(self, result: TaskResult) -> None:
        with self._state_lock:
            if self._result is not None:
                return
            self._result = result
            self._status = result.status
            waiters = tuple(self._waiters)
            self._waiters.clear()

        for waiter in waiters:
            loop = waiter.get_loop()
            if waiter.done() or loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(self._resolve_waiter, waiter, result)
            except RuntimeError:
                continue

    @staticmethod
    def _resolve_waiter(waiter: asyncio.Future[TaskResult], result: TaskResult) -> None:
        if waiter.done():
            return
        waiter.set_result(result)


@dataclass(order=True, slots=True)
class _QueueEntry:
    priority: int
    sequence: int
    item: QueueItem = field(compare=False)
    handle: TaskHandle = field(compare=False)
    is_sentinel: bool = field(default=False, compare=False)


class TaskQueue:
    """A small asyncio task queue with priorities, retries, and graceful shutdown."""

    start_time: float

    def __init__(
        self,
        *,
        size: int = 0,
        max_workers: int | None = None,
        queue: asyncio.Queue[_QueueEntry] | None = None,
        queue_timeout: float | None = None,
        on_exit: ShutdownPolicy = "complete_priority",
        mode: QueueMode = "finite",
        raise_on_error: bool = False,
        auto_worker_limit: int | None = None,
        on_task_complete: Callable[[TaskResult], Any] | None = None,
    ) -> None:
        if size < 0:
            raise ValueError("size must be greater than or equal to 0")
        if max_workers is not None and max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        if queue_timeout is not None and queue_timeout <= 0:
            raise ValueError("queue_timeout must be greater than 0")
        if on_exit not in {"cancel", "complete_priority"}:
            raise ValueError("on_exit must be 'cancel' or 'complete_priority'")
        if mode not in {"finite", "infinite"}:
            raise ValueError("mode must be 'finite' or 'infinite'")

        self.queue = queue or asyncio.PriorityQueue(maxsize=size)
        self.max_workers = max_workers
        self.auto_worker_limit = auto_worker_limit or max(4, min(32, (os.cpu_count() or 1) * 4))
        self.queue_timeout = queue_timeout
        self.on_exit = on_exit
        self.mode = mode
        self.raise_on_error = raise_on_error
        self._on_task_complete = on_task_complete

        self.worker_tasks: dict[int, asyncio.Task[None]] = {}
        self.queue_cancelled = False
        self.stop = False
        self.start_time = 0.0

        self._accepting = True
        self._closed = False
        self._counter = itertools.count()
        self._worker_ids = itertools.count(1)
        self._active_tasks: dict[str, asyncio.Task[Any]] = {}
        self._handles: dict[str, TaskHandle] = {}
        self._results: list[TaskResult] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._timed_out = False
        self._run_in_progress = False
        self._shutdown_event: asyncio.Event | None = None
        self._loop_thread_id: int | None = None
        self._lifecycle_lock = Lock()

    # -- Public properties ---------------------------------------------------

    @property
    def active_count(self) -> int:
        return len(self._active_tasks)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def pending_count(self) -> int:
        return self.queue.qsize()

    @property
    def results(self) -> tuple[TaskResult, ...]:
        return tuple(self._results)

    @property
    def stats(self) -> dict[str, int | bool]:
        return {
            "pending": self.pending_count,
            "active": self.active_count,
            "completed": len(self._results),
            "workers": len(self.worker_tasks),
            "closed": self.closed,
        }

    # -- Task submission -----------------------------------------------------

    def add_task(
        self,
        task: Any,
        /,
        *args: Any,
        must_complete: bool = False,
        priority: int = 3,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
        **kwargs: Any,
    ) -> TaskHandle:
        return self.submit(
            task,
            *args,
            must_complete=must_complete,
            priority=priority,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
            **kwargs,
        )

    def submit(
        self,
        task: Any,
        /,
        *args: Any,
        must_complete: bool = False,
        priority: int = 3,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
        **kwargs: Any,
    ) -> TaskHandle:
        item = QueueItem(task, *args, **kwargs)
        return self.add(
            item=item,
            priority=priority,
            must_complete=must_complete,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
        )

    def map(
        self,
        task: Any,
        iterable: Iterable[Any],
        *,
        must_complete: bool = False,
        priority: int = 3,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
    ) -> list[TaskHandle]:
        opts = dict(
            must_complete=must_complete,
            priority=priority,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
        )
        handles: list[TaskHandle] = []
        for entry in iterable:
            if isinstance(entry, Mapping):
                handles.append(self.add(item=QueueItem(task, **entry), **opts))
            elif isinstance(entry, tuple):
                handles.append(self.submit(task, *entry, **opts))
            else:
                handles.append(self.submit(task, entry, **opts))
        return handles

    def add(
        self,
        *,
        item: QueueItem,
        priority: int = 3,
        must_complete: bool = False,
        with_new_workers: bool = True,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
    ) -> TaskHandle:
        if self._closed or not self._accepting or self.stop:
            raise QueueClosedError("queue is not accepting new tasks")

        configured_item = item.configure(
            must_complete=must_complete,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
        )

        handle = TaskHandle(
            task_id=configured_item.task_id,
            name=configured_item.name,
            priority=priority,
            must_complete=configured_item.must_complete,
            created_at=configured_item.created_at,
            cancel_callback=self._cancel_task,
        )
        self._bind_handle(handle)
        self._call_in_queue_loop(
            self._enqueue_entry,
            _QueueEntry(
                priority=priority,
                sequence=next(self._counter),
                item=configured_item,
                handle=handle,
            ),
            with_new_workers=with_new_workers,
        )
        return handle

    # -- Lifecycle -----------------------------------------------------------

    async def start(self) -> TaskQueue:
        if self._closed:
            raise QueueClosedError("queue has already been shut down")

        self._bind_loop()
        self._shutdown_event = asyncio.Event()
        self.queue_cancelled = False
        self.stop = False
        self._timed_out = False
        self.start_time = time.perf_counter()

        if not self._started:
            self._started = True
            self._ensure_worker_count()
        elif self.max_workers is None:
            self._ensure_worker_count()

        return self

    async def join(self) -> None:
        if not self._started:
            await self.start()
        await self.queue.join()

    async def run(
        self,
        queue_timeout: float | None = None,
        *,
        raise_on_error: bool | None = None,
    ) -> QueueRunSummary:
        if self._run_in_progress:
            raise RuntimeError("run() cannot be called while another run is in progress")

        self._run_in_progress = True
        run_start = time.perf_counter()
        result_index = len(self._results)
        timeout = queue_timeout if queue_timeout is not None else self.queue_timeout

        await self.start()

        try:
            if self.mode == "finite":
                await self._run_finite(timeout=timeout)
            else:
                await self._run_infinite(timeout=timeout)
        finally:
            summary = QueueRunSummary.from_results(
                self._results[result_index:],
                run_start=run_start,
                timed_out=self._timed_out,
            )
            self._timed_out = False
            self._run_in_progress = False

        should_raise = self.raise_on_error if raise_on_error is None else raise_on_error
        if should_raise:
            summary.raise_for_errors()
        return summary

    async def shutdown(self, *, cancel_pending: bool = False) -> None:
        self._accepting = False
        self.stop = True

        if cancel_pending:
            self.queue_cancelled = True
            self._cancel_pending_entries(include_must_complete=True)
            self._cancel_running_entries(include_must_complete=True)
        else:
            if not self._started and self.pending_count > 0:
                await self.start()
            await self.queue.join()

        await self._stop_workers()
        self._closed = True
        if self._shutdown_event is not None:
            self._shutdown_event.set()

    async def __aenter__(self) -> TaskQueue:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown(cancel_pending=exc_type is not None)

    def cancel(self) -> asyncio.Task[None] | None:
        self.queue_cancelled = True
        self.stop = True
        loop = self._loop
        if loop is not None and loop.is_running():
            if self._loop_thread_id == get_ident():
                return loop.create_task(self.shutdown(cancel_pending=True))
            asyncio.run_coroutine_threadsafe(self.shutdown(cancel_pending=True), loop)
            return None
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._accepting = False
            self._closed = True
            self._cancel_pending_entries(include_must_complete=True)
            return None
        return running_loop.create_task(self.shutdown(cancel_pending=True))

    def reset(self) -> None:
        """Reset queue state for reuse after ``run()`` or ``shutdown()``."""
        if self._run_in_progress:
            raise RuntimeError("cannot reset while a run is in progress")
        self._results.clear()
        self._handles.clear()
        self._closed = False
        self._accepting = True
        self._started = False
        self.stop = False
        self.queue_cancelled = False
        self._timed_out = False
        self._counter = itertools.count()
        self._worker_ids = itertools.count(1)
        self._loop = None
        self._loop_thread_id = None
        self._shutdown_event = None

    def clear_results(self) -> None:
        """Discard accumulated results to free memory."""
        self._results.clear()

    # -- Result streaming ----------------------------------------------------

    @staticmethod
    async def as_completed(
        handles: Iterable[TaskHandle],
    ):
        """Yield handles as they finish, fastest first."""
        tasks = {asyncio.ensure_future(h.wait()): h for h in handles}
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                yield tasks[fut]

    # -- Internal: loop binding ----------------------------------------------

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lifecycle_lock:
            if self._loop is None:
                self._loop = loop
                self._loop_thread_id = get_ident()
                return
            if self._loop is not loop and (self.worker_tasks or self._active_tasks):
                raise RuntimeError("TaskQueue cannot switch event loops while it is running")
            self._loop = loop
            self._loop_thread_id = get_ident()

    def _bind_handle(self, handle: TaskHandle) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        with self._lifecycle_lock:
            if self._loop is None:
                self._loop = loop
                self._loop_thread_id = get_ident()

    def _call_in_queue_loop(self, callback: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        loop = self._loop
        if loop is None or not loop.is_running() or self._loop_thread_id == get_ident():
            return callback(*args, **kwargs)

        waiter: Future[T] = Future()

        def invoke() -> None:
            try:
                waiter.set_result(callback(*args, **kwargs))
            except BaseException as exc:
                waiter.set_exception(exc)

        loop.call_soon_threadsafe(invoke)
        return waiter.result()

    # -- Internal: enqueue / workers -----------------------------------------

    def _enqueue_entry(self, entry: _QueueEntry, *, with_new_workers: bool) -> None:
        with self._lifecycle_lock:
            if self._closed or not self._accepting or self.stop:
                raise QueueClosedError("queue is not accepting new tasks")

            self.queue.put_nowait(entry)
            self._handles[entry.handle.task_id] = entry.handle
            should_add_workers = self._started and with_new_workers

        if should_add_workers:
            self._ensure_worker_count()

    def _desired_worker_count(self) -> int:
        if self.max_workers is not None:
            return self.max_workers

        backlog = self.queue.qsize() + len(self._active_tasks)
        if backlog == 0:
            return 1 if self.mode == "infinite" else 0
        return max(1, min(self.auto_worker_limit, backlog))

    def _ensure_worker_count(self, no_of_workers: int | None = None) -> None:
        if self._closed or self.stop:
            return
        target = no_of_workers if no_of_workers is not None else self._desired_worker_count()
        while len(self.worker_tasks) < target:
            worker_id = next(self._worker_ids)
            self.worker_tasks[worker_id] = asyncio.create_task(
                self._worker(worker_id),
                name=f"TaskQueueWorker-{worker_id}",
            )

    async def _worker(self, worker_id: int) -> None:
        try:
            while True:
                entry = await self.queue.get()
                if entry.is_sentinel:
                    self.queue.task_done()
                    break

                if self.stop and not entry.item.must_complete:
                    self._record_cancelled(entry, "Task skipped during shutdown.")
                    self.queue.task_done()
                    continue

                try:
                    await self._execute_entry(entry)
                finally:
                    self.queue.task_done()
        finally:
            self.worker_tasks.pop(worker_id, None)

    async def _execute_entry(self, entry: _QueueEntry) -> None:
        item = entry.item
        handle = entry.handle
        delay = item.retry_delay

        while True:
            handle._mark_running()
            execution = asyncio.create_task(item(), name=f"TaskQueueTask-{handle.task_id}")
            self._active_tasks[handle.task_id] = execution

            try:
                if item.timeout is None:
                    value = await execution
                else:
                    value = await asyncio.wait_for(execution, timeout=item.timeout)
            except asyncio.CancelledError:
                self._record_result(
                    handle, _make_result(handle, status="cancelled", message="Task was cancelled.")
                )
                return
            except Exception as exc:
                if handle.attempts <= item.retries:
                    handle._mark_retrying()
                    if delay > 0:
                        await asyncio.sleep(delay)
                        delay *= item.backoff
                    elif item.retry_delay > 0:
                        delay = item.retry_delay * item.backoff
                    continue

                if not isinstance(exc, asyncio.TimeoutError):
                    logger.exception("Task %s failed", handle.name)

                self._record_result(
                    handle,
                    _make_result(handle, status="failed", exception=exc, message=str(exc)),
                )
                return
            else:
                self._record_result(
                    handle, _make_result(handle, status="succeeded", value=value)
                )
                return
            finally:
                self._active_tasks.pop(handle.task_id, None)

    # -- Internal: run modes -------------------------------------------------

    async def _run_finite(self, *, timeout: float | None) -> None:
        try:
            if timeout is None:
                await self.join()
            else:
                await asyncio.wait_for(self.join(), timeout=timeout)
        except asyncio.TimeoutError:
            self._timed_out = True
            await self._handle_timeout()
        finally:
            await self._stop_workers()
            self.stop = False

    async def _run_infinite(self, *, timeout: float | None) -> None:
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()

        try:
            if timeout is None:
                await self._shutdown_event.wait()
            else:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self._timed_out = True
            await self._handle_timeout()
            await self._stop_workers()
            self.stop = False
            if self._shutdown_event is not None:
                self._shutdown_event.set()
        else:
            if not self._closed:
                await self._stop_workers()

    async def _handle_timeout(self) -> None:
        self.stop = True
        if self.on_exit == "cancel":
            self.queue_cancelled = True
            self._cancel_pending_entries(include_must_complete=True)
            self._cancel_running_entries(include_must_complete=True)
            return

        self._cancel_pending_entries(include_must_complete=False)
        self._cancel_running_entries(include_must_complete=False)
        await self.queue.join()

    # -- Internal: cancellation ----------------------------------------------

    def _cancel_task(self, task_id: str) -> bool:
        return self._call_in_queue_loop(self._cancel_task_local, task_id)

    def _cancel_task_local(self, task_id: str) -> bool:
        handle = self._handles.get(task_id)
        if handle is None or handle.done():
            return False

        execution = self._active_tasks.get(task_id)
        if execution is not None:
            execution.cancel()
            return True

        return self._cancel_pending_entries(include_must_complete=True, only_task_ids={task_id}) > 0

    def _cancel_pending_entries(
        self,
        *,
        include_must_complete: bool,
        only_task_ids: set[str] | None = None,
    ) -> int:
        retained: list[_QueueEntry] = []
        cancelled = 0

        while True:
            try:
                entry = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if entry.is_sentinel:
                self.queue.task_done()
                retained.append(entry)
                continue

            task_selected = only_task_ids is None or entry.handle.task_id in only_task_ids
            should_cancel = task_selected and (
                include_must_complete or not entry.item.must_complete
            )

            if should_cancel:
                cancelled += 1
                self._record_cancelled(entry, "Task cancelled before execution.")
                self.queue.task_done()
                continue

            retained.append(entry)
            self.queue.task_done()

        for entry in retained:
            self.queue.put_nowait(entry)

        return cancelled

    def _cancel_running_entries(self, *, include_must_complete: bool) -> None:
        for task_id, execution in list(self._active_tasks.items()):
            handle = self._handles.get(task_id)
            if handle is None or handle.done():
                continue
            if include_must_complete or not handle.must_complete:
                execution.cancel()

    # -- Internal: result recording ------------------------------------------

    def _record_cancelled(self, entry: _QueueEntry, message: str) -> None:
        handle = entry.handle
        if handle.done():
            return
        self._record_result(handle, _make_result(handle, status="cancelled", message=message))

    def _record_result(self, handle: TaskHandle, result: TaskResult) -> None:
        handle._mark_finished(result)
        self._results.append(result)
        if self._on_task_complete is not None:
            try:
                self._on_task_complete(result)
            except Exception:
                logger.exception("on_task_complete callback raised")

    # -- Internal: worker lifecycle ------------------------------------------

    async def _stop_workers(self) -> None:
        if not self.worker_tasks:
            self._started = False
            return

        workers = list(self.worker_tasks.values())
        for _ in range(len(workers)):
            self.queue.put_nowait(
                _QueueEntry(
                    priority=10**9,
                    sequence=next(self._counter),
                    item=QueueItem(self._sentinel_task),
                    handle=TaskHandle(
                        task_id=f"sentinel-{next(self._counter)}",
                        name="sentinel",
                        priority=10**9,
                        must_complete=True,
                        created_at=time.perf_counter(),
                        cancel_callback=lambda _task_id: False,
                    ),
                    is_sentinel=True,
                )
            )

        await asyncio.gather(*workers, return_exceptions=True)
        self.worker_tasks.clear()
        self._started = False

    async def _sentinel_task(self) -> None:
        return None
