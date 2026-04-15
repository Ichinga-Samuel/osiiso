from __future__ import annotations

import itertools
import os
import queue as queue_module
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Literal

from .exceptions import QueueClosedError, QueueExecutionError
from .thread_queue_item import ThreadQueueItem

logger = getLogger(__name__)

TaskStatus = Literal["pending", "running", "retrying", "succeeded", "failed", "cancelled"]
QueueMode = Literal["finite", "infinite"]
ShutdownPolicy = Literal["cancel", "complete_priority"]


@dataclass(frozen=True, slots=True)
class ThreadTaskResult:
    """The final state of a task executed by `ThreadTaskQueue`."""

    task_id: str
    name: str
    status: Literal["succeeded", "failed", "cancelled"]
    value: Any = None
    exception: BaseException | None = None
    attempts: int = 0
    priority: int = 0
    must_complete: bool = False
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float = 0.0
    duration: float = 0.0
    message: str = ""


class ThreadTaskHandle:
    """A blocking handle for inspecting, waiting, or cancelling queued thread tasks."""

    __slots__ = (
        "task_id",
        "name",
        "priority",
        "must_complete",
        "created_at",
        "_attempts",
        "_cancel_callback",
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
        cancel_callback: Any,
    ) -> None:
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.must_complete = must_complete
        self.created_at = created_at
        self._attempts = 0
        self._cancel_callback = cancel_callback
        self._condition = threading.Condition()
        self._result: ThreadTaskResult | None = None
        self._status: TaskStatus = "pending"
        self._started_at: float | None = None

    def __repr__(self) -> str:
        return (
            "ThreadTaskHandle("
            f"task_id={self.task_id!r}, name={self.name!r}, status={self.status!r}, "
            f"attempts={self.attempts}, priority={self.priority})"
        )

    @property
    def status(self) -> TaskStatus:
        return self._status

    @property
    def attempts(self) -> int:
        return self._attempts

    def done(self) -> bool:
        with self._condition:
            return self._result is not None

    def cancelled(self) -> bool:
        with self._condition:
            return self._result is not None and self._result.status == "cancelled"

    def exception(self) -> BaseException | None:
        return self.result().exception

    def result(self) -> ThreadTaskResult:
        with self._condition:
            if self._result is None:
                raise RuntimeError("Task result is not ready yet")
            return self._result

    def value(self) -> Any:
        result = self.result()
        if result.status == "cancelled":
            raise CancelledError(result.message)
        if result.exception is not None:
            raise result.exception
        return result.value

    def wait(self, timeout: float | None = None) -> ThreadTaskResult:
        with self._condition:
            if self._result is None:
                completed = self._condition.wait_for(
                    lambda: self._result is not None,
                    timeout=timeout,
                )
                if not completed:
                    raise TimeoutError("Task result is not ready yet")
            return self._result

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

    def _mark_finished(self, result: ThreadTaskResult) -> None:
        with self._condition:
            if self._result is not None:
                return
            self._result = result
            self._status = result.status
            self._condition.notify_all()


@dataclass(order=True, slots=True)
class _ThreadQueueEntry:
    priority: int
    sequence: int
    item: ThreadQueueItem = field(compare=False)
    handle: ThreadTaskHandle = field(compare=False)
    is_sentinel: bool = field(default=False, compare=False)


@dataclass(slots=True)
class _RunningTask:
    cancel_event: threading.Event


@dataclass(frozen=True, slots=True)
class ThreadQueueRunSummary:
    """A compact summary for a completed threaded queue run."""

    total_submitted: int
    succeeded: int
    failed: int
    cancelled: int
    timed_out: bool
    duration: float
    results: tuple[ThreadTaskResult, ...]

    @property
    def errors(self) -> tuple[ThreadTaskResult, ...]:
        return tuple(result for result in self.results if result.status == "failed")

    def raise_for_errors(self) -> None:
        if self.errors:
            raise QueueExecutionError(list(self.errors))


class _TaskCancelled(Exception):
    """Internal signal for cooperative cancellation."""


class ThreadTaskQueue:
    """A thread-based task queue with priorities, retries, and graceful shutdown."""

    start_time: float

    def __init__(
        self,
        *,
        size: int = 0,
        max_workers: int | None = None,
        queue: queue_module.PriorityQueue[_ThreadQueueEntry] | None = None,
        queue_timeout: float | None = None,
        on_exit: ShutdownPolicy = "complete_priority",
        mode: QueueMode = "finite",
        raise_on_error: bool = False,
        auto_worker_limit: int | None = None,
        poll_interval: float = 0.05,
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
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than 0")

        self.queue = queue or queue_module.PriorityQueue(maxsize=size)
        self.max_workers = max_workers
        self.auto_worker_limit = auto_worker_limit or max(4, min(32, (os.cpu_count() or 1) * 4))
        self.queue_timeout = queue_timeout
        self.on_exit = on_exit
        self.mode = mode
        self.raise_on_error = raise_on_error
        self.poll_interval = poll_interval

        self.worker_threads: dict[int, threading.Thread] = {}
        self.queue_cancelled = False
        self.stop = False
        self.start_time = 0.0

        self._accepting = True
        self._closed = False
        self._counter = itertools.count()
        self._worker_ids = itertools.count(1)
        self._active_tasks: dict[str, _RunningTask] = {}
        self._handles: dict[str, ThreadTaskHandle] = {}
        self._results: list[ThreadTaskResult] = []
        self._started = False
        self._timed_out = False
        self._run_in_progress = False
        self._shutdown_event = threading.Event()
        self._state_lock = threading.RLock()

    @property
    def active_count(self) -> int:
        with self._state_lock:
            return len(self._active_tasks)

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._closed

    @property
    def pending_count(self) -> int:
        return self.queue.qsize()

    @property
    def results(self) -> tuple[ThreadTaskResult, ...]:
        with self._state_lock:
            return tuple(self._results)

    @property
    def stats(self) -> dict[str, int | bool]:
        return {
            "pending": self.pending_count,
            "active": self.active_count,
            "completed": len(self.results),
            "workers": len(self.worker_threads),
            "closed": self.closed,
        }

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
    ) -> ThreadTaskHandle:
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
    ) -> ThreadTaskHandle:
        item = ThreadQueueItem(task, *args, **kwargs)
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
    ) -> list[ThreadTaskHandle]:
        handles: list[ThreadTaskHandle] = []
        for entry in iterable:
            if isinstance(entry, Mapping):
                handle = self.submit(
                    task,
                    must_complete=must_complete,
                    priority=priority,
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay,
                    backoff=backoff,
                    name=name,
                    **dict(entry),
                )
            elif isinstance(entry, tuple):
                handle = self.submit(
                    task,
                    *entry,
                    must_complete=must_complete,
                    priority=priority,
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay,
                    backoff=backoff,
                    name=name,
                )
            else:
                handle = self.submit(
                    task,
                    entry,
                    must_complete=must_complete,
                    priority=priority,
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay,
                    backoff=backoff,
                    name=name,
                )
            handles.append(handle)
        return handles

    def add(
        self,
        *,
        item: ThreadQueueItem,
        priority: int = 3,
        must_complete: bool = False,
        with_new_workers: bool = True,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
    ) -> ThreadTaskHandle:
        with self._state_lock:
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

        handle = ThreadTaskHandle(
            task_id=configured_item.task_id,
            name=configured_item.name,
            priority=priority,
            must_complete=configured_item.must_complete,
            created_at=configured_item.created_at,
            cancel_callback=self._cancel_task,
        )

        self._enqueue_entry(
            _ThreadQueueEntry(
                priority=priority,
                sequence=next(self._counter),
                item=configured_item,
                handle=handle,
            ),
            with_new_workers=with_new_workers,
        )
        return handle

    def start(self) -> ThreadTaskQueue:
        with self._state_lock:
            if self._closed:
                raise QueueClosedError("queue has already been shut down")

            self._shutdown_event = threading.Event()
            self._accepting = True
            self.queue_cancelled = False
            self.stop = False
            self._timed_out = False
            self.start_time = time.perf_counter()

            should_ensure = not self._started
            if not self._started:
                self._started = True
            elif self.max_workers is None:
                should_ensure = True

        if should_ensure:
            self._ensure_worker_count()

        return self

    def join(self) -> None:
        if not self._started:
            self.start()
        self._wait_for_queue_completion(timeout=None)

    def run(
        self,
        queue_timeout: float | None = None,
        *,
        raise_on_error: bool | None = None,
    ) -> ThreadQueueRunSummary:
        with self._state_lock:
            if self._run_in_progress:
                raise RuntimeError("run() cannot be called while another run is in progress")
            self._run_in_progress = True
            result_index = len(self._results)

        run_start = time.perf_counter()
        timeout = queue_timeout if queue_timeout is not None else self.queue_timeout

        self.start()

        try:
            if self.mode == "finite":
                self._run_finite(timeout=timeout)
            else:
                self._run_infinite(timeout=timeout)
        finally:
            with self._state_lock:
                collected = list(self._results[result_index:])
                timed_out = self._timed_out
                self._timed_out = False
                self._run_in_progress = False

            summary = self._build_summary(
                results=collected,
                run_start=run_start,
                timed_out=timed_out,
            )

        should_raise = self.raise_on_error if raise_on_error is None else raise_on_error
        if should_raise:
            summary.raise_for_errors()
        return summary

    def shutdown(self, *, cancel_pending: bool = False) -> None:
        with self._state_lock:
            self._accepting = False

        if cancel_pending:
            with self._state_lock:
                self.queue_cancelled = True
                self.stop = True
            self._cancel_pending_entries(include_must_complete=True)
            self._cancel_running_entries(include_must_complete=True)
        else:
            if not self._started and self.pending_count > 0:
                self.start()
            self._wait_for_queue_completion(timeout=None)
            with self._state_lock:
                self.stop = True

        self._stop_workers()
        with self._state_lock:
            self._closed = True
            self._shutdown_event.set()

    def __enter__(self) -> ThreadTaskQueue:
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown(cancel_pending=exc_type is not None)

    def cancel(self) -> threading.Thread | None:
        workers = tuple(self.worker_threads.values())
        current = threading.current_thread()
        if any(worker is current for worker in workers):
            shutdown_thread = threading.Thread(
                target=self.shutdown,
                kwargs={"cancel_pending": True},
                name="TaskQueueCancelThread",
                daemon=True,
            )
            shutdown_thread.start()
            return shutdown_thread

        self.shutdown(cancel_pending=True)
        return None

    def cancel_all_workers(self) -> threading.Thread | None:
        return self.cancel()

    def _enqueue_entry(self, entry: _ThreadQueueEntry, *, with_new_workers: bool) -> None:
        with self._state_lock:
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

        backlog = self.queue.qsize() + self.active_count
        if backlog == 0:
            return 1 if self.mode == "infinite" else 0
        return max(1, min(self.auto_worker_limit, backlog))

    def _ensure_worker_count(self, no_of_workers: int | None = None) -> None:
        with self._state_lock:
            if self._closed or self.stop:
                return
            target = no_of_workers if no_of_workers is not None else self._desired_worker_count()
            threads_to_start: list[threading.Thread] = []
            while len(self.worker_threads) < target:
                worker_id = next(self._worker_ids)
                worker_thread = threading.Thread(
                    target=self._worker,
                    args=(worker_id,),
                    name=f"TaskQueueWorker-{worker_id}",
                    daemon=True,
                )
                self.worker_threads[worker_id] = worker_thread
                threads_to_start.append(worker_thread)

        for worker_thread in threads_to_start:
            worker_thread.start()

    def _worker(self, worker_id: int) -> None:
        try:
            while True:
                entry = self.queue.get()
                if entry.is_sentinel:
                    self.queue.task_done()
                    break

                if self.stop and not entry.item.must_complete:
                    self._record_cancelled(entry, "Task skipped during shutdown.")
                    self.queue.task_done()
                    continue

                try:
                    self._execute_entry(entry)
                finally:
                    self.queue.task_done()
        finally:
            with self._state_lock:
                self.worker_threads.pop(worker_id, None)

    def _execute_entry(self, entry: _ThreadQueueEntry) -> None:
        item = entry.item
        handle = entry.handle
        delay = item.retry_delay
        running = _RunningTask(cancel_event=threading.Event())

        with self._state_lock:
            self._active_tasks[handle.task_id] = running

        try:
            while True:
                if running.cancel_event.is_set():
                    finished_at = time.perf_counter()
                    self._record_result(
                        handle,
                        ThreadTaskResult(
                            task_id=handle.task_id,
                            name=handle.name,
                            status="cancelled",
                            attempts=handle.attempts,
                            priority=handle.priority,
                            must_complete=handle.must_complete,
                            created_at=handle.created_at,
                            started_at=handle._started_at,
                            finished_at=finished_at,
                            duration=(
                                0.0
                                if handle._started_at is None
                                else finished_at - handle._started_at
                            ),
                            message="Task execution was cancelled.",
                        ),
                    )
                    return

                handle._mark_running()
                started_at = handle._started_at or time.perf_counter()

                try:
                    value = self._run_item_with_controls(
                        item=item,
                        cancel_event=running.cancel_event,
                    )
                except _TaskCancelled as exc:
                    finished_at = time.perf_counter()
                    self._record_result(
                        handle,
                        ThreadTaskResult(
                            task_id=handle.task_id,
                            name=handle.name,
                            status="cancelled",
                            exception=exc,
                            attempts=handle.attempts,
                            priority=handle.priority,
                            must_complete=handle.must_complete,
                            created_at=handle.created_at,
                            started_at=started_at,
                            finished_at=finished_at,
                            duration=finished_at - started_at,
                            message="Task execution was cancelled.",
                        ),
                    )
                    return
                except Exception as exc:
                    should_retry = (
                        not running.cancel_event.is_set()
                        and handle.attempts <= item.retries
                    )
                    if should_retry:
                        handle._mark_retrying()
                        if delay > 0:
                            try:
                                self._sleep_with_cancel(
                                    delay=delay,
                                    cancel_event=running.cancel_event,
                                )
                            except _TaskCancelled as cancelled_exc:
                                finished_at = time.perf_counter()
                                self._record_result(
                                    handle,
                                    ThreadTaskResult(
                                        task_id=handle.task_id,
                                        name=handle.name,
                                        status="cancelled",
                                        exception=cancelled_exc,
                                        attempts=handle.attempts,
                                        priority=handle.priority,
                                        must_complete=handle.must_complete,
                                        created_at=handle.created_at,
                                        started_at=started_at,
                                        finished_at=finished_at,
                                        duration=finished_at - started_at,
                                        message="Task execution was cancelled.",
                                    ),
                                )
                                return
                            delay *= item.backoff
                        elif item.retry_delay > 0:
                            delay = item.retry_delay * item.backoff
                        continue

                    if not isinstance(exc, TimeoutError):
                        logger.exception("Task %s failed", handle.name)

                    finished_at = time.perf_counter()
                    self._record_result(
                        handle,
                        ThreadTaskResult(
                            task_id=handle.task_id,
                            name=handle.name,
                            status="failed",
                            exception=exc,
                            attempts=handle.attempts,
                            priority=handle.priority,
                            must_complete=handle.must_complete,
                            created_at=handle.created_at,
                            started_at=started_at,
                            finished_at=finished_at,
                            duration=finished_at - started_at,
                            message=str(exc),
                        ),
                    )
                    return
                else:
                    finished_at = time.perf_counter()
                    self._record_result(
                        handle,
                        ThreadTaskResult(
                            task_id=handle.task_id,
                            name=handle.name,
                            status="succeeded",
                            value=value,
                            attempts=handle.attempts,
                            priority=handle.priority,
                            must_complete=handle.must_complete,
                            created_at=handle.created_at,
                            started_at=started_at,
                            finished_at=finished_at,
                            duration=finished_at - started_at,
                        ),
                    )
                    return
        finally:
            with self._state_lock:
                self._active_tasks.pop(handle.task_id, None)

    @staticmethod
    def _invoke_item(
        item: ThreadQueueItem,
        result_queue: queue_module.Queue[tuple[str, Any]],
    ) -> None:
        try:
            result_queue.put(("value", item()))
        except BaseException as exc:  # pragma: no cover
            # Defensive catch, includes KeyboardInterrupt/SystemExit.
            result_queue.put(("error", exc))

    def _run_item_with_controls(
        self,
        *,
        item: ThreadQueueItem,
        cancel_event: threading.Event,
    ) -> Any:
        result_queue: queue_module.Queue[tuple[str, Any]] = queue_module.Queue(maxsize=1)
        execution_thread = threading.Thread(
            target=self._invoke_item,
            args=(item, result_queue),
            name=f"TaskQueueExec-{item.task_id}",
            daemon=True,
        )
        execution_thread.start()
        deadline = None if item.timeout is None else time.perf_counter() + item.timeout

        while execution_thread.is_alive():
            if cancel_event.is_set():
                raise _TaskCancelled("Task execution was cancelled.")
            if deadline is not None:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Task '{item.name}' exceeded timeout of {item.timeout} seconds."
                    )
                join_timeout = min(self.poll_interval, remaining)
            else:
                join_timeout = self.poll_interval
            execution_thread.join(timeout=join_timeout)

        if cancel_event.is_set():
            raise _TaskCancelled("Task execution was cancelled.")

        kind, payload = result_queue.get_nowait()
        if kind == "error":
            raise payload
        return payload

    def _sleep_with_cancel(self, *, delay: float, cancel_event: threading.Event) -> None:
        deadline = time.perf_counter() + delay
        while True:
            if cancel_event.is_set():
                raise _TaskCancelled("Task execution was cancelled.")
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                return
            cancel_event.wait(timeout=min(self.poll_interval, remaining))

    def _run_finite(self, *, timeout: float | None) -> None:
        try:
            completed = self._wait_for_queue_completion(timeout=timeout)
            if not completed:
                raise TimeoutError("Queue execution exceeded the configured timeout.")
        except TimeoutError:
            self._timed_out = True
            self._handle_timeout()
        finally:
            self._stop_workers()
            self.stop = False

    def _run_infinite(self, *, timeout: float | None) -> None:
        try:
            if timeout is None:
                self._shutdown_event.wait()
                timed_out = False
            else:
                timed_out = not self._shutdown_event.wait(timeout=timeout)
        except Exception:
            self._stop_workers()
            raise

        if timed_out:
            self._timed_out = True
            self._handle_timeout()
            self._stop_workers()
            self.stop = False
            self._shutdown_event.set()
        else:
            if not self._closed:
                self._stop_workers()

    def _handle_timeout(self) -> None:
        self.stop = True
        if self.on_exit == "cancel":
            self.queue_cancelled = True
            self._cancel_pending_entries(include_must_complete=True)
            self._cancel_running_entries(include_must_complete=True)
            return

        self._cancel_pending_entries(include_must_complete=False)
        self._cancel_running_entries(include_must_complete=False)
        self._wait_for_queue_completion(timeout=None)

    def _cancel_task(self, task_id: str) -> bool:
        with self._state_lock:
            handle = self._handles.get(task_id)
            if handle is None or handle.done():
                return False

            running = self._active_tasks.get(task_id)
            if running is not None:
                running.cancel_event.set()
                return True

        return self._cancel_pending_entries(include_must_complete=True, only_task_ids={task_id}) > 0

    def _cancel_pending_entries(
        self,
        *,
        include_must_complete: bool,
        only_task_ids: set[str] | None = None,
    ) -> int:
        retained: list[_ThreadQueueEntry] = []
        cancelled = 0

        while True:
            try:
                entry = self.queue.get_nowait()
            except queue_module.Empty:
                break

            if entry.is_sentinel:
                self.queue.task_done()
                retained.append(entry)
                continue

            task_selected = only_task_ids is None or entry.handle.task_id in only_task_ids
            should_cancel = (
                task_selected and (include_must_complete or not entry.item.must_complete)
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
        with self._state_lock:
            active_entries = list(self._active_tasks.items())

        for task_id, running in active_entries:
            handle = self._handles.get(task_id)
            if handle is None or handle.done():
                continue
            if include_must_complete or not handle.must_complete:
                running.cancel_event.set()

    def _record_cancelled(self, entry: _ThreadQueueEntry, message: str) -> None:
        handle = entry.handle
        if handle.done():
            return

        finished_at = time.perf_counter()
        duration = 0.0 if handle._started_at is None else finished_at - handle._started_at
        self._record_result(
            handle,
            ThreadTaskResult(
                task_id=handle.task_id,
                name=handle.name,
                status="cancelled",
                attempts=handle.attempts,
                priority=handle.priority,
                must_complete=handle.must_complete,
                created_at=handle.created_at,
                started_at=handle._started_at,
                finished_at=finished_at,
                duration=duration,
                message=message,
            ),
        )

    def _record_result(self, handle: ThreadTaskHandle, result: ThreadTaskResult) -> None:
        handle._mark_finished(result)
        with self._state_lock:
            self._results.append(result)

    def _stop_workers(self) -> None:
        with self._state_lock:
            workers = list(self.worker_threads.values())
        if not workers:
            with self._state_lock:
                self._started = False
            return

        for _ in range(len(workers)):
            self.queue.put(
                _ThreadQueueEntry(
                    priority=10**9,
                    sequence=next(self._counter),
                    item=ThreadQueueItem(self._sentinel_task),
                    handle=ThreadTaskHandle(
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

        current_thread = threading.current_thread()
        for worker in workers:
            if worker is current_thread:
                continue
            worker.join()

        with self._state_lock:
            self.worker_threads.clear()
            self._started = False

    @staticmethod
    def _sentinel_task() -> None:
        return None

    def _wait_for_queue_completion(self, *, timeout: float | None) -> bool:
        with self.queue.all_tasks_done:
            if timeout is None:
                while self.queue.unfinished_tasks:
                    self.queue.all_tasks_done.wait()
                return True

            if timeout <= 0:
                return self.queue.unfinished_tasks == 0

            deadline = time.perf_counter() + timeout
            while self.queue.unfinished_tasks:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    return False
                self.queue.all_tasks_done.wait(timeout=remaining)
            return True

    def _build_summary(
        self,
        *,
        results: list[ThreadTaskResult],
        run_start: float,
        timed_out: bool,
    ) -> ThreadQueueRunSummary:
        summary_results = tuple(results)
        succeeded = sum(1 for result in summary_results if result.status == "succeeded")
        failed = sum(1 for result in summary_results if result.status == "failed")
        cancelled = sum(1 for result in summary_results if result.status == "cancelled")
        return ThreadQueueRunSummary(
            total_submitted=len(summary_results),
            succeeded=succeeded,
            failed=failed,
            cancelled=cancelled,
            timed_out=timed_out,
            duration=time.perf_counter() - run_start,
            results=summary_results,
        )
