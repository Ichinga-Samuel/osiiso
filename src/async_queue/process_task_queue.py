from __future__ import annotations

import itertools
import multiprocessing
import os
import queue as queue_module
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from logging import getLogger
from multiprocessing.context import BaseContext
from typing import Any

from ._base import (
    FailPolicy,
    QueueMode,
    QueueRunSummary,
    ShutdownPolicy,
    TaskResult,
    TaskStatus,
    _make_result,
)
from .exceptions import QueueClosedError
from .process_queue_item import ProcessQueueItem

logger = getLogger(__name__)


def _invoke_process_item(item: ProcessQueueItem, result_queue: Any) -> None:
    try:
        result_queue.put(("value", item()))
    except BaseException as exc:
        try:
            result_queue.put(("error", exc))
        except BaseException:
            result_queue.put(("error", RuntimeError(f"{type(exc).__name__}: {exc}")))


class ProcessTaskHandle:
    """A blocking handle for inspecting, waiting, or cancelling queued process tasks."""

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
        "group_id",
        "detached",
        "scheduled_for",
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
        group_id: str | None = None,
        detached: bool = False,
        scheduled_for: float | None = None,
    ) -> None:
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.must_complete = must_complete
        self.created_at = created_at
        self.group_id = group_id
        self.detached = detached
        self.scheduled_for = scheduled_for
        self._attempts = 0
        self._cancel_callback = cancel_callback
        self._condition = threading.Condition()
        self._result: TaskResult | None = None
        self._status: TaskStatus = "pending"
        self._started_at: float | None = None

    def __repr__(self) -> str:
        return (
            "ProcessTaskHandle("
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

    def result(self) -> TaskResult:
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

    def wait(self, timeout: float | None = None) -> TaskResult:
        with self._condition:
            if self._result is None:
                completed = self._condition.wait_for(
                    lambda: self._result is not None,
                    timeout=timeout,
                )
                if not completed:
                    raise TimeoutError("Task result is not ready yet")
            return self._result  # type: ignore[return-value]

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
        with self._condition:
            if self._result is not None:
                return
            self._result = result
            self._status = result.status
            self._condition.notify_all()


@dataclass(order=True, slots=True)
class _ProcessQueueEntry:
    scheduled_key: float
    priority: int
    sequence: int
    item: ProcessQueueItem = field(compare=False)
    handle: ProcessTaskHandle = field(compare=False)
    is_sentinel: bool = field(default=False, compare=False)


class ProcessTaskGroupHandle:
    """A structured handle for a group of process queue tasks."""

    def __init__(self, group_id: str, handles: Iterable[ProcessTaskHandle]) -> None:
        self.group_id = group_id
        self.handles = tuple(handles)

    def __iter__(self):
        return iter(self.handles)

    def __len__(self) -> int:
        return len(self.handles)

    def cancel(self) -> int:
        return sum(1 for handle in self.handles if handle.cancel())

    def wait(self, timeout: float | None = None) -> QueueRunSummary:
        run_start = time.perf_counter()
        deadline = None if timeout is None else time.perf_counter() + timeout
        results: list[TaskResult] = []
        for handle in self.handles:
            remaining = None if deadline is None else max(0.0, deadline - time.perf_counter())
            results.append(handle.wait(timeout=remaining))
        return QueueRunSummary.from_results(results, run_start=run_start, timed_out=False)

    def values(self, timeout: float | None = None) -> tuple[Any, ...]:
        summary = self.wait(timeout=timeout)
        summary.raise_for_errors()
        return summary.values


@dataclass(slots=True)
class _RunningProcess:
    cancel_event: threading.Event
    process: multiprocessing.Process | None = None


class _ProcessTaskCancelled(Exception):
    """Internal signal for process task cancellation."""


class ProcessTaskQueue:
    """A process-based task queue with priorities, retries, and graceful shutdown."""

    start_time: float

    def __init__(
        self,
        *,
        size: int = 0,
        max_workers: int | None = None,
        queue: queue_module.PriorityQueue[_ProcessQueueEntry] | None = None,
        queue_timeout: float | None = None,
        on_exit: ShutdownPolicy = "complete_priority",
        mode: QueueMode = "finite",
        raise_on_error: bool = False,
        fail_policy: FailPolicy = "continue",
        auto_worker_limit: int | None = None,
        poll_interval: float = 0.05,
        on_task_complete: Callable[[TaskResult], Any] | None = None,
        context: BaseContext | None = None,
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
        if fail_policy not in {"continue", "fail_first"}:
            raise ValueError("fail_policy must be 'continue' or 'fail_first'")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than 0")

        self.queue = queue or queue_module.PriorityQueue(maxsize=size)
        self.max_workers = max_workers
        self.auto_worker_limit = auto_worker_limit or max(1, min(32, os.cpu_count() or 1))
        self.queue_timeout = queue_timeout
        self.on_exit = on_exit
        self.mode = mode
        self.raise_on_error = raise_on_error
        self.fail_policy = fail_policy
        self.poll_interval = poll_interval
        self.context = context or multiprocessing.get_context()
        self._on_task_complete = on_task_complete

        self.worker_threads: dict[int, threading.Thread] = {}
        self.queue_cancelled = False
        self.stop = False
        self.start_time = 0.0

        self._accepting = True
        self._closed = False
        self._counter = itertools.count()
        self._worker_ids = itertools.count(1)
        self._active_tasks: dict[str, _RunningProcess] = {}
        self._handles: dict[str, ProcessTaskHandle] = {}
        self._results: list[TaskResult] = []
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
    def results(self) -> tuple[TaskResult, ...]:
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
        delay: float | None = None,
        run_at: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
        **kwargs: Any,
    ) -> ProcessTaskHandle:
        return self.submit(
            task,
            *args,
            must_complete=must_complete,
            priority=priority,
            timeout=timeout,
            delay=delay,
            run_at=run_at,
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
        delay: float | None = None,
        run_at: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
        group_id: str | None = None,
        detached: bool = False,
        **kwargs: Any,
    ) -> ProcessTaskHandle:
        item = ProcessQueueItem(task, *args, **kwargs)
        return self.add(
            item=item,
            priority=priority,
            must_complete=must_complete,
            timeout=timeout,
            delay=delay,
            run_at=run_at,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
            group_id=group_id,
            detached=detached,
        )

    def map(
        self,
        task: Any,
        iterable: Iterable[Any],
        *,
        must_complete: bool = False,
        priority: int = 3,
        timeout: float | None = None,
        delay: float | None = None,
        run_at: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
        group_id: str | None = None,
        detached: bool = False,
    ) -> list[ProcessTaskHandle]:
        opts = dict(
            must_complete=must_complete,
            priority=priority,
            timeout=timeout,
            delay=delay,
            run_at=run_at,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
            group_id=group_id,
            detached=detached,
        )
        handles: list[ProcessTaskHandle] = []
        for entry in iterable:
            if isinstance(entry, Mapping):
                handles.append(self.add(item=ProcessQueueItem(task, **entry), **opts))
            elif isinstance(entry, tuple):
                handles.append(self.submit(task, *entry, **opts))
            else:
                handles.append(self.submit(task, entry, **opts))
        return handles

    def fire_and_forget(self, task: Any, /, *args: Any, **kwargs: Any) -> ProcessTaskHandle:
        return self.submit(task, *args, detached=True, **kwargs)

    def background(self, task: Any, /, *args: Any, **kwargs: Any) -> ProcessTaskHandle:
        if not self._started:
            self.start()
        return self.fire_and_forget(task, *args, **kwargs)

    def submit_group(
        self,
        task: Any,
        iterable: Iterable[Any],
        *,
        group_id: str | None = None,
        **kwargs: Any,
    ) -> ProcessTaskGroupHandle:
        group_id = group_id or f"group-{next(self._counter)}"
        handles = self.map(task, iterable, group_id=group_id, **kwargs)
        return ProcessTaskGroupHandle(group_id, handles)

    group = submit_group

    def add(
        self,
        *,
        item: ProcessQueueItem,
        priority: int = 3,
        must_complete: bool = False,
        with_new_workers: bool = True,
        timeout: float | None = None,
        delay: float | None = None,
        run_at: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
        group_id: str | None = None,
        detached: bool = False,
    ) -> ProcessTaskHandle:
        with self._state_lock:
            if self._closed or not self._accepting or self.stop:
                raise QueueClosedError("queue is not accepting new tasks")

        configured_item = item.configure(
            must_complete=must_complete,
            timeout=timeout,
            delay=delay,
            run_at=run_at,
            retries=retries,
            retry_delay=retry_delay,
            backoff=backoff,
            name=name,
            group_id=group_id,
            detached=detached,
        )

        handle = ProcessTaskHandle(
            task_id=configured_item.task_id,
            name=configured_item.name,
            priority=priority,
            must_complete=configured_item.must_complete,
            created_at=configured_item.created_at,
            cancel_callback=self._cancel_task,
            group_id=configured_item.group_id,
            detached=configured_item.detached,
            scheduled_for=configured_item.scheduled_for,
        )

        self._enqueue_entry(
            _ProcessQueueEntry(
                scheduled_key=configured_item.scheduled_for or 0.0,
                priority=priority,
                sequence=next(self._counter),
                item=configured_item,
                handle=handle,
            ),
            with_new_workers=with_new_workers,
        )
        return handle

    def start(self) -> ProcessTaskQueue:
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
        fail_policy: FailPolicy | None = None,
    ) -> QueueRunSummary:
        with self._state_lock:
            if self._run_in_progress:
                raise RuntimeError("run() cannot be called while another run is in progress")
            self._run_in_progress = True
            result_index = len(self._results)

        run_start = time.perf_counter()
        timeout = queue_timeout if queue_timeout is not None else self.queue_timeout
        original_fail_policy = self.fail_policy
        if fail_policy is not None:
            if fail_policy not in {"continue", "fail_first"}:
                raise ValueError("fail_policy must be 'continue' or 'fail_first'")
            self.fail_policy = fail_policy

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
                self.fail_policy = original_fail_policy

            summary = QueueRunSummary.from_results(
                collected, run_start=run_start, timed_out=timed_out
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

    def __enter__(self) -> ProcessTaskQueue:
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
                name="ProcessTaskQueueCancelThread",
                daemon=True,
            )
            shutdown_thread.start()
            return shutdown_thread

        self.shutdown(cancel_pending=True)
        return None

    def reset(self) -> None:
        """Reset queue state for reuse after ``run()`` or ``shutdown()``."""
        with self._state_lock:
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
            self._shutdown_event = threading.Event()

    def clear_results(self) -> None:
        """Discard accumulated results to free memory."""
        with self._state_lock:
            self._results.clear()

    def _enqueue_entry(self, entry: _ProcessQueueEntry, *, with_new_workers: bool) -> None:
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
                    name=f"ProcessTaskQueueWorker-{worker_id}",
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
                    self._wait_until_scheduled(entry)
                    self._execute_entry(entry)
                finally:
                    self.queue.task_done()
        finally:
            with self._state_lock:
                self.worker_threads.pop(worker_id, None)

    def _execute_entry(self, entry: _ProcessQueueEntry) -> None:
        item = entry.item
        handle = entry.handle
        delay = item.retry_delay
        running = _RunningProcess(cancel_event=threading.Event())

        with self._state_lock:
            self._active_tasks[handle.task_id] = running

        try:
            while True:
                if running.cancel_event.is_set():
                    raise _ProcessTaskCancelled("Task execution was cancelled.")

                handle._mark_running()

                try:
                    value = self._run_item_with_controls(
                        item=item,
                        cancel_event=running.cancel_event,
                        running=running,
                    )
                except _ProcessTaskCancelled:
                    raise
                except Exception as exc:
                    should_retry = (
                        not running.cancel_event.is_set() and handle.attempts <= item.retries
                    )
                    if should_retry:
                        handle._mark_retrying()
                        if delay > 0:
                            self._sleep_with_cancel(
                                delay=delay, cancel_event=running.cancel_event
                            )
                            delay *= item.backoff
                        elif item.retry_delay > 0:
                            delay = item.retry_delay * item.backoff
                        continue

                    if not isinstance(exc, TimeoutError):
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
        except _ProcessTaskCancelled:
            self._record_result(
                handle,
                _make_result(handle, status="cancelled", message="Task execution was cancelled."),
            )
        finally:
            with self._state_lock:
                self._active_tasks.pop(handle.task_id, None)

    def _run_item_with_controls(
        self,
        *,
        item: ProcessQueueItem,
        cancel_event: threading.Event,
        running: _RunningProcess,
    ) -> Any:
        result_queue = self.context.Queue(maxsize=1)
        process = self.context.Process(
            target=_invoke_process_item,
            args=(item, result_queue),
            name=f"ProcessTaskQueueExec-{item.task_id}",
        )
        running.process = process
        process.start()
        deadline = None if item.timeout is None else time.perf_counter() + item.timeout

        try:
            while process.is_alive():
                if cancel_event.is_set():
                    self._terminate_process(process)
                    raise _ProcessTaskCancelled("Task execution was cancelled.")
                if deadline is not None:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        self._terminate_process(process)
                        raise TimeoutError(
                            f"Task '{item.name}' exceeded timeout of {item.timeout} seconds."
                        )
                    join_timeout = min(self.poll_interval, remaining)
                else:
                    join_timeout = self.poll_interval
                process.join(timeout=join_timeout)

            if cancel_event.is_set():
                raise _ProcessTaskCancelled("Task execution was cancelled.")

            try:
                kind, payload = result_queue.get(timeout=self.poll_interval)
            except queue_module.Empty:
                if process.exitcode and process.exitcode != 0:
                    raise RuntimeError(
                        f"Task process exited with status {process.exitcode}."
                    ) from None
                raise RuntimeError("Task process exited without returning a result.") from None

            if kind == "error":
                raise payload
            return payload
        finally:
            process.join(timeout=0)
            running.process = None
            result_queue.close()
            result_queue.join_thread()

    def _sleep_with_cancel(self, *, delay: float, cancel_event: threading.Event) -> None:
        deadline = time.perf_counter() + delay
        while True:
            if cancel_event.is_set():
                raise _ProcessTaskCancelled("Task execution was cancelled.")
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
                if running.process is not None and running.process.is_alive():
                    self._terminate_process(running.process)
                return True

        return self._cancel_pending_entries(include_must_complete=True, only_task_ids={task_id}) > 0

    def _cancel_pending_entries(
        self,
        *,
        include_must_complete: bool,
        only_task_ids: set[str] | None = None,
    ) -> int:
        retained: list[_ProcessQueueEntry] = []
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
        with self._state_lock:
            active_entries = list(self._active_tasks.items())

        for task_id, running in active_entries:
            handle = self._handles.get(task_id)
            if handle is None or handle.done():
                continue
            if include_must_complete or not handle.must_complete:
                running.cancel_event.set()
                if running.process is not None and running.process.is_alive():
                    self._terminate_process(running.process)

    def _record_cancelled(self, entry: _ProcessQueueEntry, message: str) -> None:
        handle = entry.handle
        if handle.done():
            return
        self._record_result(handle, _make_result(handle, status="cancelled", message=message))

    def _record_result(self, handle: ProcessTaskHandle, result: TaskResult) -> None:
        handle._mark_finished(result)
        with self._state_lock:
            self._results.append(result)
        if result.status == "failed" and self.fail_policy == "fail_first" and not self.stop:
            with self._state_lock:
                self.stop = True
                self.queue_cancelled = True
            self._cancel_pending_entries(include_must_complete=True)
            self._cancel_running_entries(include_must_complete=True)
        if self._on_task_complete is not None:
            try:
                self._on_task_complete(result)
            except Exception:
                logger.exception("on_task_complete callback raised")

    def _stop_workers(self) -> None:
        with self._state_lock:
            workers = list(self.worker_threads.values())
        if not workers:
            with self._state_lock:
                self._started = False
            return

        for _ in range(len(workers)):
            self.queue.put(
                _ProcessQueueEntry(
                    scheduled_key=0.0,
                    priority=10**9,
                    sequence=next(self._counter),
                    item=ProcessQueueItem(self._sentinel_task),
                    handle=ProcessTaskHandle(
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

    @staticmethod
    def _terminate_process(process: multiprocessing.Process) -> None:
        if process.is_alive():
            process.terminate()
        process.join(timeout=1.0)
        if process.is_alive():
            process.kill()
            process.join(timeout=1.0)

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

    @staticmethod
    def _wait_until_scheduled(entry: _ProcessQueueEntry) -> None:
        if entry.item.scheduled_for is None:
            return
        delay = entry.item.scheduled_for - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
