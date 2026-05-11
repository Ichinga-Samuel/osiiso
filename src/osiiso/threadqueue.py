"""ThreadQueue — thread-based task queue with priorities, retries, and graceful shutdown.

Use :class:`ThreadQueue` when you need to run many blocking (CPU-light or
I/O-bound) tasks concurrently from synchronous code, without an event loop.
Each item runs in a daemon thread; the caller gets a :class:`~osiiso.SyncTaskHandle`
per submission and a :class:`~osiiso.RunSummary` at the end of ``run()``.
"""

from __future__ import annotations

import itertools
import os
import queue as queue_mod
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Literal

from .exceptions import ClosedError
from .group import SyncTaskGroup
from .handle import SyncTaskHandle
from .items import ThreadItem
from .options import TaskOptions, resolve_opts
from .result import RunSummary, TaskResult, make_result

logger = getLogger(__name__)

FailPolicy = Literal["continue", "fail_first"]
QueueMode = Literal["finite", "infinite"]


class _Cancelled(Exception):
    """Internal cooperative cancellation signal."""


@dataclass(order=True, slots=True)
class _Entry:
    scheduled_key: float
    priority: int
    seq: int
    item: ThreadItem = field(compare=False)
    handle: SyncTaskHandle = field(compare=False)
    sentinel: bool = field(default=False, compare=False)


class ThreadQueue:
    """Thread-based task queue with priorities, retries, and graceful shutdown.

    Designed to be used as a synchronous context manager.  On ``__exit__``
    the queue drains remaining tasks before stopping workers; on error it
    cancels everything immediately.

    Args:
        workers: Fixed number of worker threads.  ``None`` lets the queue
            scale automatically up to ``min(32, cpu_count * 4)``.
        size: Maximum items in the internal priority queue (``0`` = unbounded).
        timeout: Per-run time limit in seconds passed to :meth:`run`.
        mode: ``"finite"`` (default) waits for all tasks then stops;
            ``"infinite"`` runs until :meth:`shutdown` is called.
        fail_policy: ``"continue"`` collects all failures (default);
            ``"fail_first"`` cancels remaining tasks after the first failure.
        on_exit: On timeout, ``"complete_priority"`` (default) lets
            must-complete tasks finish; ``"cancel"`` stops everything.
        poll: Seconds between cancellation/timeout checks inside workers.
        on_start: Optional callback invoked with the
            :class:`~osiiso.SyncTaskHandle` just before a task executes.
        on_complete: Optional callback invoked with the
            :class:`~osiiso.TaskResult` immediately after a task finishes.
        on_retry: Optional callback invoked with the handle and the exception
            that triggered a retry.

    Example::

        with ThreadQueue(workers=4) as q:
            q.submit(process, data1)
            q.submit(process, data2)
            summary = q.run()
    """

    def __init__(
        self,
        *,
        workers: int | None = None,
        size: int = 0,
        timeout: float | None = None,
        mode: QueueMode = "finite",
        fail_policy: FailPolicy = "continue",
        on_exit: Literal["cancel", "complete_priority"] = "complete_priority",
        poll: float = 0.05,
        on_start: Callable[[SyncTaskHandle], Any] | None = None,
        on_complete: Callable[[TaskResult], Any] | None = None,
        on_retry: Callable[[SyncTaskHandle, BaseException], Any] | None = None) -> None:
        """Initialise the queue.  See class docstring for parameter details.

        Raises:
            ValueError: If *size*, *workers*, *timeout*, or *poll* are out of range.
        """

        if size < 0:
            raise ValueError("size must be >= 0")
        if workers is not None and workers <= 0:
            raise ValueError("workers must be > 0")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0")
        if poll <= 0:
            raise ValueError("poll must be > 0")

        self._queue: queue_mod.PriorityQueue[_Entry] = queue_mod.PriorityQueue(maxsize=size)
        self._workers = workers
        self._auto_limit = max(4, min(32, (os.cpu_count() or 1) * 4))
        self._timeout = timeout
        self._mode: QueueMode = mode
        self._fail_policy: FailPolicy = fail_policy
        self._on_exit = on_exit
        self._poll = poll
        self._on_start = on_start
        self._on_complete = on_complete
        self._on_retry = on_retry

        self._threads: dict[int, threading.Thread] = {}
        self._active: dict[str, threading.Event] = {}  # task_id -> cancel_event
        self._handles: dict[str, SyncTaskHandle] = {}
        self._results: list[TaskResult] = []
        self._counter = itertools.count()
        self._wids = itertools.count(1)
        self._accepting = True
        self._closed = False
        self._started = False
        self._stop = False
        self._cancelled = False
        self._timed_out = False
        self._running = False
        self._shutdown_event = threading.Event()
        self._lock = threading.RLock()
        self._start_time = 0.0

    @property
    def active_count(self) -> int:
        """Number of tasks currently executing."""
        with self._lock:
            return len(self._active)

    @property
    def pending_count(self) -> int:
        """Number of tasks waiting in the queue."""
        return self._queue.qsize()

    @property
    def closed(self) -> bool:
        """``True`` after :meth:`shutdown` has completed."""
        with self._lock:
            return self._closed

    @property
    def results(self) -> tuple[TaskResult, ...]:
        """Snapshot of all :class:`~osiiso.TaskResult` objects accumulated so far."""
        with self._lock:
            return tuple(self._results)

    @property
    def stats(self) -> dict[str, int | bool]:
        """Snapshot of current queue metrics.

        Returns:
            A dict with keys ``pending``, ``active``, ``completed``,
            ``workers``, and ``closed``.
        """
        return {
            "pending": self.pending_count,
            "active": self.active_count,
            "completed": len(self.results),
            "workers": len(self._threads),
            "closed": self.closed,
        }

    def submit(self, fn: Any, /, *args: Any, opts: TaskOptions | None = None, **overrides: Any) -> SyncTaskHandle:
        """Submit a task and return a blocking :class:`~osiiso.SyncTaskHandle`.

        Args:
            fn: The sync callable to execute.
            *args: Positional arguments forwarded to *fn*.
            opts: Optional base :class:`~osiiso.TaskOptions`.
            **overrides: Field overrides applied on top of *opts*.

        Returns:
            A :class:`~osiiso.SyncTaskHandle` that blocks on :meth:`~osiiso.SyncTaskHandle.wait`.

        Raises:
            ~osiiso.ClosedError: If the queue is not accepting tasks.
            TypeError: If *fn* is a coroutine function or awaitable.
        """
        effective = resolve_opts(opts, overrides)
        return self._enqueue(fn, args, effective)

    def map(self, fn: Any, iterable: Iterable[Any], *, opts: TaskOptions | None = None, **overrides: Any) -> list[SyncTaskHandle]:
        """Submit *fn* once for each element in *iterable*.

        Element interpretation mirrors :meth:`submit`: tuples are unpacked as
        positional args, mappings are passed via ``functools.partial``, and all
        other values are passed as a single positional arg.

        Args:
            fn: Callable to invoke for each element.
            iterable: Elements to map over.
            opts: Optional base :class:`~osiiso.TaskOptions`.
            **overrides: Field overrides applied on top of *opts*.

        Returns:
            A list of :class:`~osiiso.SyncTaskHandle` objects, one per element.
        """
        effective = resolve_opts(opts, overrides)
        handles: list[SyncTaskHandle] = []
        for entry in iterable:
            if isinstance(entry, Mapping):
                from functools import partial

                handles.append(self._enqueue(partial(fn, **entry), (), effective))
            elif isinstance(entry, tuple):
                handles.append(self._enqueue(fn, entry, effective))
            else:
                handles.append(self._enqueue(fn, (entry,), effective))
        return handles

    def group(
        self,
        tasks: Any,
        iterable: Iterable[Any] | None = None,
        *,
        group_id: str | None = None,
        opts: TaskOptions | None = None,
        **overrides: Any,
    ) -> SyncTaskGroup:
        """Submit heterogeneous tasks and return a :class:`SyncTaskGroup` handle.

        Use ``group([(fn, *args), ...])`` for heterogeneous work or
        ``group(fn, iterable)`` for a single callable over many inputs.

        Example::

            grp = q.group([
                (process, data1),
                (validate, data2),
                (save, record, db),
            ])
            summary = grp.wait()
        """
        effective = resolve_opts(opts, overrides)
        gid = group_id or effective.group_id or f"group-{next(self._counter)}"
        if effective.group_id is None:
            effective = effective.replace(group_id=gid)
        handles: list[SyncTaskHandle] = []
        if iterable is not None:
            fn = tasks
            for entry in iterable:
                if isinstance(entry, Mapping):
                    from functools import partial

                    handles.append(self._enqueue(partial(fn, **entry), (), effective))
                elif isinstance(entry, tuple):
                    handles.append(self._enqueue(fn, entry, effective))
                else:
                    handles.append(self._enqueue(fn, (entry,), effective))
        else:
            for entry in tasks:
                if not isinstance(entry, tuple) or len(entry) < 1:
                    raise TypeError("each group entry must be a tuple of (callable, *args)")
                fn, *args = entry
                handles.append(self._enqueue(fn, tuple(args), effective))
        return SyncTaskGroup(gid, handles)

    def task(self, opts: TaskOptions | None = None, **overrides: Any) -> Callable[..., Any]:
        """Decorator that binds a callable to this queue.

        The decorated function is replaced by a :class:`_BoundSyncTask` that,
        when called, submits the original function with the specified options.

        Example::

            @q.task(retries=3, timeout=10)
            def process(data): ...

            handle = process(my_data)
            handles = process.map([data1, data2])
        """
        effective = resolve_opts(opts, overrides)

        def decorator(fn: Any) -> _BoundSyncTask:
            return _BoundSyncTask(fn, self, effective)

        return decorator

    def start(self) -> ThreadQueue:
        """Start worker threads and reset transient state for a new run.

        Called automatically by :meth:`run` and ``__enter__``.  Safe to call
        explicitly before submitting tasks.

        Returns:
            ``self`` for chaining.

        Raises:
            ~osiiso.ClosedError: If :meth:`shutdown` has already completed.
        """
        with self._lock:
            if self._closed:
                raise ClosedError("queue has been shut down")
            self._shutdown_event = threading.Event()
            self._accepting = True
            self._cancelled = False
            self._stop = False
            self._timed_out = False
            self._start_time = time.perf_counter()
            need_spawn = not self._started
            if not self._started:
                self._started = True
            elif self._workers is None:
                need_spawn = True
        if need_spawn:
            self._spawn_workers()
        return self

    def join(self) -> None:
        """Block until all queued tasks complete (starts the queue if needed)."""
        if not self._started:
            self.start()
        self._wait_completion(timeout=None)

    def run(self, timeout: float | None = None, *, strict: bool = False, fail_policy: FailPolicy | None = None) -> RunSummary:
        """Execute all pending tasks and return a :class:`~osiiso.RunSummary`.

        Args:
            timeout: Time limit in seconds for this run.  ``None`` = no limit.
            strict: If ``True``, raises :class:`~osiiso.ExecutionError` when
                any task failed.
            fail_policy: Override the queue-level fail policy for this run only.

        Returns:
            A :class:`~osiiso.RunSummary` for all tasks processed in this run.

        Raises:
            RuntimeError: If ``run()`` is already in progress.
            ~osiiso.ExecutionError: If *strict* is ``True`` and any task failed.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("run() is already in progress")
            self._running = True
            idx = len(self._results)

        run_start = time.perf_counter()
        effective_timeout = timeout if timeout is not None else self._timeout
        saved_fp = self._fail_policy
        if fail_policy is not None:
            self._fail_policy = fail_policy

        self.start()
        try:
            if self._mode == "finite":
                self._run_finite(effective_timeout)
            else:
                self._run_infinite(effective_timeout)
        finally:
            with self._lock:
                collected = list(self._results[idx:])
                timed_out_snapshot = self._timed_out
                self._timed_out = False
                self._running = False
                self._fail_policy = saved_fp
            summary = RunSummary.from_results(collected, run_start=run_start, timed_out=timed_out_snapshot)

        if strict:
            summary.raise_for_errors()
        return summary

    def shutdown(self, *, force: bool = False) -> None:
        """Stop the queue.

        Args:
            force: If ``True``, cancel all pending and active tasks
                immediately.  If ``False`` (default), drain the queue first.
        """
        with self._lock:
            self._accepting = False
        if force:
            with self._lock:
                self._cancelled = True
                self._stop = True
            self._cancel_pending(force=True)
            self._cancel_active(force=True)
        else:
            if not self._started and self.pending_count > 0:
                self.start()
            self._wait_completion(timeout=None)
            with self._lock:
                self._stop = True
        self._stop_workers()
        with self._lock:
            self._closed = True
            self._shutdown_event.set()

    def __enter__(self) -> ThreadQueue:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.shutdown(force=exc_type is not None)

    def cancel(self) -> threading.Thread | None:
        """Request immediate cancellation of the queue.

        Safe to call from a worker thread; in that case a background thread
        performs the forced shutdown to avoid a deadlock.

        Returns:
            A :class:`threading.Thread` if called from inside a worker,
            ``None`` otherwise.
        """
        current = threading.current_thread()
        if any(w is current for w in self._threads.values()):
            t = threading.Thread(target=self.shutdown, kwargs={"force": True}, daemon=True)
            t.start()
            return t
        self.shutdown(force=True)
        return None

    def reset(self) -> None:
        """Reset accumulated state so the queue can be reused after ``run()``.

        Clears all stored results, handles, and internal counters.

        Raises:
            RuntimeError: If called while ``run()`` is in progress.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("cannot reset during run()")
            self._results.clear()
            self._handles.clear()
            self._closed = False
            self._accepting = True
            self._started = False
            self._stop = False
            self._cancelled = False
            self._timed_out = False
            self._counter = itertools.count()
            self._wids = itertools.count(1)
            self._shutdown_event = threading.Event()

    def clear_results(self) -> None:
        """Discard all accumulated :class:`~osiiso.TaskResult` objects to free memory."""
        with self._lock:
            self._results.clear()

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> SyncTaskHandle:
        with self._lock:
            if self._closed or not self._accepting or self._stop:
                raise ClosedError("queue is not accepting tasks")

        item = ThreadItem(fn, args, opts)
        handle = SyncTaskHandle(
            task_id=item.task_id,
            name=item.name,
            priority=opts.priority,
            must_complete=opts.must_complete,
            created_at=item.created_at,
            cancel_fn=self._cancel_task,
            group_id=opts.group_id,
            detached=opts.detached,
            scheduled_for=item.scheduled_for,
        )

        entry = _Entry(
            scheduled_key=item.scheduled_for or 0.0, priority=opts.priority, seq=next(self._counter), item=item, handle=handle
        )

        with self._lock:
            if self._closed or not self._accepting or self._stop:
                raise ClosedError("queue is not accepting tasks")
            self._queue.put_nowait(entry)
            self._handles[handle.task_id] = handle
            need_spawn = self._started

        if need_spawn:
            self._spawn_workers()
        return handle

    def _desired_count(self) -> int:
        if self._workers is not None:
            return self._workers
        backlog = self._queue.qsize() + self.active_count
        if backlog == 0:
            return 1 if self._mode == "infinite" else 0
        return max(1, min(self._auto_limit, backlog))

    def _spawn_workers(self) -> None:
        with self._lock:
            if self._closed or self._stop:
                return
            target = self._desired_count()
            to_start: list[threading.Thread] = []
            while len(self._threads) < target:
                wid = next(self._wids)
                t = threading.Thread(target=self._worker, args=(wid,), name=f"TTQ-Worker-{wid}", daemon=True)
                self._threads[wid] = t
                to_start.append(t)
        for t in to_start:
            t.start()

    def _worker(self, wid: int) -> None:
        try:
            while True:
                entry = self._queue.get()
                if entry.sentinel:
                    self._queue.task_done()
                    break
                if self._stop and not entry.item.opts.must_complete:
                    self._record_cancelled(entry, "skipped during shutdown")
                    self._queue.task_done()
                    continue
                try:
                    self._wait_scheduled(entry)
                    self._execute(entry)
                finally:
                    self._queue.task_done()
        finally:
            with self._lock:
                self._threads.pop(wid, None)

    def _execute(self, entry: _Entry) -> None:
        item, handle = entry.item, entry.handle
        delay = item.opts.retry_delay
        cancel_event = threading.Event()

        with self._lock:
            self._active[handle.task_id] = cancel_event

        try:
            while True:
                if cancel_event.is_set():
                    raise _Cancelled
                handle._mark_running()
                self._fire_start(handle)

                try:
                    value = self._run_with_controls(item, cancel_event)
                except _Cancelled:
                    raise
                except Exception as exc:
                    if not cancel_event.is_set() and handle.attempts <= item.opts.retries:
                        handle._mark_retrying()
                        self._fire_retry(handle, exc)
                        if delay > 0:
                            self._interruptible_sleep(delay, cancel_event)
                            delay *= item.opts.backoff
                        elif item.opts.retry_delay > 0:
                            delay = item.opts.retry_delay * item.opts.backoff
                        continue
                    if not isinstance(exc, TimeoutError):
                        logger.exception("Task %s failed", handle.name)
                    self._record(handle, make_result(handle, status="failed", exception=exc, message=str(exc)))
                    return
                else:
                    self._record(handle, make_result(handle, status="succeeded", value=value))
                    return
        except _Cancelled:
            self._record(handle, make_result(handle, status="cancelled", message="cancelled"))
        finally:
            with self._lock:
                self._active.pop(handle.task_id, None)

    def _run_with_controls(self, item: ThreadItem, cancel_event: threading.Event) -> Any:
        result_q: queue_mod.Queue[tuple[str, Any]] = queue_mod.Queue(maxsize=1)
        t = threading.Thread(target=self._invoke, args=(item, result_q), daemon=True)
        t.start()
        deadline = None if item.opts.timeout is None else time.perf_counter() + item.opts.timeout

        while t.is_alive():
            if cancel_event.is_set():
                raise _Cancelled
            if deadline is not None:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    raise TimeoutError(f"Task '{item.name}' exceeded {item.opts.timeout}s timeout")
                t.join(timeout=min(self._poll, remaining))
            else:
                t.join(timeout=self._poll)

        if cancel_event.is_set():
            raise _Cancelled

        kind, payload = result_q.get_nowait()
        if kind == "error":
            raise payload
        return payload

    @staticmethod
    def _invoke(item: ThreadItem, result_q: queue_mod.Queue[tuple[str, Any]]) -> None:
        try:
            result_q.put(("value", item()))
        except BaseException as exc:
            result_q.put(("error", exc))

    def _interruptible_sleep(self, delay: float, cancel_event: threading.Event) -> None:
        deadline = time.perf_counter() + delay
        while True:
            if cancel_event.is_set():
                raise _Cancelled
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                return
            cancel_event.wait(timeout=min(self._poll, remaining))

    def _run_finite(self, timeout: float | None) -> None:
        try:
            if not self._wait_completion(timeout=timeout):
                raise TimeoutError
        except TimeoutError:
            self._timed_out = True
            self._handle_timeout()
        finally:
            self._stop_workers()
            self._stop = False

    def _run_infinite(self, timeout: float | None) -> None:
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
            self._stop = False
            self._shutdown_event.set()
        elif not self._closed:
            self._stop_workers()

    def _handle_timeout(self) -> None:
        self._stop = True
        if self._on_exit == "cancel":
            self._cancelled = True
            self._cancel_pending(force=True)
            self._cancel_active(force=True)
            return
        self._cancel_pending(force=False)
        self._cancel_active(force=False)
        self._wait_completion(timeout=None)

    def _cancel_task(self, task_id: str) -> bool:
        with self._lock:
            h = self._handles.get(task_id)
            if h is None or h.done():
                return False
            ev = self._active.get(task_id)
            if ev is not None:
                ev.set()
                return True
        return self._cancel_pending(force=True, only={task_id}) > 0

    def _cancel_pending(self, *, force: bool, only: set[str] | None = None) -> int:
        retained: list[_Entry] = []
        count = 0
        while True:
            try:
                e = self._queue.get_nowait()
            except queue_mod.Empty:
                break
            if e.sentinel:
                self._queue.task_done()
                retained.append(e)
                continue
            selected = only is None or e.handle.task_id in only
            should = selected and (force or not e.item.opts.must_complete)
            if should:
                count += 1
                self._record_cancelled(e, "cancelled before execution")
                self._queue.task_done()
            else:
                retained.append(e)
                self._queue.task_done()
        for e in retained:
            self._queue.put_nowait(e)
        return count

    def _cancel_active(self, *, force: bool) -> None:
        with self._lock:
            entries = list(self._active.items())
        for tid, ev in entries:
            h = self._handles.get(tid)
            if h is None or h.done():
                continue
            if force or not h.must_complete:
                ev.set()

    def _record_cancelled(self, entry: _Entry, msg: str) -> None:
        h = entry.handle
        if h.done():
            return
        self._record(h, make_result(h, status="cancelled", message=msg))

    def _record(self, handle: SyncTaskHandle, result: TaskResult) -> None:
        handle._mark_finished(result)
        with self._lock:
            self._results.append(result)
        if result.status == "failed" and self._fail_policy == "fail_first" and not self._stop:
            with self._lock:
                self._stop = True
                self._cancelled = True
            self._cancel_pending(force=True)
            self._cancel_active(force=True)
        self._fire_complete(result)

    def _stop_workers(self) -> None:
        with self._lock:
            workers = list(self._threads.values())
        if not workers:
            with self._lock:
                self._started = False
            return
        for _ in workers:
            self._queue.put(
                _Entry(
                    scheduled_key=0.0,
                    priority=10**9,
                    seq=next(self._counter),
                    item=ThreadItem(self._noop, (), TaskOptions()),
                    handle=SyncTaskHandle(
                        task_id=f"sentinel-{next(self._counter)}",
                        name="sentinel",
                        priority=10**9,
                        must_complete=True,
                        created_at=time.perf_counter(),
                        cancel_fn=lambda _: False,
                    ),
                    sentinel=True,
                )
            )
        current = threading.current_thread()
        for w in workers:
            if w is not current:
                w.join()
        with self._lock:
            self._threads.clear()
            self._started = False

    @staticmethod
    def _noop() -> None:
        return None

    def _wait_completion(self, *, timeout: float | None) -> bool:
        with self._queue.all_tasks_done:
            if timeout is None:
                while self._queue.unfinished_tasks:
                    self._queue.all_tasks_done.wait()
                return True
            if timeout <= 0:
                return self._queue.unfinished_tasks == 0
            deadline = time.perf_counter() + timeout
            while self._queue.unfinished_tasks:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(timeout=remaining)
            return True

    @staticmethod
    def _wait_scheduled(entry: _Entry) -> None:
        if entry.item.scheduled_for is None:
            return
        delay = entry.item.scheduled_for - time.perf_counter()
        if delay > 0:
            time.sleep(delay)

    def _fire_start(self, handle: SyncTaskHandle) -> None:
        if self._on_start is not None:
            try:
                self._on_start(handle)
            except Exception:
                logger.exception("on_start callback raised")

    def _fire_complete(self, result: TaskResult) -> None:
        if self._on_complete is not None:
            try:
                self._on_complete(result)
            except Exception:
                logger.exception("on_complete callback raised")

    def _fire_retry(self, handle: SyncTaskHandle, exc: BaseException) -> None:
        if self._on_retry is not None:
            try:
                self._on_retry(handle, exc)
            except Exception:
                logger.exception("on_retry callback raised")


class _BoundSyncTask:
    """A function bound to a ThreadQueue via ``@q.task()``."""

    __slots__ = ("_fn", "_queue", "_opts")

    def __init__(self, fn: Any, queue: ThreadQueue, opts: TaskOptions) -> None:
        self._fn = fn
        self._queue = queue
        self._opts = opts

    def __call__(self, *args: Any, **overrides: Any) -> SyncTaskHandle:
        effective = resolve_opts(self._opts, overrides) if overrides else self._opts
        return self._queue._enqueue(self._fn, args, effective)

    def map(self, iterable: Iterable[Any], **overrides: Any) -> list[SyncTaskHandle]:
        effective = resolve_opts(self._opts, overrides) if overrides else self._opts
        return self._queue.map(self._fn, iterable, opts=effective)

    def group(self, iterable: Iterable[Any], **overrides: Any) -> SyncTaskGroup:
        effective = resolve_opts(self._opts, overrides) if overrides else self._opts
        tasks = []
        for entry in iterable:
            if isinstance(entry, tuple):
                tasks.append((self._fn, *entry))
            else:
                tasks.append((self._fn, entry))
        return self._queue.group(tasks, opts=effective)

    @property
    def __name__(self) -> str:
        return getattr(self._fn, "__name__", type(self._fn).__name__)
