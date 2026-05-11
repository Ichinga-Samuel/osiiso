"""AsyncQueue — asyncio-native task queue with priorities, retries, and structured concurrency.

Use :class:`AsyncQueue` when you need to run many async (or sync) tasks
concurrently inside a single event loop.  Worker coroutines pull items from a
priority queue and dispatch them; the caller gets a :class:`~osiiso.TaskHandle`
per submission and a :class:`~osiiso.RunSummary` at the end of ``run()``.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import time
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from logging import getLogger
from threading import Lock, get_ident
from typing import Any, Literal

from .exceptions import ClosedError
from .group import TaskGroup
from .handle import TaskHandle
from .items import AsyncItem
from .options import TaskOptions, resolve_opts
from .result import RunSummary, TaskResult, make_result

logger = getLogger(__name__)

FailPolicy = Literal["continue", "fail_first"]
QueueMode = Literal["finite", "infinite"]
ShutdownPolicy = Literal["cancel", "complete_priority"]


@dataclass(order=True, slots=True)
class _Entry:
    scheduled_key: float
    priority: int
    seq: int
    item: AsyncItem = field(compare=False)
    handle: TaskHandle = field(compare=False)
    sentinel: bool = field(default=False, compare=False)


class AsyncQueue:
    """Asyncio task queue with priorities, retries, and graceful shutdown.

    Designed to be used as an async context manager.  On ``__aexit__``
    the queue drains remaining tasks before stopping workers; on error it
    cancels everything immediately.

    Args:
        workers: Fixed number of worker coroutines.  ``None`` lets the queue
            scale automatically up to ``min(32, cpu_count * 4)``.
        size: Maximum items in the internal priority queue (``0`` = unbounded).
        timeout: Per-run time limit in seconds passed to :meth:`run`.
        mode: ``"finite"`` (default) waits for all tasks then stops;
            ``"infinite"`` runs until :meth:`shutdown` is called.
        fail_policy: ``"continue"`` collects all failures (default);
            ``"fail_first"`` cancels remaining tasks after the first failure.
        on_exit: On timeout, ``"complete_priority"`` (default) lets
            must-complete tasks finish; ``"cancel"`` stops everything.
        on_start: Optional callback invoked with the :class:`~osiiso.TaskHandle`
            just before a task begins executing.
        on_complete: Optional callback invoked with the
            :class:`~osiiso.TaskResult` immediately after a task finishes.
        on_retry: Optional callback invoked with the handle and the exception
            that triggered a retry.

    Example::

        async with AsyncQueue(workers=4) as q:
            q.submit(fetch, "https://a.com", retries=3)
            q.submit(fetch, "https://b.com", retries=3)
            summary = await q.run()
            print(summary.values)
    """

    def __init__(
        self,
        *,
        workers: int | None = None,
        size: int = 0,
        timeout: float | None = None,
        mode: QueueMode = "finite",
        fail_policy: FailPolicy = "continue",
        on_exit: ShutdownPolicy = "complete_priority",
        on_start: Callable[[TaskHandle], Any] | None = None,
        on_complete: Callable[[TaskResult], Any] | None = None,
        on_retry: Callable[[TaskHandle, BaseException], Any] | None = None,
    ) -> None:
        """Initialise the queue.  See class docstring for parameter details.

        Raises:
            ValueError: If *size*, *workers*, or *timeout* are out of range.
        """
        if size < 0:
            raise ValueError("size must be >= 0")
        if workers is not None and workers <= 0:
            raise ValueError("workers must be > 0")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0")

        self._queue: asyncio.PriorityQueue[_Entry] = asyncio.PriorityQueue(maxsize=size)
        self._workers = workers
        self._auto_limit = max(4, min(32, (os.cpu_count() or 1) * 4))
        self._timeout = timeout
        self._mode: QueueMode = mode
        self._fail_policy: FailPolicy = fail_policy
        self._on_exit: ShutdownPolicy = on_exit
        self._on_start = on_start
        self._on_complete = on_complete
        self._on_retry = on_retry

        # Internal state
        self._worker_tasks: dict[int, asyncio.Task[None]] = {}
        self._active: dict[str, asyncio.Task[Any]] = {}
        self._handles: dict[str, TaskHandle] = {}
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
        self._shutdown_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_tid: int | None = None
        self._lock = Lock()
        self._start_time = 0.0

    @property
    def active_count(self) -> int:
        """Number of tasks currently executing."""
        return len(self._active)

    @property
    def pending_count(self) -> int:
        """Number of tasks waiting in the queue."""
        return self._queue.qsize()

    @property
    def closed(self) -> bool:
        """``True`` after :meth:`shutdown` has completed."""
        return self._closed

    @property
    def results(self) -> tuple[TaskResult, ...]:
        """Snapshot of all :class:`~osiiso.TaskResult` objects accumulated so far."""
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
            "completed": len(self._results),
            "workers": len(self._worker_tasks),
            "closed": self._closed,
        }

    def submit(self, fn: Any, /, *args: Any, opts: TaskOptions | None = None, **overrides: Any) -> TaskHandle:
        """Submit a single task and return its :class:`~osiiso.TaskHandle`.

        Positional *args* are forwarded to *fn*.  Use :func:`functools.partial`
        for keyword arguments to *fn*.  Options come from *opts* and/or keyword
        overrides; overrides win on collision.

        Args:
            fn: The async or sync callable (or awaitable) to execute.
            *args: Positional arguments forwarded to *fn*.
            opts: Optional base :class:`~osiiso.TaskOptions`.
            **overrides: Field overrides applied on top of *opts*.

        Returns:
            A :class:`~osiiso.TaskHandle` that can be awaited or inspected.

        Raises:
            ~osiiso.ClosedError: If the queue is not accepting tasks.
        """
        effective = resolve_opts(opts, overrides)
        return self._enqueue(fn, args, effective)

    def map(self, fn: Any, iterable: Iterable[Any], *, opts: TaskOptions | None = None, **overrides: Any) -> list[TaskHandle]:
        """Submit *fn* once for each element in *iterable*.

        Element interpretation:

        * ``tuple`` — unpacked as positional args.
        * ``dict`` / :class:`~collections.abc.Mapping` — passed as keyword
          args via :func:`functools.partial`.
        * Any other value — passed as a single positional arg.

        Args:
            fn: Callable to invoke for each element.
            iterable: Elements to map over.
            opts: Optional base :class:`~osiiso.TaskOptions`.
            **overrides: Field overrides applied on top of *opts*.

        Returns:
            A list of :class:`~osiiso.TaskHandle` objects, one per element.
        """
        effective = resolve_opts(opts, overrides)
        handles: list[TaskHandle] = []
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
    ) -> TaskGroup:
        """Submit heterogeneous tasks and return a :class:`TaskGroup` handle.

        Use ``group([(fn, *args), ...])`` for heterogeneous work or
        ``group(fn, iterable)`` for a single callable over many inputs.

        Example::

            grp = q.group([
                (fetch, "https://a.com"),
                (parse, raw_html),
                (save, record, db),
            ])
            summary = await grp.wait()
        """
        effective = resolve_opts(opts, overrides)
        gid = group_id or effective.group_id or f"group-{next(self._counter)}"
        if effective.group_id is None:
            effective = effective.replace(group_id=gid)
        handles: list[TaskHandle] = []
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
        return TaskGroup(gid, handles)

    def task(self, opts: TaskOptions | None = None, **overrides: Any) -> Callable[..., Any]:
        """Decorator that binds a function to this queue.

        Example::

            @q.task(retries=3, timeout=10)
            async def fetch(url): ...

            handle = fetch("https://example.com")
            handles = fetch.map(["url1", "url2"])
        """
        effective = resolve_opts(opts, overrides)

        def decorator(fn: Any) -> _BoundTask:
            return _BoundTask(fn, self, effective)

        return decorator

    async def start(self) -> AsyncQueue:
        """Bind the current event loop and start worker coroutines.

        Called automatically by :meth:`run` and ``__aenter__``.  Safe to
        call explicitly when submitting tasks before entering ``run()``.

        Returns:
            ``self`` for chaining.

        Raises:
            ~osiiso.ClosedError: If :meth:`shutdown` has already completed.
        """
        if self._closed:
            raise ClosedError("queue has been shut down")
        self._bind_loop()
        self._shutdown_event = asyncio.Event()
        self._cancelled = False
        self._stop = False
        self._timed_out = False
        self._start_time = time.perf_counter()
        if not self._started:
            self._started = True
            self._spawn_workers()
        elif self._workers is None:
            self._spawn_workers()
        return self

    async def join(self) -> None:
        """Block until all queued tasks complete (starts the queue if needed)."""
        if not self._started:
            await self.start()
        await self._queue.join()

    async def run(self, timeout: float | None = None, *, strict: bool = False, fail_policy: FailPolicy | None = None) -> RunSummary:
        """Execute all pending tasks and return a :class:`~osiiso.RunSummary`.

        Args:
            timeout: Time limit in seconds for this run.  Overrides the
                queue-level timeout for this call only.  ``None`` = no limit.
            strict: If ``True``, calls :meth:`~osiiso.RunSummary.raise_for_errors`
                on the summary before returning.
            fail_policy: Override the queue-level fail policy for this run
                only.

        Returns:
            A :class:`~osiiso.RunSummary` for all tasks processed in this run.

        Raises:
            RuntimeError: If ``run()`` is already in progress.
            ~osiiso.ExecutionError: If *strict* is ``True`` and any task failed.
        """
        if self._running:
            raise RuntimeError("run() is already in progress")

        self._running = True
        run_start = time.perf_counter()
        idx = len(self._results)
        effective_timeout = timeout if timeout is not None else self._timeout
        saved_fp = self._fail_policy
        if fail_policy is not None:
            self._fail_policy = fail_policy

        await self.start()
        try:
            if self._mode == "finite":
                await self._run_finite(effective_timeout)
            else:
                await self._run_infinite(effective_timeout)
        finally:
            summary = RunSummary.from_results(self._results[idx:], run_start=run_start, timed_out=self._timed_out)
            self._timed_out = False
            self._running = False
            self._fail_policy = saved_fp

        if strict:
            summary.raise_for_errors()
        return summary

    async def shutdown(self, *, force: bool = False) -> None:
        """Stop the queue.

        Args:
            force: If ``True``, cancel all pending and active tasks
                immediately.  If ``False`` (default), drain the queue first.
        """
        self._accepting = False
        self._stop = True
        if force:
            self._cancelled = True
            self._cancel_pending(force=True)
            self._cancel_active(force=True)
        else:
            if not self._started and self.pending_count > 0:
                await self.start()
            await self._queue.join()
        await self._stop_workers()
        self._closed = True
        if self._shutdown_event is not None:
            self._shutdown_event.set()

    async def __aenter__(self) -> AsyncQueue:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.shutdown(force=exc_type is not None)

    def cancel(self) -> asyncio.Task[None] | None:
        """Request immediate cancellation of the queue from any thread.

        Safe to call from outside the event-loop thread.  Schedules a
        ``force=True`` shutdown coroutine on the running loop.

        Returns:
            An :class:`asyncio.Task` if called from the loop thread,
            ``None`` otherwise.
        """
        self._cancelled = True
        self._stop = True
        loop = self._loop
        if loop is not None and loop.is_running():
            if self._loop_tid == get_ident():
                return loop.create_task(self.shutdown(force=True))
            asyncio.run_coroutine_threadsafe(self.shutdown(force=True), loop)
            return None
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            self._accepting = False
            self._closed = True
            self._cancel_pending(force=True)
            return None
        return running.create_task(self.shutdown(force=True))

    def reset(self) -> None:
        """Reset for reuse after ``run()`` or ``shutdown()``."""
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
        self._loop = None
        self._loop_tid = None
        self._shutdown_event = None

    def clear_results(self) -> None:
        """Discard all accumulated :class:`~osiiso.TaskResult` objects to free memory."""
        self._results.clear()

    @staticmethod
    async def as_completed(handles: Iterable[TaskHandle]) -> AsyncIterator[TaskHandle]:
        """Yield handles in completion order, fastest first.

        Args:
            handles: Any iterable of :class:`~osiiso.TaskHandle` objects.

        Yields:
            Each handle as its underlying task finishes.
        """
        tasks = {asyncio.ensure_future(h.wait()): h for h in handles}
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                yield tasks[fut]

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._loop is None:
                self._loop = loop
                self._loop_tid = get_ident()
                return
            if self._loop is not loop and (self._worker_tasks or self._active):
                raise RuntimeError("cannot switch event loops while running")
            self._loop = loop
            self._loop_tid = get_ident()

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> TaskHandle:
        if self._closed or not self._accepting or self._stop:
            raise ClosedError("queue is not accepting tasks")

        item = AsyncItem(fn, args, opts)
        handle = TaskHandle(
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
        self._queue.put_nowait(entry)
        self._handles[handle.task_id] = handle
        if self._started:
            self._spawn_workers()
        return handle

    def _desired_count(self) -> int:
        if self._workers is not None:
            return self._workers
        backlog = self._queue.qsize() + len(self._active)
        if backlog == 0:
            return 1 if self._mode == "infinite" else 0
        return max(1, min(self._auto_limit, backlog))

    def _spawn_workers(self) -> None:
        if self._closed or self._stop:
            return
        target = self._desired_count()
        while len(self._worker_tasks) < target:
            wid = next(self._wids)
            self._worker_tasks[wid] = asyncio.create_task(self._worker(wid), name=f"TQ-Worker-{wid}")

    async def _worker(self, wid: int) -> None:
        try:
            while True:
                entry = await self._queue.get()
                if entry.sentinel:
                    self._queue.task_done()
                    break
                if self._stop and not entry.item.opts.must_complete:
                    self._record_cancelled(entry, "skipped during shutdown")
                    self._queue.task_done()
                    continue
                try:
                    await self._wait_scheduled(entry)
                    await self._execute(entry)
                finally:
                    self._queue.task_done()
        finally:
            self._worker_tasks.pop(wid, None)

    async def _execute(self, entry: _Entry) -> None:
        item, handle = entry.item, entry.handle
        delay = item.opts.retry_delay

        while True:
            handle._mark_running()
            self._fire_start(handle)
            task = asyncio.create_task(item(), name=f"TQ-Task-{handle.task_id[:8]}")
            self._active[handle.task_id] = task
            try:
                if item.opts.timeout is None:
                    value = await task
                else:
                    value = await asyncio.wait_for(task, timeout=item.opts.timeout)
            except asyncio.CancelledError:
                self._record(handle, make_result(handle, status="cancelled", message="cancelled"))
                return
            except Exception as exc:
                if handle.attempts <= item.opts.retries:
                    handle._mark_retrying()
                    self._fire_retry(handle, exc)
                    if delay > 0:
                        await asyncio.sleep(delay)
                        delay *= item.opts.backoff
                    elif item.opts.retry_delay > 0:
                        delay = item.opts.retry_delay * item.opts.backoff
                    continue
                if not isinstance(exc, asyncio.TimeoutError):
                    logger.exception("Task %s failed", handle.name)
                self._record(handle, make_result(handle, status="failed", exception=exc, message=str(exc)))
                return
            else:
                self._record(handle, make_result(handle, status="succeeded", value=value))
                return
            finally:
                self._active.pop(handle.task_id, None)

    async def _run_finite(self, timeout: float | None) -> None:
        try:
            if timeout is None:
                await self.join()
            else:
                await asyncio.wait_for(self.join(), timeout=timeout)
        except TimeoutError:
            self._timed_out = True
            await self._handle_timeout()
        finally:
            await self._stop_workers()
            self._stop = False

    async def _run_infinite(self, timeout: float | None) -> None:
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        try:
            if timeout is None:
                await self._shutdown_event.wait()
            else:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=timeout)
        except TimeoutError:
            self._timed_out = True
            await self._handle_timeout()
            await self._stop_workers()
            self._stop = False
            if self._shutdown_event:
                self._shutdown_event.set()
        else:
            if not self._closed:
                await self._stop_workers()

    async def _handle_timeout(self) -> None:
        self._stop = True
        if self._on_exit == "cancel":
            self._cancelled = True
            self._cancel_pending(force=True)
            self._cancel_active(force=True)
            return
        self._cancel_pending(force=False)
        self._cancel_active(force=False)
        await self._queue.join()

    def _cancel_task(self, task_id: str) -> bool:
        h = self._handles.get(task_id)
        if h is None or h.done():
            return False
        t = self._active.get(task_id)
        if t is not None:
            t.cancel()
            return True
        return self._cancel_pending(force=True, only={task_id}) > 0

    def _cancel_pending(self, *, force: bool, only: set[str] | None = None) -> int:
        retained: list[_Entry] = []
        count = 0
        while True:
            try:
                e = self._queue.get_nowait()
            except asyncio.QueueEmpty:
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
        for tid, t in list(self._active.items()):
            h = self._handles.get(tid)
            if h is None or h.done():
                continue
            if force or not h.must_complete:
                t.cancel()

    def _record_cancelled(self, entry: _Entry, msg: str) -> None:
        h = entry.handle
        if h.done():
            return
        self._record(h, make_result(h, status="cancelled", message=msg))

    def _record(self, handle: TaskHandle, result: TaskResult) -> None:
        handle._mark_finished(result)
        self._results.append(result)
        if result.status == "failed" and self._fail_policy == "fail_first" and not self._stop:
            self._stop = True
            self._cancelled = True
            self._cancel_pending(force=True)
            self._cancel_active(force=True)
        self._fire_complete(result)

    async def _stop_workers(self) -> None:
        if not self._worker_tasks:
            self._started = False
            return
        workers = list(self._worker_tasks.values())
        for _ in workers:
            self._queue.put_nowait(
                _Entry(
                    scheduled_key=0.0,
                    priority=10**9,
                    seq=next(self._counter),
                    item=AsyncItem(self._noop, (), TaskOptions()),
                    handle=TaskHandle(
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
        await asyncio.gather(*workers, return_exceptions=True)
        self._worker_tasks.clear()
        self._started = False

    @staticmethod
    async def _noop() -> None:
        return None

    @staticmethod
    async def _wait_scheduled(entry: _Entry) -> None:
        if entry.item.scheduled_for is None:
            return
        delay = entry.item.scheduled_for - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)

    def _fire_start(self, handle: TaskHandle) -> None:
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

    def _fire_retry(self, handle: TaskHandle, exc: BaseException) -> None:
        if self._on_retry is not None:
            try:
                self._on_retry(handle, exc)
            except Exception:
                logger.exception("on_retry callback raised")


class _BoundTask:
    """A function bound to a queue via the ``@q.task()`` decorator."""

    __slots__ = ("_fn", "_queue", "_opts")

    def __init__(self, fn: Any, queue: AsyncQueue, opts: TaskOptions) -> None:
        self._fn = fn
        self._queue = queue
        self._opts = opts

    def __call__(self, *args: Any, **overrides: Any) -> TaskHandle:
        effective = resolve_opts(self._opts, overrides) if overrides else self._opts
        return self._queue._enqueue(self._fn, args, effective)

    def map(self, iterable: Iterable[Any], **overrides: Any) -> list[TaskHandle]:
        effective = resolve_opts(self._opts, overrides) if overrides else self._opts
        return self._queue.map(self._fn, iterable, opts=effective)

    def group(self, iterable: Iterable[Any], **overrides: Any) -> TaskGroup:
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

    def __repr__(self) -> str:
        return f"BoundTask({self.__name__!r})"
