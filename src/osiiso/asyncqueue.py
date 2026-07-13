"""AsyncQueue — asyncio-native task queue with priorities, retries, scheduling, and rate limiting.

Worker coroutines pull from a priority queue; each submission returns a
:class:`~osiiso.TaskHandle` and each ``run()`` a :class:`~osiiso.RunSummary`.
Delayed tasks are armed on loop timers, so they never occupy a worker before
they are due.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import threading
import time
from collections.abc import Callable
from inspect import isawaitable, iscoroutinefunction
from typing import Any

from .core import FailPolicy, QueueMode, TimeoutPolicy, _check_queue_args, _emit, _RateGate, _stop_task, _SubmitPlane, _Task
from .exceptions import ClosedError, QueueFullError
from .group import TaskGroup
from .handle import TaskHandle
from .options import TaskOptions
from .result import RunSummary, TaskResult, make_result


class AsyncQueue(_SubmitPlane):
    """Asyncio task queue with priorities, retries, and graceful shutdown.

    Use as an async context manager: on ``__aexit__`` the queue drains
    remaining tasks before stopping workers, or cancels everything if the
    block raised.

    Args:
        workers: Fixed number of worker coroutines.  ``None`` (default) scales
            with the backlog up to ``min(32, cpu_count * 4)``.
        size: Maximum outstanding (queued + scheduled + running) tasks.
            ``0`` = unbounded.  When full, ``submit`` raises
            :class:`~osiiso.QueueFullError`.
        timeout: Default per-run time limit in seconds for :meth:`run`.
        mode: ``"finite"`` (default) — ``run()`` returns once all tasks finish;
            ``"infinite"`` — ``run()`` blocks until :meth:`shutdown`.
        fail_policy: ``"continue"`` (default) collects all failures;
            ``"fail_first"`` aborts remaining tasks after the first failure
            (``must_complete`` tasks are spared).
        on_timeout: When a run times out — ``"complete"`` (default) lets
            ``must_complete`` tasks finish; ``"cancel"`` stops everything.
        rate: Maximum task attempts per second across all workers (``None`` = unlimited).
        burst: With *rate*, how many attempts may start back-to-back after idle.
        on_start: ``fn(handle)`` called as each attempt begins.
        on_complete: ``fn(result)`` called as each task finishes.
        on_retry: ``fn(handle, exception)`` called before each retry.

    Example::

        async with AsyncQueue(workers=4, rate=10) as q:
            q.map(fetch, urls, retries=3)
            summary = await q.run()
            print(summary.values)

    Note:
        The queue is bound to a single event loop; ``submit()`` must be called
        from the loop's thread (use :class:`~osiiso.SharedQueue` to submit
        from other threads).  ``handle.cancel()`` and ``queue.cancel()``
        are safe from any thread.
    """

    _group_cls = TaskGroup
    _handle_cls = TaskHandle

    def __init__(
        self,
        *,
        workers: int | None = None,
        size: int = 0,
        timeout: float | None = None,
        mode: QueueMode = "finite",
        fail_policy: FailPolicy = "continue",
        on_timeout: TimeoutPolicy = "complete",
        rate: float | None = None,
        burst: int = 1,
        on_start: Callable[[TaskHandle], Any] | None = None,
        on_complete: Callable[[TaskResult], Any] | None = None,
        on_retry: Callable[[TaskHandle, BaseException], Any] | None = None,
    ) -> None:
        _check_queue_args(workers, size, timeout, rate, burst)
        self._ready: asyncio.PriorityQueue[_Task] = asyncio.PriorityQueue()
        self._workers = workers
        self._auto_limit = max(4, min(32, (os.cpu_count() or 1) * 4))
        self._size = size
        self._timeout = timeout
        self._mode: QueueMode = mode
        self._fail_policy: FailPolicy = fail_policy
        self._on_timeout: TimeoutPolicy = on_timeout
        self._gate = _RateGate(rate, burst) if rate is not None else None
        self._on_start = on_start
        self._on_complete = on_complete
        self._on_retry = on_retry

        self._counter = itertools.count()
        self._wids = itertools.count(1)
        self._worker_tasks: dict[int, asyncio.Task[None]] = {}
        self._runners: dict[str, tuple[asyncio.Task[None], _Task]] = {}
        self._timers: dict[str, tuple[asyncio.TimerHandle, _Task]] = {}
        self._dormant: list[_Task] = []
        self._results: list[TaskResult] = []
        self._outstanding = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._wake = asyncio.Event()
        self._accepting = True
        self._halt = False
        self._closed = False
        self._started = False
        self._running = False
        self._timed_out = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_tid: int | None = None
        self._bind_lock = threading.Lock()

    # -- introspection ------------------------------------------------------

    @property
    def active_count(self) -> int:
        """Tasks currently executing."""
        return len(self._runners)

    @property
    def pending_count(self) -> int:
        """Tasks waiting to execute (queued or scheduled)."""
        return self._outstanding - len(self._runners)

    @property
    def closed(self) -> bool:
        """``True`` after :meth:`shutdown` has completed."""
        return self._closed

    @property
    def results(self) -> tuple[TaskResult, ...]:
        """Snapshot of every :class:`~osiiso.TaskResult` accumulated so far."""
        return tuple(self._results)

    @property
    def stats(self) -> dict[str, int | bool]:
        """Snapshot of queue metrics: pending/active/scheduled/completed/workers/closed."""
        return {
            "pending": self.pending_count,
            "active": self.active_count,
            "scheduled": len(self._timers) + len(self._dormant),
            "completed": len(self._results),
            "workers": len(self._worker_tasks),
            "closed": self._closed,
        }

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> AsyncQueue:
        """Bind the running loop, arm pending timers, and spawn workers.

        Called automatically by :meth:`run` and ``__aenter__``.

        Raises:
            ~osiiso.ClosedError: If the queue has been shut down.
        """
        if self._closed:
            raise ClosedError("queue has been shut down")
        self._bind_loop()
        self._halt = False
        self._timed_out = False
        self._started = True
        if self._dormant:
            dormant, self._dormant = self._dormant, []
            for t in dormant:
                self._arm(t)
        self._spawn_workers()
        return self

    async def join(self) -> None:
        """Wait until every outstanding task has finished (starts the queue if needed)."""
        if not self._started:
            await self.start()
        await self._idle.wait()

    async def run(self, timeout: float | None = None, *, strict: bool = False, fail_policy: FailPolicy | None = None) -> RunSummary:
        """Execute all outstanding tasks and return a :class:`~osiiso.RunSummary`.

        Args:
            timeout: Time limit for this run (overrides the queue default).
            strict: Raise :class:`~osiiso.ExecutionError` if any task failed.
            fail_policy: Override the queue fail policy for this run only.

        Raises:
            RuntimeError: If ``run()`` is already in progress.
        """
        if self._running:
            raise RuntimeError("run() is already in progress")
        self._running = True
        run_start = time.perf_counter()
        idx = len(self._results)
        saved_fp = self._fail_policy
        if fail_policy is not None:
            self._fail_policy = fail_policy
        effective = timeout if timeout is not None else self._timeout

        try:
            await self.start()
            waiter = self._idle.wait() if self._mode == "finite" else self._wake.wait()
            try:
                await asyncio.wait_for(waiter, effective)
            except TimeoutError:
                self._timed_out = True
                self._abort(kill=self._on_timeout == "cancel")
                await self._idle.wait()
            if self._mode == "finite" or self._timed_out:
                await self._stop_workers()
        finally:
            window = [r for r in self._results[idx:] if not r.detached]
            summary = RunSummary.from_results(window, run_start=run_start, timed_out=self._timed_out)
            self._timed_out = False
            self._halt = False
            self._running = False
            self._fail_policy = saved_fp

        if strict:
            summary.raise_for_errors()
        return summary

    async def shutdown(self, *, force: bool = False) -> None:
        """Stop the queue.

        Args:
            force: ``True`` cancels everything (including ``must_complete``
                tasks) immediately; ``False`` (default) finishes all
                outstanding work first.
        """
        if self._closed:
            self._wake.set()
            return
        if force:
            self._accepting = False
            self._abort(kill=True)
        else:
            if self._outstanding and not self._started:
                await self.start()
            self._accepting = False
            await self._idle.wait()
            self._halt = True
        await self._stop_workers()
        self._closed = True
        self._wake.set()

    async def __aenter__(self) -> AsyncQueue:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.shutdown(force=exc_type is not None)

    def cancel(self) -> Any:
        """Force-shutdown the queue from any thread.

        Returns the scheduled task/future when a loop is running, else ``None``.
        """
        loop = self._loop
        if loop is not None and loop.is_running():
            if threading.get_ident() == self._loop_tid:
                return loop.create_task(self.shutdown(force=True))
            return asyncio.run_coroutine_threadsafe(self.shutdown(force=True), loop)
        # Never started: cancel synchronously.
        self._accepting = False
        self._abort(kill=True)
        self._closed = True
        return None

    def reset(self) -> None:
        """Restore a finished or shut-down queue for reuse.

        Cancels anything still pending and clears accumulated results.

        Raises:
            RuntimeError: If called during ``run()`` or while workers are alive.
        """
        if self._running:
            raise RuntimeError("cannot reset during run()")
        if self._worker_tasks:
            raise RuntimeError("cannot reset while workers are running — call shutdown() first")
        self._abort(kill=True)
        self._drain_sentinels()
        self._results.clear()
        self._outstanding = 0
        self._idle.set()
        self._wake = asyncio.Event()
        self._accepting = True
        self._halt = False
        self._closed = False
        self._started = False
        self._timed_out = False
        self._counter = itertools.count()
        self._wids = itertools.count(1)
        self._loop = None
        self._loop_tid = None

    def clear_results(self) -> None:
        """Discard accumulated results to free memory."""
        self._results.clear()

    # -- internals ----------------------------------------------------------

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        with self._bind_lock:
            if self._loop is not None and self._loop is not loop and (self._worker_tasks or self._runners):
                raise RuntimeError("cannot switch event loops while running")
            self._loop = loop
            self._loop_tid = threading.get_ident()

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> TaskHandle:
        if self._closed or not self._accepting or self._halt:
            raise ClosedError("queue is not accepting tasks")
        if self._size and self._outstanding >= self._size:
            raise QueueFullError(f"queue is full ({self._size} tasks outstanding)")
        if isawaitable(fn) and opts.retries:
            raise ValueError("a bare awaitable cannot be retried — submit a coroutine function instead")
        t = self._new_task(fn, args, opts)
        self._outstanding += 1
        self._idle.clear()
        if t.handle.scheduled_for is not None and t.handle.scheduled_for > time.perf_counter():
            self._arm(t)
        else:
            self._ready.put_nowait(t)
            if self._started:
                self._spawn_workers()
        return t.handle

    def _arm(self, t: _Task) -> None:
        if not self._started or self._loop is None:
            self._dormant.append(t)
            return
        delay = max(0.0, (t.handle.scheduled_for or 0.0) - time.perf_counter())
        timer = self._loop.call_later(delay, self._release, t)
        self._timers[t.handle.task_id] = (timer, t)

    def _release(self, t: _Task) -> None:
        self._timers.pop(t.handle.task_id, None)
        if t.handle.done():
            return
        self._ready.put_nowait(t)
        self._spawn_workers()

    def _target_workers(self) -> int:
        if self._workers is not None:
            return self._workers
        backlog = self._ready.qsize() + len(self._runners)
        floor = 1 if self._mode == "infinite" else 0
        return max(floor, min(self._auto_limit, backlog))

    def _spawn_workers(self) -> None:
        if self._closed or self._halt or not self._started or self._loop is None:
            return
        target = self._target_workers()
        while len(self._worker_tasks) < target:
            wid = next(self._wids)
            self._worker_tasks[wid] = self._loop.create_task(self._worker(wid), name=f"osiiso-worker-{wid}")

    async def _worker(self, wid: int) -> None:
        try:
            while True:
                t = await self._ready.get()
                if t.fn is None:
                    return
                h = t.handle
                if h.done():
                    continue
                if self._halt and not t.opts.must_complete:
                    self._record(h, make_result(h, "cancelled", message="cancelled during shutdown"))
                    continue
                runner = asyncio.create_task(self._attempts(t), name=f"osiiso-task-{h.task_id[:8]}")
                self._runners[h.task_id] = (runner, t)
                try:
                    await runner
                except asyncio.CancelledError:
                    if not runner.cancelled():
                        # The worker itself was cancelled from outside.
                        runner.cancel()
                        raise
                finally:
                    self._runners.pop(h.task_id, None)
        finally:
            self._worker_tasks.pop(wid, None)

    async def _attempts(self, t: _Task) -> None:
        h, o = t.handle, t.opts
        delay = o.retry_delay
        try:
            while True:
                if self._gate is not None:
                    pause = self._gate.reserve()
                    if pause > 0:
                        await asyncio.sleep(pause)
                h._mark_running()
                _emit(self._on_start, h)
                try:
                    value = await asyncio.wait_for(self._call(t), o.timeout)
                except Exception as exc:
                    if h.attempts <= o.retries and not (self._halt and not o.must_complete):
                        h._mark_retrying()
                        _emit(self._on_retry, h, exc)
                        if delay > 0:
                            await asyncio.sleep(delay)
                            delay *= o.backoff
                        continue
                    self._record(h, make_result(h, "failed", exception=exc, message=str(exc)))
                    return
                self._record(h, make_result(h, "succeeded", value=value))
                return
        except asyncio.CancelledError:
            self._record(h, make_result(h, "cancelled", message="cancelled"))
            raise

    @staticmethod
    async def _call(t: _Task) -> Any:
        fn = t.fn
        if isawaitable(fn):
            return await fn
        if iscoroutinefunction(fn):
            return await fn(*t.args)
        result = await asyncio.to_thread(fn, *t.args)
        if isawaitable(result):
            return await result
        return result

    def _record(self, h: TaskHandle, result: TaskResult) -> None:
        if not h._mark_finished(result):
            return
        self._results.append(result)
        self._outstanding -= 1
        if self._outstanding <= 0:
            self._idle.set()
        if result.status == "failed" and self._fail_policy == "fail_first" and not self._halt:
            self._abort(kill=False)
        _emit(self._on_complete, result)

    def _cancel_task(self, task_id: str) -> bool:
        loop = self._loop
        if loop is not None and loop.is_running() and threading.get_ident() != self._loop_tid:
            loop.call_soon_threadsafe(self._cancel_local, task_id)
            return True
        return self._cancel_local(task_id)

    def _cancel_local(self, task_id: str) -> bool:
        entry = self._runners.get(task_id)
        if entry is not None:
            entry[0].cancel()
            return True
        timed = self._timers.pop(task_id, None)
        if timed is not None:
            timer, t = timed
            timer.cancel()
            self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
            return True
        for t in self._dormant:
            if t.handle.task_id == task_id:
                self._dormant.remove(t)
                self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
                return True
        return self._drain_ready(kill=True, only={task_id}) > 0

    def _drain_ready(self, *, kill: bool, only: set[str] | None = None) -> int:
        kept: list[_Task] = []
        cancelled = 0
        while True:
            try:
                t = self._ready.get_nowait()
            except asyncio.QueueEmpty:
                break
            if t.fn is None:
                kept.append(t)
                continue
            if t.handle.done():
                continue
            selected = only is None or t.handle.task_id in only
            if selected and (kill or not t.opts.must_complete):
                self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
                cancelled += 1
            else:
                kept.append(t)
        for t in kept:
            self._ready.put_nowait(t)
        return cancelled

    def _abort(self, *, kill: bool) -> None:
        """Cancel scheduled, queued, and running tasks (sparing ``must_complete`` unless *kill*)."""
        self._halt = True
        for task_id, (timer, t) in list(self._timers.items()):
            if kill or not t.opts.must_complete:
                timer.cancel()
                self._timers.pop(task_id, None)
                self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
        for t in list(self._dormant):
            if kill or not t.opts.must_complete:
                self._dormant.remove(t)
                self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
        self._drain_ready(kill=kill)
        for runner, t in list(self._runners.values()):
            if kill or not t.opts.must_complete:
                runner.cancel()

    async def _stop_workers(self) -> None:
        if self._worker_tasks:
            workers = list(self._worker_tasks.values())
            for _ in workers:
                self._ready.put_nowait(_stop_task(next(self._counter)))
            await asyncio.gather(*workers, return_exceptions=True)
            self._worker_tasks.clear()
        self._drain_sentinels()
        self._started = False

    def _drain_sentinels(self) -> None:
        kept: list[_Task] = []
        while True:
            try:
                t = self._ready.get_nowait()
            except asyncio.QueueEmpty:
                break
            if t.fn is not None:
                kept.append(t)
        for t in kept:
            self._ready.put_nowait(t)
