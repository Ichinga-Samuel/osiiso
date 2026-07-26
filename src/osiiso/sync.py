"""Shared engine for the thread- and process-backed queues.

:class:`_SyncQueue` implements the whole control plane — submission,
scheduling, retries, rate limiting, cancellation, timeouts, and shutdown —
on top of worker threads.  Concrete queues only define how one attempt of
one task is executed (:meth:`_SyncQueue._execute`) and how to interrupt it.
"""

from __future__ import annotations

import heapq
import itertools
import queue as queue_mod
import threading
import time
from collections.abc import Callable
from typing import Any

from .core import FailPolicy, QueueMode, TimeoutPolicy, _check_queue_args, _emit, _RateGate, _stop_task, _SubmitPlane, _Task, logger
from .exceptions import ClosedError
from .group import SyncTaskGroup
from .handle import SyncTaskHandle
from .options import TaskOptions
from .result import RunSummary, TaskResult, make_result


class _Cancelled(Exception):
    """Internal cooperative cancellation signal."""


class _Ctl:
    """Per-active-task control block used to interrupt a running attempt."""

    __slots__ = ("cancel_ev", "wake", "slot")

    def __init__(self) -> None:
        """Create an unset control block; *wake* and *slot* are filled in by the executor."""
        self.cancel_ev = threading.Event()
        self.wake: threading.Event | None = None
        self.slot: Any = None

    def interrupt(self) -> None:
        """Flag cancellation and break the attempt out of whatever it is blocked on.

        Wakes a waiting thread executor and terminates a busy pool subprocess.
        """
        self.cancel_ev.set()
        wake = self.wake
        if wake is not None:
            wake.set()
        slot = self.slot
        if slot is not None:
            slot.terminate()


class _SyncQueue(_SubmitPlane):
    """Thread-coordinated task queue base (see :class:`ThreadQueue` / :class:`ProcessQueue`)."""

    _group_cls = SyncTaskGroup
    _handle_cls = SyncTaskHandle

    def __init__(
        self,
        *,
        workers: int | None,
        size: int,
        timeout: float | None,
        mode: QueueMode,
        fail_policy: FailPolicy,
        on_timeout: TimeoutPolicy,
        rate: float | None,
        burst: int,
        auto_limit: int,
        initializer: Callable[..., Any] | None,
        initargs: tuple[Any, ...],
        on_start: Callable[[SyncTaskHandle], Any] | None,
        on_complete: Callable[[TaskResult], Any] | None,
        on_retry: Callable[[SyncTaskHandle, BaseException], Any] | None,
    ) -> None:
        """Store the shared configuration; see :class:`ThreadQueue` / :class:`ProcessQueue` for the argument meanings.

        Args:
            auto_limit: Upper bound on auto-scaled workers, chosen by the subclass.
        """
        _check_queue_args(workers, size, timeout, rate, burst)
        self._ready: queue_mod.PriorityQueue[_Task] = queue_mod.PriorityQueue()
        self._workers = workers
        self._auto_limit = auto_limit
        self._size = size
        self._timeout = timeout
        self._mode: QueueMode = mode
        self._fail_policy: FailPolicy = fail_policy
        self._on_timeout: TimeoutPolicy = on_timeout
        self._gate = _RateGate(rate, burst) if rate is not None else None
        self._initializer = initializer
        self._initargs = initargs
        self._on_start = on_start
        self._on_complete = on_complete
        self._on_retry = on_retry
        self._counter = itertools.count()
        self._wids = itertools.count(1)
        self._lock = threading.RLock()
        self._done_cond = threading.Condition(self._lock)
        self._timer_cond = threading.Condition(self._lock)
        self._threads: dict[int, threading.Thread] = {}
        self._active: dict[str, tuple[_Ctl, _Task]] = {}
        self._later: list[tuple[float, int, _Task]] = []
        self._timer_thread: threading.Thread | None = None
        self._results: list[TaskResult] = []
        self._outstanding = 0
        self._shutdown_ev = threading.Event()
        self._accepting = True
        self._halt = False
        self._closed = False
        self._started = False
        self._running = False
        self._timed_out = False

    @property
    def active_count(self) -> int:
        """Tasks currently executing."""
        with self._lock:
            return len(self._active)

    @property
    def pending_count(self) -> int:
        """Tasks waiting to execute (queued or scheduled)."""
        with self._lock:
            return self._outstanding - len(self._active)

    @property
    def closed(self) -> bool:
        """``True`` after :meth:`shutdown` has completed."""
        with self._lock:
            return self._closed

    @property
    def results(self) -> tuple[TaskResult, ...]:
        """Snapshot of every :class:`~osiiso.TaskResult` accumulated so far."""
        with self._lock:
            return tuple(self._results)

    @property
    def stats(self) -> dict[str, int | bool]:
        """Snapshot of queue metrics: pending/active/scheduled/completed/workers/closed."""
        with self._lock:
            return {
                "pending": self._outstanding - len(self._active),
                "active": len(self._active),
                "scheduled": len(self._later),
                "completed": len(self._results),
                "workers": len(self._threads),
                "closed": self._closed,
            }

    def start(self):
        """Spawn workers and accept a new run.

        Called automatically by :meth:`run` and ``__enter__``.

        Returns:
            The queue itself, so ``q = ThreadQueue().start()`` reads naturally.

        Raises:
            ~osiiso.ClosedError: If the queue has been shut down.
        """
        with self._lock:
            if self._closed:
                raise ClosedError("queue has been shut down")
            self._halt = False
            self._timed_out = False
            self._started = True
        self._spawn_workers()
        return self

    def join(self) -> None:
        """Block until every outstanding task has finished (starts the queue if needed)."""
        if not self._started:
            self.start()
        self._await_idle(None)

    def run(self, timeout: float | None = None, *, strict: bool = False, fail_policy: FailPolicy | None = None) -> RunSummary:
        """Execute all outstanding tasks and return a :class:`~osiiso.RunSummary`.

        Args:
            timeout: Time limit for this run (overrides the queue default).
            strict: Raise :class:`~osiiso.ExecutionError` if any task failed.
            fail_policy: Override the queue fail policy for this run only.

        Returns:
            A summary of the tasks completed during this run (detached tasks excluded).

        Raises:
            RuntimeError: If ``run()`` is already in progress.
            ~osiiso.ExecutionError: If *strict* and any task failed.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("run() is already in progress")
            self._running = True
            idx = len(self._results)
        run_start = time.perf_counter()
        saved_fp = self._fail_policy
        if fail_policy is not None:
            self._fail_policy = fail_policy
        effective = timeout if timeout is not None else self._timeout
        try:
            self.start()
            if self._mode == "finite":
                finished = self._await_idle(effective)
            elif effective is None:
                self._shutdown_ev.wait()
                finished = True
            else:
                finished = self._shutdown_ev.wait(effective)
            if not finished:
                self._timed_out = True
                self._abort(kill=self._on_timeout == "cancel")
                self._await_idle(None)
            if self._mode == "finite" or self._timed_out:
                self._stop_workers()
        finally:
            with self._lock:
                window = [r for r in self._results[idx:] if not r.detached]
                timed_out = self._timed_out
                self._timed_out = False
                self._halt = False
                self._running = False
                self._fail_policy = saved_fp
            summary = RunSummary.from_results(window, run_start=run_start, timed_out=timed_out)

        if strict:
            summary.raise_for_errors()
        return summary

    def shutdown(self, *, force: bool = False) -> None:
        """Stop the queue.

        Args:
            force: ``True`` cancels everything (including ``must_complete``
                tasks) immediately; ``False`` (default) finishes all
                outstanding work first.
        """
        with self._lock:
            if self._closed:
                self._shutdown_ev.set()
                return
        if force:
            with self._lock:
                self._accepting = False
            self._abort(kill=True)
        else:
            with self._lock:
                need_start = self._outstanding > 0 and not self._started
            if need_start:
                self.start()
            with self._lock:
                self._accepting = False
            self._await_idle(None)
            with self._lock:
                self._halt = True
        self._stop_workers()
        with self._lock:
            self._closed = True
            self._timer_cond.notify_all()
            self._done_cond.notify_all()
        self._shutdown_ev.set()

    def __enter__(self):
        """Start the queue and return it."""
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Drain outstanding work on a clean exit, or force-cancel it if the block raised."""
        self.shutdown(force=exc_type is not None)

    def cancel(self) -> threading.Thread | None:
        """Force-shutdown the queue; safe to call from inside a task.

        Returns the background shutdown thread when called from a worker,
        ``None`` otherwise.
        """
        current = threading.current_thread()
        with self._lock:
            inside = any(w is current for w in self._threads.values())
        if inside:
            t = threading.Thread(target=self.shutdown, kwargs={"force": True}, daemon=True)
            t.start()
            return t
        self.shutdown(force=True)
        return None

    def reset(self) -> None:
        """Restore a finished or shut-down queue for reuse.

        Cancels anything still pending and clears accumulated results.

        Raises:
            RuntimeError: If called during ``run()`` or while workers are alive.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("cannot reset during run()")
            if self._threads:
                raise RuntimeError("cannot reset while workers are running — call shutdown() first")
        self._abort(kill=True)
        self._await_idle(None)
        self._drain_sentinels()
        with self._lock:
            self._results.clear()
            self._outstanding = 0
            self._shutdown_ev = threading.Event()
            self._accepting = True
            self._halt = False
            self._closed = False
            self._started = False
            self._timed_out = False
            self._counter = itertools.count()
            self._wids = itertools.count(1)
            self._done_cond.notify_all()

    def clear_results(self) -> None:
        """Discard accumulated results to free memory."""
        with self._lock:
            self._results.clear()

    def _validate_fn(self, fn: Any) -> None:
        """Reject unsupported callables at submission time (subclass hook).

        Raises:
            TypeError: If the subclass cannot run *fn*.
        """

    def _worker_ctx(self) -> Any:
        """Create per-worker state (e.g. a pooled process slot); a failure aborts the queue.

        Returns:
            The context object handed to every :meth:`_execute` call on this worker.
        """
        if self._initializer is not None:
            self._initializer(*self._initargs)
        return None

    def _close_worker_ctx(self, wctx: Any) -> None:
        """Release per-worker state when the worker exits."""

    def _execute(self, t: _Task, ctl: _Ctl, wctx: Any) -> Any:
        """Run one attempt of one task (subclass hook).

        Args:
            t: The task to execute.
            ctl: Control block to watch for cancellation and register interrupt targets on.
            wctx: This worker's context from :meth:`_worker_ctx`.

        Returns:
            The task's return value.

        Raises:
            _Cancelled: If cancellation was requested.
            TimeoutError: If ``opts.timeout`` elapsed.
            Exception: Whatever the task itself raised.
        """
        raise NotImplementedError

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> SyncTaskHandle:
        """Admit one task, then either schedule it for later or make it ready to run.

        On a bounded queue this blocks until the backlog drops below ``size``.

        Returns:
            The handle for the new task.

        Raises:
            ~osiiso.ClosedError: If the queue is not accepting tasks.
            TypeError: If the subclass cannot run *fn*.
        """
        self._validate_fn(fn)
        with self._lock:
            self._check_accepting()
            if self._size:
                while self._outstanding >= self._size:
                    self._done_cond.wait()
                    self._check_accepting()
            t = self._new_task(fn, args, opts)
            self._outstanding += 1
            target = t.handle.scheduled_for
            if target is not None and target > time.perf_counter():
                heapq.heappush(self._later, (target, t.seq, t))
                self._ensure_timer()
                self._timer_cond.notify_all()
            else:
                self._ready.put(t)
            need_spawn = self._started
        if need_spawn:
            self._spawn_workers()
        return t.handle

    def _check_accepting(self) -> None:
        """Guard submission (lock held).

        Raises:
            ~osiiso.ClosedError: If the queue is closed, draining, or halted.
        """
        if self._closed or not self._accepting or self._halt:
            raise ClosedError("queue is not accepting tasks")

    def _ensure_timer(self) -> None:
        """Start the scheduling thread if it is not already alive (lock held)."""
        th = self._timer_thread
        if th is None or not th.is_alive():
            self._timer_thread = threading.Thread(target=self._timer_loop, name="osiiso-timer", daemon=True)
            self._timer_thread.start()

    def _timer_loop(self) -> None:
        """Scheduling thread: move delayed tasks onto the ready queue as they come due."""
        with self._timer_cond:
            while not self._closed:
                if not self._later:
                    self._timer_cond.wait()
                    continue
                due, _, t = self._later[0]
                now = time.perf_counter()
                if due > now:
                    self._timer_cond.wait(due - now)
                    continue
                heapq.heappop(self._later)
                if not t.handle.done():
                    self._ready.put(t)
                    if self._started:
                        self._spawn_workers()

    def _target_workers(self) -> int:
        """How many workers should exist right now: the fixed count, or one per backlogged task."""
        if self._workers is not None:
            return self._workers
        backlog = self._ready.qsize() + len(self._active)
        floor = 1 if self._mode == "infinite" else 0
        return max(floor, min(self._auto_limit, backlog))

    def _spawn_workers(self) -> None:
        """Create worker threads until the target count is reached (no-op once halted)."""
        to_start: list[threading.Thread] = []
        with self._lock:
            if self._closed or self._halt or not self._started:
                return
            target = self._target_workers()
            while len(self._threads) < target:
                wid = next(self._wids)
                th = threading.Thread(target=self._worker, args=(wid,), name=f"osiiso-worker-{wid}", daemon=True)
                self._threads[wid] = th
                to_start.append(th)
        for th in to_start:
            th.start()

    def _worker(self, wid: int) -> None:
        """Pull ready tasks and run them until a stop sentinel arrives.

        A failing :meth:`_worker_ctx` aborts the whole queue.

        Args:
            wid: Worker id used for the thread name and the worker registry.
        """
        try:
            wctx = self._worker_ctx()
        except BaseException:
            logger.exception("worker initializer failed; aborting queue")
            with self._lock:
                self._threads.pop(wid, None)
            self._abort(kill=True)
            return
        try:
            while True:
                t = self._ready.get()
                if t.fn is None:
                    return
                h = t.handle
                if h.done():
                    continue
                if self._halt and not t.opts.must_complete:
                    self._record(h, make_result(h, "cancelled", message="cancelled during shutdown"))
                    continue
                self._attempts(t, wctx)
        finally:
            self._close_worker_ctx(wctx)
            with self._lock:
                self._threads.pop(wid, None)

    def _attempts(self, t: _Task, wctx: Any) -> None:
        """Run one task to a final result, applying rate limiting, timeout, and retries.

        Registers the task as active for the duration so it can be interrupted,
        and always records a result: succeeded, failed, or cancelled.

        Args:
            t: The task to run.
            wctx: This worker's context from :meth:`_worker_ctx`.
        """
        h, o = t.handle, t.opts
        ctl = _Ctl()
        with self._lock:
            self._active[h.task_id] = (ctl, t)
        delay = o.retry_delay
        try:
            while True:
                if ctl.cancel_ev.is_set():
                    raise _Cancelled
                if self._gate is not None:
                    pause = self._gate.reserve()
                    if pause > 0 and ctl.cancel_ev.wait(pause):
                        raise _Cancelled
                h._mark_running()
                _emit(self._on_start, h)
                try:
                    value = self._execute(t, ctl, wctx)
                except _Cancelled:
                    raise
                except Exception as exc:
                    retriable = not ctl.cancel_ev.is_set() and h.attempts <= o.retries and not (self._halt and not o.must_complete)
                    if retriable:
                        h._mark_retrying()
                        _emit(self._on_retry, h, exc)
                        if delay > 0:
                            if ctl.cancel_ev.wait(delay):
                                raise _Cancelled from None
                            delay *= o.backoff
                        continue
                    self._record(h, make_result(h, "failed", exception=exc, message=str(exc)))
                    return
                self._record(h, make_result(h, "succeeded", value=value))
                return
        except _Cancelled:
            self._record(h, make_result(h, "cancelled", message="cancelled"))
        finally:
            with self._lock:
                self._active.pop(h.task_id, None)

    def _record(self, h: SyncTaskHandle, result: TaskResult) -> None:
        """Publish a task's final result: store it, wake waiters, apply ``fail_first``, fire ``on_complete``.

        Ignores repeat calls for a task that already finished.
        """
        if not h._mark_finished(result):
            return
        fail_abort = False
        with self._lock:
            self._results.append(result)
            self._outstanding -= 1
            self._done_cond.notify_all()
            if result.status == "failed" and self._fail_policy == "fail_first" and not self._halt:
                self._halt = True
                fail_abort = True
        if fail_abort:
            self._abort_tasks(kill=False)
        _emit(self._on_complete, result)

    def _await_idle(self, timeout: float | None) -> bool:
        """Block until nothing is outstanding.

        Args:
            timeout: Seconds to wait, or ``None`` for no limit.

        Returns:
            ``True`` if the queue went idle, ``False`` if *timeout* elapsed first.
        """
        with self._done_cond:
            return self._done_cond.wait_for(lambda: self._outstanding == 0, timeout=timeout)

    def _cancel_task(self, task_id: str) -> bool:
        """Cancel one task wherever it currently sits: active, scheduled, or queued.

        Returns:
            ``True`` if the task was found and cancelled.
        """
        found: _Task | None = None
        with self._lock:
            entry = self._active.get(task_id)
            if entry is not None:
                entry[0].interrupt()
                return True
            for i, (_, _, t) in enumerate(self._later):
                if t.handle.task_id == task_id:
                    del self._later[i]
                    heapq.heapify(self._later)
                    found = t
                    break
        if found is not None:
            self._record(found.handle, make_result(found.handle, "cancelled", message="cancelled before execution"))
            return True
        return self._drain_ready(kill=True, only={task_id}) > 0

    def _drain_ready(self, *, kill: bool, only: set[str] | None = None) -> int:
        """Cancel queued tasks and put the spared ones back.

        Args:
            kill: Also cancel ``must_complete`` tasks.
            only: Restrict cancellation to these task ids (``None`` = every queued task).

        Returns:
            How many tasks were cancelled.
        """
        kept: list[_Task] = []
        dropped: list[_Task] = []
        while True:
            try:
                t = self._ready.get_nowait()
            except queue_mod.Empty:
                break
            if t.fn is None:
                kept.append(t)
                continue
            if t.handle.done():
                continue
            selected = only is None or t.handle.task_id in only
            if selected and (kill or not t.opts.must_complete):
                dropped.append(t)
            else:
                kept.append(t)
        for t in kept:
            self._ready.put(t)
        for t in dropped:
            self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
        return len(dropped)

    def _abort(self, *, kill: bool) -> None:
        """Halt the queue and cancel its tasks.

        Args:
            kill: Also cancel ``must_complete`` tasks, which are otherwise spared.
        """
        with self._lock:
            self._halt = True
        self._abort_tasks(kill=kill)

    def _abort_tasks(self, *, kill: bool) -> None:
        """Cancel scheduled, queued, and active tasks without touching the halt flag.

        Args:
            kill: Also cancel ``must_complete`` tasks, which are otherwise spared.
        """
        dropped: list[_Task] = []
        with self._lock:
            keep: list[tuple[float, int, _Task]] = []
            for due, seq, t in self._later:
                if kill or not t.opts.must_complete:
                    dropped.append(t)
                else:
                    keep.append((due, seq, t))
            if dropped:
                self._later[:] = keep
                heapq.heapify(self._later)
                self._timer_cond.notify_all()
        for t in dropped:
            self._record(t.handle, make_result(t.handle, "cancelled", message="cancelled before execution"))
        self._drain_ready(kill=kill)
        with self._lock:
            actives = list(self._active.values())
        for ctl, t in actives:
            if kill or not t.opts.must_complete:
                ctl.interrupt()

    def _stop_workers(self) -> None:
        """Send one stop sentinel per worker and join them all (never joining the calling thread)."""
        with self._lock:
            workers = list(self._threads.values())
        if workers:
            for _ in workers:
                self._ready.put(_stop_task(next(self._counter)))
            current = threading.current_thread()
            for w in workers:
                if w is not current:
                    w.join()
        self._drain_sentinels()
        with self._lock:
            self._threads.clear()
            self._started = False

    def _drain_sentinels(self) -> None:
        """Discard unconsumed stop sentinels so a restarted worker does not exit at once."""
        kept: list[_Task] = []
        while True:
            try:
                t = self._ready.get_nowait()
            except queue_mod.Empty:
                break
            if t.fn is not None:
                kept.append(t)
        for t in kept:
            self._ready.put(t)
