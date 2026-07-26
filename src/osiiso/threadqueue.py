"""ThreadQueue — thread-based task queue for blocking I/O-bound work."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from inspect import isawaitable, iscoroutinefunction
from typing import Any

from .core import FailPolicy, QueueMode, TimeoutPolicy, _Task
from .handle import SyncTaskHandle
from .result import TaskResult
from .sync import _Cancelled, _Ctl, _SyncQueue


class ThreadQueue(_SyncQueue):
    """Thread-based task queue with priorities, retries, and graceful shutdown.

    Use as a context manager: on ``__exit__`` the queue drains remaining
    tasks before stopping workers, or cancels everything if the block raised.

    Each attempt runs on a sidecar thread the worker waits on, so per-task
    timeouts and cancellation stay responsive even for code that never
    yields.  A cancelled or timed-out attempt is *abandoned* — the sidecar
    keeps running to completion in the background, but its outcome is
    discarded.

    Args:
        workers: Fixed number of worker threads.  ``None`` (default) scales
            with the backlog up to ``min(32, cpu_count * 4)``.
        size: Maximum outstanding (queued + scheduled + running) tasks.
            ``0`` = unbounded.  When full, ``submit`` blocks until space frees.
        timeout: Default per-run time limit in seconds for :meth:`run`.
        mode: ``"finite"`` (default) — ``run()`` returns once all tasks finish;
            ``"infinite"`` — ``run()`` blocks until :meth:`shutdown`.
        fail_policy: ``"continue"`` (default) collects all failures;
            ``"fail_first"`` aborts remaining tasks after the first failure
            (``must_complete`` tasks are spared).
        on_timeout: When a run times out — ``"complete"`` (default) lets
            ``must_complete`` tasks finish; ``"cancel"`` stops everything.
        rate: Maximum task attempts per second across all workers.
        burst: With *rate*, how many attempts may start back-to-back after idle.
        initializer / initargs: Callable run once in each worker thread before
            it processes tasks; a failure aborts the queue.
        on_start: ``fn(handle)`` called as each attempt begins.
        on_complete: ``fn(result)`` called as each task finishes.
        on_retry: ``fn(handle, exception)`` called before each retry.

    Example::

        with ThreadQueue(workers=4) as q:
            q.map(process, items, retries=2)
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
        on_timeout: TimeoutPolicy = "complete",
        rate: float | None = None,
        burst: int = 1,
        initializer: Callable[..., Any] | None = None,
        initargs: tuple[Any, ...] = (),
        on_start: Callable[[SyncTaskHandle], Any] | None = None,
        on_complete: Callable[[TaskResult], Any] | None = None,
        on_retry: Callable[[SyncTaskHandle, BaseException], Any] | None = None,
    ) -> None:
        """Configure the queue; see the class docstring for what each argument means."""
        super().__init__(
            workers=workers,
            size=size,
            timeout=timeout,
            mode=mode,
            fail_policy=fail_policy,
            on_timeout=on_timeout,
            rate=rate,
            burst=burst,
            auto_limit=max(4, min(32, (os.cpu_count() or 1) * 4)),
            initializer=initializer,
            initargs=initargs,
            on_start=on_start,
            on_complete=on_complete,
            on_retry=on_retry,
        )

    def _validate_fn(self, fn: Any) -> None:
        """Reject async work — thread tasks must be plain sync callables.

        Raises:
            TypeError: If *fn* is an awaitable or a coroutine function.
        """
        if isawaitable(fn):
            raise TypeError("thread tasks must be callable, not awaitable — use AsyncQueue for async work")
        if iscoroutinefunction(fn):
            raise TypeError("thread tasks must be sync callables, not coroutine functions — use AsyncQueue for async work")

    def _execute(self, t: _Task, ctl: _Ctl, wctx: Any) -> Any:
        """Run one attempt on a sidecar thread and wait on it, staying responsive to timeout and cancellation.

        An abandoned sidecar keeps running in the background; its outcome is discarded.

        Returns:
            The task's return value.

        Raises:
            _Cancelled: If cancellation was requested.
            TimeoutError: If ``opts.timeout`` elapsed.
            RuntimeError: If the sidecar thread died without producing an outcome.
            Exception: Whatever the task itself raised.
        """
        wake = threading.Event()
        ctl.wake = wake
        if ctl.cancel_ev.is_set():
            raise _Cancelled
        box: list[tuple[str, Any]] = []

        def sidecar() -> None:
            """Run the task, box its value or exception, then wake the worker."""
            try:
                box.append(("v", t.fn(*t.args)))
            except BaseException as exc:
                box.append(("e", exc))
            finally:
                wake.set()

        sidecar_thread = threading.Thread(target=sidecar, name=f"osiiso-exec-{t.handle.task_id[:8]}", daemon=True)
        sidecar_thread.start()
        deadline = None if t.opts.timeout is None else time.perf_counter() + t.opts.timeout
        while True:
            if box:
                kind, payload = box[0]
                if kind == "e":
                    raise payload
                return payload
            if ctl.cancel_ev.is_set():
                raise _Cancelled
            if not sidecar_thread.is_alive():
                raise RuntimeError(f"task thread for {t.handle.name!r} died without a result")
            remaining = None if deadline is None else deadline - time.perf_counter()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"task {t.handle.name!r} exceeded {t.opts.timeout}s timeout")
            wake.wait(remaining)
