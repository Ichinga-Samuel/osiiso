"""ProcessQueue — process-backed task queue for CPU-bound work.

Each worker owns a **persistent** subprocess: tasks are shipped over a pipe
and results shipped back, so spawn cost is paid once per worker instead of
once per task.  Timeouts and cancellation terminate the subprocess (it is
respawned for the next task), which makes them effective even against
tight native loops.

Standard :mod:`multiprocessing` spawn semantics apply: on spawn platforms
(Windows, macOS) a script that uses :class:`ProcessQueue` must guard its
entry point with ``if __name__ == "__main__":``.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Callable
from inspect import isawaitable, iscoroutinefunction
from multiprocessing import connection as mp_conn
from multiprocessing.context import BaseContext
from typing import Any

from .core import FailPolicy, QueueMode, TimeoutPolicy, _Task
from .handle import SyncTaskHandle
from .result import TaskResult
from .sync import _Cancelled, _Ctl, _SyncQueue


def _run_callable(fn: Any, args: tuple[Any, ...]) -> Any:
    """Call *fn* inside the subprocess, driving coroutines with :func:`asyncio.run`.

    Returns:
        The task's return value, resolved if it was awaitable.
    """
    if iscoroutinefunction(fn):
        return asyncio.run(fn(*args))
    result = fn(*args)
    if isawaitable(result):
        return asyncio.run(_await(result))
    return result


async def _await(awaitable: Any) -> Any:
    """Await *awaitable* so it can be driven by :func:`asyncio.run`."""
    return await awaitable


def _send_safe(conn: Any, payload: tuple[str, Any]) -> None:
    """Send *payload* over *conn*, degrading to an error frame when it will not pickle."""
    try:
        conn.send(payload)
    except Exception as exc:
        try:
            conn.send(("e", RuntimeError(f"task result could not be pickled: {exc}")))
        except Exception:
            pass


def _pool_main(conn: Any, initializer: Any, initargs: tuple[Any, ...]) -> None:
    """Entry point of a pool subprocess: run tasks from the pipe until told to stop.

    Replies with ``("v", value)`` or ``("e", exception)`` per task, and exits on
    a ``None`` message or a closed pipe.

    Args:
        conn: The child end of the pipe to the coordinating worker thread.
        initializer: Callable run once before serving tasks; a failure is reported and ends the process.
        initargs: Positional arguments for *initializer*.
    """
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (ValueError, OSError):
        pass
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException as exc:
            _send_safe(conn, ("e", exc))
            conn.close()
            return
    while True:
        try:
            msg = conn.recv()
        except (EOFError, OSError):
            return
        except BaseException as exc:
            # The message frame was consumed but its payload would not unpickle
            # (e.g. the callable lives in __main__). Report and keep serving.
            _send_safe(conn, ("e", RuntimeError(f"could not unpickle task in worker (is the callable importable?): {exc!r}")))
            continue
        if msg is None:
            break
        fn, args = msg
        try:
            value = _run_callable(fn, args)
        except BaseException as exc:
            _send_safe(conn, ("e", exc))
        else:
            _send_safe(conn, ("v", value))
    conn.close()


class _Slot:
    """One worker's persistent subprocess and its pipe."""

    __slots__ = ("_ctx", "_init", "_initargs", "proc", "conn", "lock")

    def __init__(self, ctx: BaseContext, initializer: Any, initargs: tuple[Any, ...]) -> None:
        """Record how to spawn the subprocess; nothing starts until :meth:`ensure`.

        Args:
            ctx: Multiprocessing context supplying the start method.
            initializer: Callable run once inside the subprocess.
            initargs: Positional arguments for *initializer*.
        """
        self._ctx = ctx
        self._init = initializer
        self._initargs = initargs
        self.proc: Any = None
        self.conn: Any = None
        self.lock = threading.Lock()

    def ensure(self) -> None:
        """Spawn (or respawn) the subprocess if it is not alive."""
        with self.lock:
            if self.proc is not None and self.proc.is_alive():
                return
            self._close_locked(graceful=False)
            parent, child = self._ctx.Pipe()
            proc = self._ctx.Process(
                target=_pool_main, args=(child, self._init, self._initargs), name="osiiso-pool-worker", daemon=True
            )
            proc.start()
            child.close()
            self.proc, self.conn = proc, parent

    def terminate(self) -> None:
        """Kill the subprocess (used by cancellation); safe from any thread."""
        with self.lock:
            p = self.proc
            if p is not None and p.is_alive():
                try:
                    p.terminate()
                except (OSError, ValueError):
                    pass

    def reap(self) -> int | None:
        """Collect a dead or killed subprocess, escalating to ``kill`` if it lingers.

        Returns:
            Its exit code, or ``None`` if the slot had no process.
        """
        with self.lock:
            p = self.proc
            if p is None:
                return None
            p.join(timeout=2.0)
            if p.is_alive():
                p.kill()
                p.join(timeout=2.0)
            code = p.exitcode
            self._close_locked(graceful=False)
            return code

    def close(self) -> None:
        """Shut the subprocess down gracefully (send stop, then escalate)."""
        with self.lock:
            self._close_locked(graceful=True)

    def _close_locked(self, *, graceful: bool) -> None:
        """Drop the pipe and stop the process, escalating join → terminate → kill (lock held).

        Args:
            graceful: Ask the subprocess to exit first and allow it longer to do so.
        """
        p, c = self.proc, self.conn
        self.proc = self.conn = None
        if c is not None:
            if graceful and p is not None and p.is_alive():
                try:
                    c.send(None)
                except Exception:
                    pass
            try:
                c.close()
            except Exception:
                pass
        if p is not None:
            p.join(timeout=2.0 if graceful else 0.5)
            if p.is_alive():
                p.terminate()
                p.join(timeout=1.0)
            if p.is_alive():
                p.kill()
                p.join(timeout=1.0)


class ProcessQueue(_SyncQueue):
    """Process-backed task queue for CPU-bound work.

    Use as a context manager: on ``__exit__`` the queue drains remaining
    tasks before stopping workers, or terminates everything if the block
    raised.

    Tasks and their arguments must be picklable (standard spawn rules; guard
    script entry points with ``if __name__ == "__main__":``).  Coroutine
    functions are supported (executed with :func:`asyncio.run` in the
    subprocess).  Pool subprocesses are daemonic, so task code must not
    spawn multiprocessing children of its own.

    Args:
        workers: Fixed number of worker subprocesses.  ``None`` (default)
            scales with the backlog up to ``min(32, cpu_count)``.
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
        context: Optional multiprocessing context controlling the start method
            (``"spawn"``, ``"fork"``, ``"forkserver"``).
        initializer / initargs: Picklable callable run once inside each
            subprocess before it accepts tasks; a failure is reported as the
            failure of the task that triggered the spawn.
        on_start: ``fn(handle)`` called as each attempt begins.
        on_complete: ``fn(result)`` called as each task finishes.
        on_retry: ``fn(handle, exception)`` called before each retry.

    Example::

        with ProcessQueue(workers=4) as q:
            q.map(crunch, datasets, timeout=60)
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
        context: BaseContext | None = None,
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
            auto_limit=max(1, min(32, os.cpu_count() or 1)),
            initializer=initializer,
            initargs=initargs,
            on_start=on_start,
            on_complete=on_complete,
            on_retry=on_retry,
        )
        self._ctx = context or multiprocessing.get_context()

    def _validate_fn(self, fn: Any) -> None:
        """Reject bare awaitables — process tasks must be picklable callables.

        Raises:
            TypeError: If *fn* is an awaitable.
        """
        if isawaitable(fn):
            raise TypeError("process tasks must be callable, not awaitable")

    def _worker_ctx(self) -> _Slot:
        """Give the worker its own subprocess slot (spawned lazily on first use)."""
        # The initializer runs inside the subprocess, not the coordinator thread.
        return _Slot(self._ctx, self._initializer, self._initargs)

    def _close_worker_ctx(self, wctx: Any) -> None:
        """Shut down the worker's subprocess when the worker exits."""
        if wctx is not None:
            wctx.close()

    def _execute(self, t: _Task, ctl: _Ctl, wctx: Any) -> Any:
        """Ship one attempt to the worker's subprocess and wait for its reply.

        Cancellation and timeout both terminate the subprocess, which is
        respawned for the next task.

        Returns:
            The task's return value.

        Raises:
            _Cancelled: If cancellation was requested.
            TimeoutError: If ``opts.timeout`` elapsed.
            RuntimeError: If the subprocess died or its reply could not be read.
            Exception: Whatever the task itself raised.
        """
        slot: _Slot = wctx
        if ctl.cancel_ev.is_set():
            raise _Cancelled
        slot.ensure()
        ctl.slot = slot
        if ctl.cancel_ev.is_set():
            slot.terminate()
            slot.reap()
            raise _Cancelled
        conn, proc = slot.conn, slot.proc
        conn.send((t.fn, t.args))
        deadline = None if t.opts.timeout is None else time.perf_counter() + t.opts.timeout
        while True:
            remaining = None if deadline is None else max(0.0, deadline - time.perf_counter())
            ready = mp_conn.wait([conn, proc.sentinel], timeout=remaining)
            if conn in ready:
                try:
                    kind, payload = conn.recv()
                except Exception:
                    code = slot.reap()
                    if ctl.cancel_ev.is_set():
                        raise _Cancelled from None
                    raise RuntimeError(f"worker process died (exit code {code})") from None
                if kind == "e":
                    raise payload
                return payload
            if proc.sentinel in ready:
                code = slot.reap()
                if ctl.cancel_ev.is_set():
                    raise _Cancelled
                raise RuntimeError(f"worker process died (exit code {code})")
            # Neither fired: the per-task deadline expired.
            slot.terminate()
            slot.reap()
            raise TimeoutError(f"task {t.handle.name!r} exceeded {t.opts.timeout}s timeout")
