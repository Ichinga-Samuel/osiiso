"""SharedQueue — an :class:`~osiiso.AsyncQueue` that accepts submissions from any thread.

Execution still happens on the single event loop the queue is bound to (the
*shared loop*); only the submission plane changes.  ``submit()``, ``map()``,
and ``group()`` calls from foreign threads are marshaled onto the loop with
``call_soon_threadsafe`` and block until the loop admits the work, so
:class:`~osiiso.ClosedError` and :class:`~osiiso.QueueFullError` raise at the
call site exactly as they do on the loop thread.
"""

from __future__ import annotations

import concurrent.futures
import threading
from collections.abc import Callable, Iterable
from typing import Any

from .asyncqueue import AsyncQueue
from .exceptions import ClosedError
from .handle import TaskHandle
from .options import TaskOptions


class SharedQueue(AsyncQueue):
    """Asyncio task queue with a thread-safe submission plane.

    Accepts the same constructor arguments as :class:`~osiiso.AsyncQueue`.
    Typical use is a long-lived queue serving one event loop while plain
    threads produce work::

        q = SharedQueue(workers=4, mode="infinite")

        def producer():                      # runs on any thread
            for item in source:
                q.submit(work, item, retries=2)

        async def main():
            async with q:                    # binds the shared loop
                threading.Thread(target=producer).start()
                await q.run()                # serve until shutdown()

    A cross-thread submission blocks the caller for one loop round-trip and
    returns the fully registered :class:`~osiiso.TaskHandle`.  Producer
    threads can observe completion with ``handle.add_done_callback`` (or poll
    ``handle.done()``); ``await handle`` works from any coroutine, including
    ones running on other event loops.

    Note:
        Thread-safe submission is guaranteed once the queue has started and
        while its loop is running — hand the queue to producer threads only
        after ``start()`` / ``__aenter__``.  Before that, submission is
        single-threaded, exactly like :class:`~osiiso.AsyncQueue`.  ``run()``,
        ``join()``, ``shutdown()``, and ``reset()`` still belong to the owning
        loop; ``queue.cancel()`` and ``handle.cancel()`` are safe from any
        thread.  Avoid submitting from another *event loop's* thread — the
        round-trip would block that loop briefly.
    """

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> TaskHandle:
        return self._marshal(super()._enqueue, fn, args, opts)

    def map(self, fn: Any, iterable: Iterable[Any], *, opts: TaskOptions | None = None, **overrides: Any) -> list[TaskHandle]:
        """Like :meth:`AsyncQueue.map`, but safe from any thread (one loop round-trip for the whole batch)."""
        return self._marshal(super().map, fn, iterable, opts=opts, **overrides)

    def group(
        self,
        tasks: Any,
        iterable: Iterable[Any] | None = None,
        *,
        group_id: str | None = None,
        opts: TaskOptions | None = None,
        **overrides: Any,
    ) -> Any:
        """Like :meth:`AsyncQueue.group`, but safe from any thread (one loop round-trip for the whole batch)."""
        return self._marshal(super().group, tasks, iterable, group_id=group_id, opts=opts, **overrides)

    def _marshal(self, call: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Run *call* on the queue's loop thread and block the caller until it returns."""
        loop = self._loop
        if loop is None or not loop.is_running() or threading.get_ident() == self._loop_tid:
            return call(*args, **kwargs)
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()

        def _commit() -> None:
            try:
                fut.set_result(call(*args, **kwargs))
            except BaseException as exc:
                fut.set_exception(exc)

        try:
            loop.call_soon_threadsafe(_commit)
        except RuntimeError:
            raise ClosedError("queue loop is closed") from None
        while True:
            try:
                return fut.result(timeout=1.0)
            except concurrent.futures.TimeoutError:
                if fut.done():
                    raise
                if loop.is_closed():
                    raise ClosedError("queue loop closed before the submission was processed") from None
