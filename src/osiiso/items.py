"""Internal task items — wrap callables for queue execution.

Each item subclass couples a callable with its resolved :class:`~osiiso.TaskOptions`
and handles the differences between async, thread, and process execution:

* :class:`AsyncItem` — coroutines or sync callables run in asyncio.
* :class:`ThreadItem` — sync callables run in a thread pool.
* :class:`ProcessItem` — callables (including coroutine functions) run in a
  subprocess.

These classes are **internal** and not part of the public API.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from typing import Any

from .options import TaskOptions


def _resolve_name(fn: Any) -> str:
    """Extract a human-readable name from a callable or awaitable.

    Args:
        fn: Any callable or awaitable object.

    Returns:
        The ``__name__`` attribute of *fn* if available, the coroutine's
        code object name for awaitables, or the type name as a fallback.
    """
    if inspect.isawaitable(fn):
        code = getattr(fn, "cr_code", None)
        if code is not None:
            return code.co_name
        return type(fn).__name__
    return getattr(fn, "__name__", None) or type(fn).__name__


def _resolve_schedule(opts: TaskOptions) -> float | None:
    """Convert *delay* / *run_at* into an absolute ``perf_counter`` target.

    Args:
        opts: Resolved :class:`~osiiso.TaskOptions` for the task.

    Returns:
        An absolute ``perf_counter`` timestamp, or ``None`` for immediate
        execution.
    """
    if opts.delay is not None:
        return time.perf_counter() + opts.delay
    if opts.run_at is not None:
        return time.perf_counter() + max(0.0, opts.run_at - time.time())
    return None


class _Item:
    """Internal wrapper pairing a callable with its resolved options.

    Subclasses specialise ``__call__`` for async, thread, or process execution.
    Not part of the public API.

    Attributes:
        fn: The callable or awaitable to execute.
        args: Positional arguments to pass to *fn*.
        opts: Resolved :class:`~osiiso.TaskOptions` for this task.
        task_id: UUID hex string that uniquely identifies this task.
        name: Human-readable task name.
        created_at: ``perf_counter`` timestamp of item creation.
        scheduled_for: Absolute ``perf_counter`` target start time, or ``None``.
    """

    __slots__ = ("fn", "args", "task_id", "name", "created_at", "scheduled_for", "opts")

    def __init__(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> None:
        """Initialise the item.

        Args:
            fn: Callable or awaitable to execute.
            args: Positional arguments forwarded to *fn* on execution.
            opts: Resolved options that control scheduling, retries, etc.
        """
        self.fn = fn
        self.args = args
        self.opts = opts
        self.task_id = uuid.uuid4().hex
        self.name = opts.name or _resolve_name(fn)
        self.created_at = time.perf_counter()
        self.scheduled_for = _resolve_schedule(opts)


class AsyncItem(_Item):
    """Queue item for :class:`~osiiso.AsyncQueue`.

    Coroutines and awaitables are awaited directly; plain sync callables are
    offloaded to a thread via :func:`asyncio.to_thread`.
    """

    async def __call__(self) -> Any:
        """Execute the wrapped callable and return its result."""
        fn, args = self.fn, self.args
        if inspect.isawaitable(fn):
            return await fn
        if inspect.iscoroutinefunction(fn):
            return await fn(*args)
        result = await asyncio.to_thread(fn, *args)
        if inspect.isawaitable(result):
            return await result
        return result


class ThreadItem(_Item):
    """Queue item for :class:`~osiiso.ThreadQueue`.

    Only sync callables are accepted; passing a coroutine or coroutine
    function raises :exc:`TypeError` at construction time.
    """

    def __init__(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> None:
        """Initialise the item, rejecting coroutines.

        Args:
            fn: A sync callable.  Awaitables and coroutine functions are
                rejected.
            args: Positional arguments forwarded to *fn*.
            opts: Resolved task options.

        Raises:
            TypeError: If *fn* is an awaitable or a coroutine function.
        """
        if inspect.isawaitable(fn):
            raise TypeError("thread tasks must be callable, not awaitable — use AsyncQueue for async work")
        if inspect.iscoroutinefunction(fn):
            raise TypeError("thread tasks must be sync callable, not coroutine function — use AsyncQueue for async work")
        super().__init__(fn, args, opts)

    def __call__(self) -> Any:
        """Execute the wrapped sync callable and return its result."""
        return self.fn(*self.args)


class ProcessItem(_Item):
    """Queue item for :class:`~osiiso.ProcessQueue`.

    Runs the callable in a subprocess.  Coroutine functions are supported
    (they are executed with :func:`asyncio.run` in the subprocess), but
    bare awaitables are rejected at construction time.
    """

    def __init__(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> None:
        """Initialise the item, rejecting bare awaitables.

        Args:
            fn: A callable (sync or coroutine function).  Bare awaitables are
                rejected.
            args: Positional arguments forwarded to *fn*.
            opts: Resolved task options.

        Raises:
            TypeError: If *fn* is a bare awaitable object.
        """
        if inspect.isawaitable(fn):
            raise TypeError("process tasks must be callable, not awaitable")
        super().__init__(fn, args, opts)

    def __call__(self) -> Any:
        """Execute the callable in the current process (subprocess context) and return its result."""
        fn, args = self.fn, self.args
        if inspect.iscoroutinefunction(fn):
            return asyncio.run(fn(*args))
        result = fn(*args)
        if inspect.isawaitable(result):
            return asyncio.run(result)
        return result
