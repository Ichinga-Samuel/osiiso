"""Shared internals: task records, rate limiting, and the submission plane.

Everything here is private.  The three queues compose these pieces so that
submission, option resolution, and fan-out behave identically everywhere.
"""

from __future__ import annotations

import itertools
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from functools import partial
from inspect import isawaitable
from logging import getLogger
from typing import Any, Literal

from .options import TaskOptions, resolve_opts

logger = getLogger("osiiso")

FailPolicy = Literal["continue", "fail_first"]
QueueMode = Literal["finite", "infinite"]
TimeoutPolicy = Literal["cancel", "complete"]


def _callable_name(fn: Any) -> str:
    """Best-effort human-readable name for a callable or awaitable."""
    if isawaitable(fn):
        code = getattr(fn, "cr_code", None)
        return code.co_name if code is not None else type(fn).__name__
    return getattr(fn, "__name__", None) or type(fn).__name__


def _schedule_target(opts: TaskOptions) -> float | None:
    """Convert ``delay``/``run_at`` into an absolute ``perf_counter`` target."""
    if opts.delay is not None:
        return time.perf_counter() + opts.delay
    if opts.run_at is not None:
        return time.perf_counter() + max(0.0, opts.run_at - time.time())
    return None


def _emit(cb: Callable[..., Any] | None, *args: Any) -> None:
    """Invoke a user callback, logging (never propagating) exceptions."""
    if cb is None:
        return
    try:
        cb(*args)
    except Exception:
        logger.exception("callback %r raised", getattr(cb, "__name__", cb))


@dataclass(order=True, slots=True)
class _Task:
    """A queued unit of work, ordered by ``(priority, seq)``.

    A ``fn`` of ``None`` marks a worker-stop sentinel; sentinels sort last so
    real work drains first.
    """

    priority: float
    seq: int
    fn: Any = field(compare=False, default=None)
    args: tuple[Any, ...] = field(compare=False, default=())
    opts: TaskOptions = field(compare=False, default=TaskOptions())
    handle: Any = field(compare=False, default=None)


def _stop_task(seq: int) -> _Task:
    return _Task(priority=float("inf"), seq=seq)


class _RateGate:
    """Thread-safe GCRA rate limiter.

    ``reserve()`` books the next execution slot and returns how many seconds
    the caller must sleep before proceeding (0.0 when a token is available).
    The first *burst* calls after an idle period pass immediately.
    """

    __slots__ = ("_interval", "_tau", "_tat", "_lock")

    def __init__(self, rate: float, burst: int = 1) -> None:
        self._interval = 1.0 / rate
        self._tau = (burst - 1) * self._interval
        self._tat = 0.0
        self._lock = threading.Lock()

    def reserve(self) -> float:
        with self._lock:
            now = time.perf_counter()
            tat = max(self._tat, now)
            self._tat = tat + self._interval
            return max(0.0, tat - self._tau - now)


def _check_queue_args(workers: int | None, size: int, timeout: float | None, rate: float | None, burst: int) -> None:
    if size < 0:
        raise ValueError("size must be >= 0")
    if workers is not None and workers <= 0:
        raise ValueError("workers must be > 0")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be > 0")
    if rate is not None and rate <= 0:
        raise ValueError("rate must be > 0")
    if burst < 1:
        raise ValueError("burst must be >= 1")


def _fan(fn: Any, entry: Any) -> tuple[Any, tuple[Any, ...]]:
    """Interpret one map/group element: Mapping → kwargs, tuple → args, else single arg."""
    if isinstance(entry, Mapping):
        return partial(fn, **entry), ()
    if isinstance(entry, tuple):
        return fn, entry
    return fn, (entry,)


class _SubmitPlane:
    """Submission API shared by all queues.

    Concrete queues provide ``_enqueue(fn, args, opts) -> handle``, a
    ``_counter`` (itertools.count), and a ``_group_cls`` class attribute.
    """

    _group_cls: type
    _handle_cls: type
    _counter: itertools.count

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> Any:
        raise NotImplementedError

    def _new_task(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> _Task:
        handle = self._handle_cls(
            task_id=uuid.uuid4().hex,
            name=opts.name or _callable_name(fn),
            priority=opts.priority,
            must_complete=opts.must_complete,
            created_at=time.perf_counter(),
            cancel_fn=self._cancel_task,
            group_id=opts.group_id,
            detached=opts.detached,
            scheduled_for=_schedule_target(opts),
            metadata=opts.metadata,
        )
        return _Task(priority=opts.priority, seq=next(self._counter), fn=fn, args=args, opts=opts, handle=handle)

    def _cancel_task(self, task_id: str) -> bool:
        raise NotImplementedError

    def submit(self, fn: Any, /, *args: Any, opts: TaskOptions | None = None, **overrides: Any) -> Any:
        """Submit one task and return its handle.

        Positional *args* are forwarded to *fn*; use :func:`functools.partial`
        for keyword arguments.  Option fields may be passed inline
        (``retries=3``) or via *opts*; inline overrides win.

        Raises:
            ~osiiso.ClosedError: If the queue is not accepting tasks.
            ~osiiso.QueueFullError: Bounded :class:`~osiiso.AsyncQueue` only.
        """
        return self._enqueue(fn, args, resolve_opts(opts, overrides))

    def map(self, fn: Any, iterable: Iterable[Any], *, opts: TaskOptions | None = None, **overrides: Any) -> list[Any]:
        """Submit *fn* once per element and return the handles.

        Elements: ``tuple`` → positional args, ``Mapping`` → keyword args,
        anything else → a single positional arg.
        """
        eff = resolve_opts(opts, overrides)
        return [self._enqueue(f, a, eff) for f, a in (_fan(fn, e) for e in iterable)]

    def group(
        self,
        tasks: Any,
        iterable: Iterable[Any] | None = None,
        *,
        group_id: str | None = None,
        opts: TaskOptions | None = None,
        **overrides: Any,
    ) -> Any:
        """Submit a batch and return a group handle.

        Two forms: ``group([(fn, *args), ...])`` for heterogeneous work, or
        ``group(fn, iterable)`` to map one callable over many inputs.

        Example::

            grp = q.group([(fetch, url), (parse, raw), (save, record)])
            summary = await grp.wait()   # or grp.wait() on sync queues
        """
        eff = resolve_opts(opts, overrides)
        gid = group_id or eff.group_id or f"group-{next(self._counter)}"
        if eff.group_id != gid:
            eff = eff.replace(group_id=gid)
        if iterable is not None:
            handles = [self._enqueue(f, a, eff) for f, a in (_fan(tasks, e) for e in iterable)]
        else:
            handles = []
            for entry in tasks:
                if not isinstance(entry, tuple) or not entry:
                    raise TypeError("each group entry must be a tuple of (callable, *args)")
                handles.append(self._enqueue(entry[0], entry[1:], eff))
        return self._group_cls(gid, handles)

    def task(self, opts: TaskOptions | None = None, **overrides: Any) -> Callable[[Any], BoundTask]:
        """Decorator binding a function to this queue.

        Example::

            @q.task(retries=3, timeout=10)
            async def fetch(url): ...

            handle = fetch("https://example.com")
            handles = fetch.map(["u1", "u2"])
        """
        eff = resolve_opts(opts, overrides)

        def decorator(fn: Any) -> BoundTask:
            return BoundTask(fn, self, eff)

        return decorator


class BoundTask:
    """A callable bound to a queue by the ``@q.task()`` decorator."""

    __slots__ = ("_fn", "_q", "_opts")

    def __init__(self, fn: Any, q: _SubmitPlane, opts: TaskOptions) -> None:
        self._fn = fn
        self._q = q
        self._opts = opts

    def __call__(self, *args: Any, **overrides: Any) -> Any:
        eff = resolve_opts(self._opts, overrides) if overrides else self._opts
        return self._q._enqueue(self._fn, args, eff)

    def map(self, iterable: Iterable[Any], **overrides: Any) -> list[Any]:
        return self._q.map(self._fn, iterable, opts=self._opts, **overrides)

    def group(self, iterable: Iterable[Any], **overrides: Any) -> Any:
        return self._q.group(self._fn, iterable, opts=self._opts, **overrides)

    @property
    def __name__(self) -> str:
        return _callable_name(self._fn)

    def __repr__(self) -> str:
        return f"BoundTask({self.__name__!r})"
