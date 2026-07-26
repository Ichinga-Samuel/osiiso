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
from typing import TYPE_CHECKING, Any, Literal

from .checkpoint import canonical_key
from .options import TaskOptions, resolve_opts
from .result import make_result

if TYPE_CHECKING:
    from .checkpoint import Checkpoint

logger = getLogger("osiiso")

FailPolicy = Literal["continue", "fail_first"]
QueueMode = Literal["finite", "infinite"]
TimeoutPolicy = Literal["cancel", "complete"]


def _callable_name(fn: Any) -> str:
    """Best-effort human-readable name for a callable or awaitable.

    Args:
        fn: A callable, coroutine, or any other awaitable.

    Returns:
        Its ``__name__`` when available, else the coroutine's code name, else its type name.
    """
    if isawaitable(fn):
        code = getattr(fn, "cr_code", None)
        return code.co_name if code is not None else type(fn).__name__
    return getattr(fn, "__name__", None) or type(fn).__name__


def _schedule_target(opts: TaskOptions) -> float | None:
    """Convert ``delay``/``run_at`` into an absolute ``perf_counter`` target.

    Args:
        opts: Options carrying at most one of ``delay`` / ``run_at``.

    Returns:
        The monotonic start time, or ``None`` when the task is not scheduled.
    """
    if opts.delay is not None:
        return time.perf_counter() + opts.delay
    if opts.run_at is not None:
        return time.perf_counter() + max(0.0, opts.run_at - time.time())
    return None


def _emit(cb: Callable[..., Any] | None, *args: Any) -> None:
    """Invoke a user callback, logging (never propagating) exceptions.

    Args:
        cb: The callback to call, or ``None`` to do nothing.
        *args: Positional arguments forwarded to *cb*.
    """
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
    """Build a worker-stop sentinel that sorts after every real task.

    Args:
        seq: Tie-breaking sequence number from the queue's counter.

    Returns:
        A :class:`_Task` whose ``fn`` is ``None``.
    """
    return _Task(priority=float("inf"), seq=seq)


class _RateGate:
    """Thread-safe GCRA rate limiter.

    ``reserve()`` books the next execution slot and returns how many seconds
    the caller must sleep before proceeding (0.0 when a token is available).
    The first *burst* calls after an idle period pass immediately.
    """

    __slots__ = ("_interval", "_tau", "_tat", "_lock")

    def __init__(self, rate: float, burst: int = 1) -> None:
        """Configure the gate.

        Args:
            rate: Sustained executions allowed per second.
            burst: Executions allowed back-to-back after an idle period.
        """
        self._interval = 1.0 / rate
        self._tau = (burst - 1) * self._interval
        self._tat = 0.0
        self._lock = threading.Lock()

    def reserve(self) -> float:
        """Book the next execution slot.

        Returns:
            Seconds the caller must sleep before proceeding (``0.0`` if a token is available now).
        """
        with self._lock:
            now = time.perf_counter()
            tat = max(self._tat, now)
            self._tat = tat + self._interval
            return max(0.0, tat - self._tau - now)


def _check_queue_args(workers: int | None, size: int, timeout: float | None, rate: float | None, burst: int) -> None:
    """Validate the constructor arguments shared by every queue.

    Raises:
        ValueError: If any argument falls outside its allowed range.
    """
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
    """Interpret one map/group element: Mapping → kwargs, tuple → args, else single arg.

    Args:
        fn: The callable being fanned out.
        entry: One element of the input iterable.

    Returns:
        A ``(callable, args)`` pair ready to enqueue.
    """
    if isinstance(entry, Mapping):
        return partial(fn, **entry), ()
    if isinstance(entry, tuple):
        return fn, entry
    return fn, (entry,)


def _never_cancels(task_id: str) -> bool:
    """Cancellation hook for handles restored from a checkpoint — they are already done."""
    return False


def _task_key(entry: Any, key_fn: Callable[[Any], Any] | None, keyable: Any = None) -> str:
    """Derive the checkpoint key for one fan-out input.

    Args:
        entry: The original input element, as the user wrote it.
        key_fn: User-supplied key function; it always sees *entry* itself.
        keyable: JSON-encodable stand-in for *entry* when the entry itself is
            not (heterogeneous group tuples carry a callable).

    Returns:
        The key to look up and record this input under.
    """
    if key_fn is not None:
        return str(key_fn(entry))
    return canonical_key(entry if keyable is None else keyable)


class _SubmitPlane:
    """Submission API shared by all queues.

    Concrete queues provide ``_enqueue(fn, args, opts) -> handle``, a
    ``_counter`` (itertools.count), and a ``_group_cls`` class attribute.
    """

    _group_cls: type
    _handle_cls: type
    _counter: itertools.count

    def _enqueue(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> Any:
        """Queue one task and return its handle — implemented by each concrete queue.

        Raises:
            NotImplementedError: Always; subclasses must override this.
        """
        raise NotImplementedError

    def _new_task(self, fn: Any, args: tuple[Any, ...], opts: TaskOptions) -> _Task:
        """Build a handle and its task record from one submission.

        Args:
            fn: The callable (or awaitable) to run.
            args: Positional arguments for *fn*.
            opts: Fully resolved options for this task.

        Returns:
            The queued :class:`_Task`, with its handle ready to hand back to the caller.
        """
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
        """Cancel the task with *task_id* — implemented by each concrete queue.

        Raises:
            NotImplementedError: Always; subclasses must override this.
        """
        raise NotImplementedError

    def _restored_handle(self, fn: Any, opts: TaskOptions, hit: Any) -> Any:
        """Build an already-succeeded handle for an input the checkpoint has seen.

        The task is never submitted, so it costs no worker and never reaches the
        queue's result list.  ``attempts == 0`` distinguishes a restored result
        from one produced by this run.

        Args:
            fn: The callable the input would have run, used only for the name.
            opts: The resolved options for this submission.
            hit: The :class:`~osiiso.checkpoint.Hit` recovered from the store.

        Returns:
            A finished handle carrying the recorded value.
        """
        handle = self._handle_cls(
            task_id=uuid.uuid4().hex,
            name=opts.name or _callable_name(fn),
            priority=opts.priority,
            must_complete=opts.must_complete,
            created_at=time.perf_counter(),
            cancel_fn=_never_cancels,
            group_id=opts.group_id,
            detached=opts.detached,
            scheduled_for=None,
            metadata=opts.metadata,
        )
        note = "restored from checkpoint" if hit.has_value else "restored from checkpoint (value not retained)"
        handle._mark_finished(make_result(handle, "succeeded", value=hit.value, message=note))
        return handle

    def _enqueue_tracked(
        self, plan: list[tuple[str, Any, tuple[Any, ...]]], opts: TaskOptions, cp: Checkpoint, namespace: str
    ) -> list[Any]:
        """Submit a fan-out, skipping inputs the checkpoint already has.

        Args:
            plan: One ``(key, callable, args)`` triple per input, in input order.
            opts: The resolved options shared by every task.
            cp: The checkpoint to consult and record into.
            namespace: Namespace isolating this fan-out from others in the same store.

        Returns:
            One handle per input, in input order — restored handles for keys
            already recorded, live handles for everything else.
        """
        hits = cp.lookup(namespace, [key for key, _, _ in plan])
        handles = []
        for key, fn, args in plan:
            hit = hits.get(key)
            if hit is not None:
                handles.append(self._restored_handle(fn, opts, hit))
                continue
            handle = self._enqueue(fn, args, opts)
            cp._watch(handle, namespace, key)
            handles.append(handle)
        return handles

    def submit(self, fn: Any, /, *args: Any, opts: TaskOptions | None = None, **overrides: Any) -> Any:
        """Submit one task and return its handle.

        Positional *args* are forwarded to *fn*; use :func:`functools.partial`
        for keyword arguments.  Option fields may be passed inline
        (``retries=3``) or via *opts*; inline overrides win.

        Args:
            fn: The callable to run (:class:`~osiiso.AsyncQueue` also accepts a bare awaitable).
            *args: Positional arguments for *fn*.
            opts: Base :class:`~osiiso.TaskOptions` to apply.
            **overrides: Individual option fields, overriding *opts*.

        Returns:
            A :class:`~osiiso.TaskHandle` (async queue) or :class:`~osiiso.SyncTaskHandle`.

        Raises:
            ~osiiso.ClosedError: If the queue is not accepting tasks.
            ~osiiso.QueueFullError: Bounded :class:`~osiiso.AsyncQueue` only.
        """
        return self._enqueue(fn, args, resolve_opts(opts, overrides))

    def map(
        self,
        fn: Any,
        iterable: Iterable[Any],
        *,
        opts: TaskOptions | None = None,
        checkpoint: Checkpoint | None = None,
        key: Callable[[Any], Any] | None = None,
        namespace: str | None = None,
        **overrides: Any,
    ) -> list[Any]:
        """Submit *fn* once per element and return the handles.

        Elements: ``tuple`` → positional args, ``Mapping`` → keyword args,
        anything else → a single positional arg.

        Args:
            fn: The callable to run for every element.
            iterable: The elements to fan *fn* out over.
            opts: Base :class:`~osiiso.TaskOptions` shared by every task.
            checkpoint: A :class:`~osiiso.Checkpoint` to resume against.
                Elements it has already recorded are not submitted; they come
                back as finished handles carrying the stored value.
            key: Derives the checkpoint key from an element.  Defaults to the
                element's canonical JSON, so pass one when elements are not
                JSON-encodable (or are large).
            namespace: Isolates this fan-out inside the checkpoint.  Defaults
                to the name of *fn*.
            **overrides: Individual option fields, overriding *opts*.

        Returns:
            One handle per element, in input order.

        Raises:
            TypeError: If an element is not JSON-encodable and no *key* was given.
        """
        eff = resolve_opts(opts, overrides)
        if checkpoint is None:
            return [self._enqueue(f, a, eff) for f, a in (_fan(fn, e) for e in iterable)]
        plan = []
        for entry in iterable:
            f, a = _fan(fn, entry)
            plan.append((_task_key(entry, key), f, a))
        return self._enqueue_tracked(plan, eff, checkpoint, namespace or _callable_name(fn))

    def group(
        self,
        tasks: Any,
        iterable: Iterable[Any] | None = None,
        *,
        group_id: str | None = None,
        opts: TaskOptions | None = None,
        checkpoint: Checkpoint | None = None,
        key: Callable[[Any], Any] | None = None,
        namespace: str | None = None,
        **overrides: Any,
    ) -> Any:
        """Submit a batch and return a group handle.

        Two forms: ``group([(fn, *args), ...])`` for heterogeneous work, or
        ``group(fn, iterable)`` to map one callable over many inputs.

        Args:
            tasks: ``(callable, *args)`` tuples, or a single callable when *iterable* is given.
            iterable: Elements to map *tasks* over, interpreted as in :meth:`map`.
            group_id: Identifier for the group; generated when omitted.
            opts: Base :class:`~osiiso.TaskOptions` shared by every task.
            checkpoint: A :class:`~osiiso.Checkpoint` to resume against; see :meth:`map`.
            key: Derives the checkpoint key from an entry; see :meth:`map`.
            namespace: Isolates this batch inside the checkpoint.  Defaults to
                the name of *tasks* in the mapped form; in the heterogeneous
                form it must be given explicitly (or via an explicit
                *group_id*), because a generated group id changes every run and
                would never match on resume.
            **overrides: Individual option fields, overriding *opts*.

        Returns:
            A :class:`~osiiso.TaskGroup` (async queue) or :class:`~osiiso.SyncTaskGroup`.

        Raises:
            TypeError: If an entry of *tasks* is not a non-empty tuple.
            ValueError: If a heterogeneous batch is checkpointed without a stable namespace.

        Example::

            grp = q.group([(fetch, url), (parse, raw), (save, record)])
            summary = await grp.wait()   # or grp.wait() on sync queues
        """
        eff = resolve_opts(opts, overrides)
        gid = group_id or eff.group_id or f"group-{next(self._counter)}"
        if eff.group_id != gid:
            eff = eff.replace(group_id=gid)
        if iterable is not None:
            if checkpoint is None:
                handles = [self._enqueue(f, a, eff) for f, a in (_fan(tasks, e) for e in iterable)]
            else:
                plan = []
                for entry in iterable:
                    f, a = _fan(tasks, entry)
                    plan.append((_task_key(entry, key), f, a))
                handles = self._enqueue_tracked(plan, eff, checkpoint, namespace or _callable_name(tasks))
        elif checkpoint is None:
            handles = []
            for entry in tasks:
                if not isinstance(entry, tuple) or not entry:
                    raise TypeError("each group entry must be a tuple of (callable, *args)")
                handles.append(self._enqueue(entry[0], entry[1:], eff))
        else:
            ns = namespace or group_id
            if ns is None:
                raise ValueError(
                    "checkpointing group([(fn, *args), ...]) needs a stable name — pass namespace= "
                    "(or an explicit group_id=), since a generated group id never matches on resume"
                )
            plan = []
            for entry in tasks:
                if not isinstance(entry, tuple) or not entry:
                    raise TypeError("each group entry must be a tuple of (callable, *args)")
                # The entry holds a live callable, so key on its name plus the arguments.
                plan.append((_task_key(entry, key, [_callable_name(entry[0]), list(entry[1:])]), entry[0], entry[1:]))
            handles = self._enqueue_tracked(plan, eff, checkpoint, ns)
        return self._group_cls(gid, handles)

    def task(self, opts: TaskOptions | None = None, **overrides: Any) -> Callable[[Any], BoundTask]:
        """Decorator binding a function to this queue.

        Args:
            opts: Base :class:`~osiiso.TaskOptions` applied to every call.
            **overrides: Individual option fields, overriding *opts*.

        Returns:
            A decorator that wraps the function in a :class:`BoundTask`.

        Example::

            @q.task(retries=3, timeout=10)
            async def fetch(url): ...

            handle = fetch("https://example.com")
            handles = fetch.map(["u1", "u2"])
        """
        eff = resolve_opts(opts, overrides)

        def decorator(fn: Any) -> BoundTask:
            """Wrap *fn* so that calling it submits to this queue."""
            return BoundTask(fn, self, eff)

        return decorator


class BoundTask:
    """A callable bound to a queue by the ``@q.task()`` decorator.

    Calling it submits the wrapped function instead of running it, and
    :meth:`map` / :meth:`group` mirror the queue methods of the same name.
    """

    __slots__ = ("_fn", "_q", "_opts")

    def __init__(self, fn: Any, q: _SubmitPlane, opts: TaskOptions) -> None:
        """Bind *fn* to queue *q* with the default options *opts*."""
        self._fn = fn
        self._q = q
        self._opts = opts

    def __call__(self, *args: Any, **overrides: Any) -> Any:
        """Submit one call of the bound function.

        Args:
            *args: Positional arguments for the function.
            **overrides: Option fields overriding the bound defaults.

        Returns:
            The handle for the submitted task.
        """
        eff = resolve_opts(self._opts, overrides) if overrides else self._opts
        return self._q._enqueue(self._fn, args, eff)

    def map(
        self,
        iterable: Iterable[Any],
        *,
        checkpoint: Checkpoint | None = None,
        key: Callable[[Any], Any] | None = None,
        namespace: str | None = None,
        **overrides: Any,
    ) -> list[Any]:
        """Submit the bound function once per element; returns one handle each.

        *checkpoint*, *key*, and *namespace* behave exactly as in :meth:`_SubmitPlane.map`.
        """
        return self._q.map(self._fn, iterable, opts=self._opts, checkpoint=checkpoint, key=key, namespace=namespace, **overrides)

    def group(
        self,
        iterable: Iterable[Any],
        *,
        checkpoint: Checkpoint | None = None,
        key: Callable[[Any], Any] | None = None,
        namespace: str | None = None,
        **overrides: Any,
    ) -> Any:
        """Submit the bound function once per element as one group; returns the group handle.

        *checkpoint*, *key*, and *namespace* behave exactly as in :meth:`_SubmitPlane.map`.
        """
        return self._q.group(self._fn, iterable, opts=self._opts, checkpoint=checkpoint, key=key, namespace=namespace, **overrides)

    @property
    def __name__(self) -> str:
        """Name of the wrapped function."""
        return _callable_name(self._fn)

    def __repr__(self) -> str:
        """Return ``BoundTask('name')``."""
        return f"BoundTask({self.__name__!r})"
