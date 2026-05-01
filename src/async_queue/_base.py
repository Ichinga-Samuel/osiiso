from __future__ import annotations

import inspect
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

TaskStatus = Literal["pending", "running", "retrying", "succeeded", "failed", "cancelled"]
QueueMode = Literal["finite", "infinite"]
ShutdownPolicy = Literal["cancel", "complete_priority"]

_QI = TypeVar("_QI", bound="_BaseQueueItem")


@dataclass(frozen=True, slots=True)
class TaskResult:
    """The final state of a queued task."""

    task_id: str
    name: str
    status: Literal["succeeded", "failed", "cancelled"]
    value: Any = None
    exception: BaseException | None = None
    attempts: int = 0
    priority: int = 0
    must_complete: bool = False
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float = 0.0
    duration: float = 0.0
    message: str = ""


@dataclass(frozen=True, slots=True)
class QueueRunSummary:
    """A compact summary for a completed queue run."""

    total_submitted: int
    succeeded: int
    failed: int
    cancelled: int
    timed_out: bool
    duration: float
    results: tuple[TaskResult, ...]

    @property
    def errors(self) -> tuple[TaskResult, ...]:
        return tuple(r for r in self.results if r.status == "failed")

    def raise_for_errors(self) -> None:
        from .exceptions import QueueExecutionError

        if self.errors:
            raise QueueExecutionError(list(self.errors))

    @classmethod
    def from_results(
        cls,
        results: list[TaskResult],
        *,
        run_start: float,
        timed_out: bool,
    ) -> QueueRunSummary:
        items = tuple(results)
        counts = Counter(r.status for r in items)
        return cls(
            total_submitted=len(items),
            succeeded=counts["succeeded"],
            failed=counts["failed"],
            cancelled=counts["cancelled"],
            timed_out=timed_out,
            duration=time.perf_counter() - run_start,
            results=items,
        )


class _BaseQueueItem:
    """Shared initialisation, configuration, and comparison logic for queue items."""

    must_complete: bool

    def __init__(self, task: Any, /, *args: Any, **kwargs: Any) -> None:
        if not callable(task) and not inspect.isawaitable(task):
            raise TypeError("task must be a callable or an awaitable object")
        if inspect.isawaitable(task) and (args or kwargs):
            raise TypeError("awaitable tasks cannot receive extra args or kwargs")

        self.task = task
        self.args = args
        self.kwargs = dict(kwargs)
        self.created_at = time.perf_counter()
        self.task_id = uuid.uuid4().hex
        self.name = _resolve_name(task)
        self.timeout: float | None = None
        self.retries = 0
        self.retry_delay = 0.0
        self.backoff = 1.0
        self.must_complete = False

    def __hash__(self) -> int:
        return hash(self.task_id)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, _BaseQueueItem):
            return NotImplemented
        return (self.created_at, self.task_id) < (other.created_at, other.task_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _BaseQueueItem):
            return NotImplemented
        return self.task_id == other.task_id

    def configure(
        self: _QI,
        *,
        must_complete: bool = False,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
    ) -> _QI:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        if retries < 0:
            raise ValueError("retries must be greater than or equal to 0")
        if retry_delay < 0:
            raise ValueError("retry_delay must be greater than or equal to 0")
        if backoff <= 0:
            raise ValueError("backoff must be greater than 0")

        self.must_complete = must_complete
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.backoff = backoff
        if name:
            self.name = name
        return self


def _resolve_name(task: Any) -> str:
    if inspect.isawaitable(task):
        code = getattr(task, "cr_code", None)
        if code is not None:
            return code.co_name
        return task.__class__.__name__

    name = getattr(task, "__name__", None)
    if name:
        return name
    return task.__class__.__name__


def _make_result(
    handle: Any,
    *,
    status: Literal["succeeded", "failed", "cancelled"],
    value: Any = None,
    exception: BaseException | None = None,
    message: str = "",
) -> TaskResult:
    """Build a ``TaskResult`` from a handle's current state."""
    finished_at = time.perf_counter()
    started_at = handle._started_at
    return TaskResult(
        task_id=handle.task_id,
        name=handle.name,
        status=status,
        value=value,
        exception=exception,
        attempts=handle.attempts,
        priority=handle.priority,
        must_complete=handle.must_complete,
        created_at=handle.created_at,
        started_at=started_at,
        finished_at=finished_at,
        duration=(finished_at - started_at) if started_at else 0.0,
        message=message,
    )
