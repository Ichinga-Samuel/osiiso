"""Immutable result types: :class:`TaskResult` per task and :class:`RunSummary` per run."""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Literal

TaskStatus = Literal["succeeded", "failed", "cancelled"]


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Immutable record of a single completed task.

    Attributes:
        task_id: Unique hex identifier for the task.
        name: Human-readable name (from the callable or ``TaskOptions.name``).
        status: ``"succeeded"``, ``"failed"``, or ``"cancelled"``.
        value: The callable's return value (succeeded only).
        exception: The exception raised by the callable (failed only).
        attempts: Total execution attempts (1 on first success, >1 after retries).
        priority: Priority copied from :class:`~osiiso.TaskOptions`.
        must_complete: Whether the task was protected from cancellation.
        group_id: Owning group identifier, or ``None``.
        detached: ``True`` if the result was excluded from the run summary.
        scheduled_for: Absolute monotonic target start time, or ``None``.
        created_at / started_at / finished_at: Monotonic timestamps.
        duration: Seconds from first execution to completion (0.0 if never started).
        message: Short human-readable description of the outcome.
        metadata: User data from :class:`~osiiso.TaskOptions.metadata`.
    """

    task_id: str
    name: str
    status: TaskStatus
    value: Any = None
    exception: BaseException | None = None
    attempts: int = 0
    priority: int = 0
    must_complete: bool = False
    group_id: str | None = None
    detached: bool = False
    scheduled_for: float | None = None
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float = 0.0
    duration: float = 0.0
    message: str = ""
    metadata: Any = None


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Aggregate summary of a queue run or group wait.

    Attributes:
        total_submitted: Total tasks covered by this summary.
        succeeded / failed / cancelled: Counts by final status.
        timed_out: ``True`` if the run was cut short by a timeout.
        duration: Wall-clock seconds from run start to summary creation.
        results: Ordered tuple of every :class:`TaskResult` in this run.
    """

    total_submitted: int
    succeeded: int
    failed: int
    cancelled: int
    timed_out: bool
    duration: float
    results: tuple[TaskResult, ...]

    @property
    def errors(self) -> tuple[TaskResult, ...]:
        """All failed results."""
        return tuple(r for r in self.results if r.status == "failed")

    @property
    def values(self) -> tuple[Any, ...]:
        """Return values of all succeeded tasks, in result order."""
        return tuple(r.value for r in self.results if r.status == "succeeded")

    @property
    def ok(self) -> bool:
        """``True`` if no failures, cancellations, or timeout."""
        return self.failed == 0 and self.cancelled == 0 and not self.timed_out

    def by_task_id(self) -> dict[str, TaskResult]:
        """Index results by ``task_id``."""
        return {r.task_id: r for r in self.results}

    def by_name(self) -> dict[str, tuple[TaskResult, ...]]:
        """Group results by task name."""
        grouped: dict[str, list[TaskResult]] = defaultdict(list)
        for r in self.results:
            grouped[r.name].append(r)
        return {k: tuple(v) for k, v in grouped.items()}

    def by_group(self) -> dict[str | None, tuple[TaskResult, ...]]:
        """Group results by ``group_id`` (``None`` for ungrouped tasks)."""
        grouped: dict[str | None, list[TaskResult]] = defaultdict(list)
        for r in self.results:
            grouped[r.group_id].append(r)
        return {k: tuple(v) for k, v in grouped.items()}

    def successes(self) -> tuple[TaskResult, ...]:
        """All succeeded results."""
        return tuple(r for r in self.results if r.status == "succeeded")

    def cancellations(self) -> tuple[TaskResult, ...]:
        """All cancelled results."""
        return tuple(r for r in self.results if r.status == "cancelled")

    def raise_for_errors(self, *, include_cancelled: bool = False) -> None:
        """Raise :class:`~osiiso.ExecutionError` if any task failed.

        Args:
            include_cancelled: Also raise when tasks were cancelled.
        """
        from .exceptions import ExecutionError

        bad = self.errors + (self.cancellations() if include_cancelled else ())
        if bad:
            raise ExecutionError(bad)

    def display(self) -> None:
        """Print a clean, human-readable summary to stdout."""
        status = "TIMED OUT" if self.timed_out else ("PASSED" if self.ok else "COMPLETED WITH ERRORS")
        bar = "-" * 40
        lines = [
            bar,
            f"  Run Summary: {status}",
            bar,
            f"  Total tasks : {self.total_submitted}",
            f"  Succeeded   : {self.succeeded}",
            f"  Failed      : {self.failed}",
            f"  Cancelled   : {self.cancelled}",
            f"  Duration    : {self.duration:.3f}s",
        ]
        if self.errors:
            lines += [bar, "  Failures:"]
            for r in self.errors:
                label = f"    - {r.name}"
                if r.attempts > 1:
                    label += f" ({r.attempts} attempts)"
                lines.append(label)
                if r.message:
                    lines.append(f"      {r.message}")
        lines.append(bar)
        print("\n".join(lines))

    @classmethod
    def from_results(cls, results: list[TaskResult] | tuple[TaskResult, ...], *, run_start: float, timed_out: bool) -> RunSummary:
        """Build a summary from collected results (internal factory)."""
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


def make_result(
    handle: Any, status: TaskStatus, *, value: Any = None, exception: BaseException | None = None, message: str = ""
) -> TaskResult:
    """Build a :class:`TaskResult` from a handle's current state (internal helper)."""
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
        group_id=handle.group_id,
        detached=handle.detached,
        scheduled_for=handle.scheduled_for,
        created_at=handle.created_at,
        started_at=started_at,
        finished_at=finished_at,
        duration=(finished_at - started_at) if started_at else 0.0,
        message=message,
        metadata=handle.metadata,
    )
