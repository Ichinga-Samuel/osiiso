"""Immutable result types produced after task and queue execution.

This module provides two frozen dataclasses:

* :class:`TaskResult` — the outcome of a single task execution.
* :class:`RunSummary` — an aggregate summary of a completed queue run or
  group wait.
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Immutable record of a single completed task.

    Created internally by queue workers via :func:`make_result`.

    Attributes:
        task_id: UUID hex string that uniquely identifies the task.
        name: Human-readable name derived from the callable or
            ``TaskOptions.name``.
        status: Final outcome — ``"succeeded"``, ``"failed"``, or
            ``"cancelled"``.
        value: The callable's return value (``status == "succeeded"`` only).
        exception: The exception raised by the callable (``"failed"`` only).
        attempts: Total execution attempts (1 on first success, >1 after
            retries).
        priority: Priority level copied from :class:`~osiiso.TaskOptions`.
        must_complete: Whether the task bypasses cancellation on shutdown.
        group_id: Owning group identifier, or ``None``.
        detached: ``True`` if the result is not awaited by ``run()``.
        scheduled_for: Absolute ``perf_counter`` target time, or ``None``.
        created_at: ``perf_counter`` timestamp when the task was submitted.
        started_at: ``perf_counter`` timestamp of the first attempt, or
            ``None`` if cancelled before starting.
        finished_at: ``perf_counter`` timestamp when the task completed.
        duration: Wall-clock seconds from first execution to completion.
        message: Short human-readable description of the outcome.
    """

    task_id: str
    name: str
    status: Literal["succeeded", "failed", "cancelled"]
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


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Aggregate summary of a completed queue run or group wait.

    Produced by ``AsyncQueue.run()``, ``ThreadQueue.run()``,
    ``ProcessQueue.run()``, and the ``wait()`` methods of task groups.

    Attributes:
        total_submitted: Total tasks covered by this summary.
        succeeded: Count of tasks with ``status == "succeeded"``.
        failed: Count of tasks with ``status == "failed"``.
        cancelled: Count of tasks with ``status == "cancelled"``.
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
        """All failed :class:`TaskResult` objects in this run."""
        return tuple(r for r in self.results if r.status == "failed")

    @property
    def values(self) -> tuple[Any, ...]:
        """Return values of all succeeded tasks, in result order."""
        return tuple(r.value for r in self.results if r.status == "succeeded")

    @property
    def ok(self) -> bool:
        """``True`` if the run completed with no failures, cancellations, or timeouts."""
        return self.failed == 0 and self.cancelled == 0 and not self.timed_out

    def by_task_id(self) -> dict[str, TaskResult]:
        """Index results by ``task_id``.

        Returns:
            Mapping of ``task_id`` → :class:`TaskResult`.
        """
        return {r.task_id: r for r in self.results}

    def by_name(self) -> dict[str, tuple[TaskResult, ...]]:
        """Group results by task name.

        Returns:
            Mapping of task name → tuple of :class:`TaskResult` objects.
        """
        grouped: dict[str, list[TaskResult]] = defaultdict(list)
        for r in self.results:
            grouped[r.name].append(r)
        return {k: tuple(v) for k, v in grouped.items()}

    def by_group(self) -> dict[str | None, tuple[TaskResult, ...]]:
        """Group results by ``group_id``.

        Returns:
            Mapping of ``group_id`` (``None`` for ungrouped tasks) → tuple
            of :class:`TaskResult` objects.
        """
        grouped: dict[str | None, list[TaskResult]] = defaultdict(list)
        for r in self.results:
            grouped[r.group_id].append(r)
        return {k: tuple(v) for k, v in grouped.items()}

    def successes(self) -> tuple[TaskResult, ...]:
        """All succeeded :class:`TaskResult` objects in this run."""
        return tuple(r for r in self.results if r.status == "succeeded")

    def cancellations(self) -> tuple[TaskResult, ...]:
        """All cancelled :class:`TaskResult` objects in this run."""
        return tuple(r for r in self.results if r.status == "cancelled")

    def raise_for_errors(self) -> None:
        """Raise if any task in this run failed.

        Raises:
            ~osiiso.ExecutionError: Contains all failed :class:`TaskResult`
                objects when ``failed > 0``.
        """
        from .exceptions import ExecutionError

        if self.errors:
            raise ExecutionError(list(self.errors))

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
            lines.append(bar)
            lines.append("  Failures:")
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
    def from_results(
        cls, results: list[TaskResult], *, run_start: float, timed_out: bool
    ) -> RunSummary:
        """Build a :class:`RunSummary` from a list of task results.

        Internal factory used by queue implementations.

        Args:
            results: :class:`TaskResult` objects collected during the run.
            run_start: A ``perf_counter`` timestamp marking the run start.
            timed_out: Whether the run was interrupted by a timeout.

        Returns:
            A fully-populated :class:`RunSummary`.
        """
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
    handle: Any,
    *,
    status: Literal["succeeded", "failed", "cancelled"],
    value: Any = None,
    exception: BaseException | None = None,
    message: str = "",
) -> TaskResult:
    """Build a :class:`TaskResult` from a handle's current state.

    Internal helper called by queue worker loops.

    Args:
        handle: A :class:`~osiiso.TaskHandle` or
            :class:`~osiiso.SyncTaskHandle` whose metadata is copied into
            the result.
        status: Final outcome of the task.
        value: Callable return value (``"succeeded"`` only).
        exception: Exception raised by the callable (``"failed"`` only).
        message: Short description of the outcome.

    Returns:
        A frozen :class:`TaskResult`.
    """
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
    )
