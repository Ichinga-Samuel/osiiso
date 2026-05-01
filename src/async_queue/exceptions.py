from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._base import TaskResult


class TaskQueueError(Exception):
    """Base exception for the task queue package."""


class QueueClosedError(TaskQueueError):
    """Raised when work is submitted while the queue is shutting down."""


class QueueExecutionError(TaskQueueError):
    """Raised when one or more tasks fail and the caller requests strict mode."""

    def __init__(self, results: list[TaskResult] | tuple[TaskResult, ...]):
        self.results = tuple(results)
        super().__init__(f"{len(self.results)} queued task(s) failed.")
