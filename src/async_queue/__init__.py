from ._base import QueueRunSummary, TaskResult
from .exceptions import QueueClosedError, QueueExecutionError, TaskQueueError
from .process_queue_item import ProcessQueueItem
from .process_task_queue import ProcessTaskHandle, ProcessTaskQueue
from .queue_item import QueueItem
from .task_queue import TaskHandle, TaskQueue
from .thread_queue_item import ThreadQueueItem
from .thread_task_queue import ThreadTaskHandle, ThreadTaskQueue

# Backward-compatible aliases for the unified types
ThreadTaskResult = TaskResult
ThreadQueueRunSummary = QueueRunSummary
ProcessTaskResult = TaskResult
ProcessQueueRunSummary = QueueRunSummary

__all__ = [
    "ProcessQueueItem",
    "ProcessQueueRunSummary",
    "ProcessTaskHandle",
    "ProcessTaskQueue",
    "ProcessTaskResult",
    "QueueClosedError",
    "QueueExecutionError",
    "QueueItem",
    "QueueRunSummary",
    "TaskHandle",
    "TaskQueue",
    "TaskQueueError",
    "TaskResult",
    "ThreadQueueItem",
    "ThreadQueueRunSummary",
    "ThreadTaskHandle",
    "ThreadTaskQueue",
    "ThreadTaskResult",
]
