from ._base import FailPolicy, QueueRunSummary, TaskResult
from .exceptions import QueueClosedError, QueueExecutionError, TaskQueueError
from .process_queue_item import ProcessQueueItem
from .process_task_queue import ProcessTaskGroupHandle, ProcessTaskHandle, ProcessTaskQueue
from .queue_item import QueueItem
from .task_queue import TaskGroupHandle, TaskHandle, TaskQueue
from .thread_queue_item import ThreadQueueItem
from .thread_task_queue import ThreadTaskGroupHandle, ThreadTaskHandle, ThreadTaskQueue

# Backward-compatible aliases for the unified types
ThreadTaskResult = TaskResult
ThreadQueueRunSummary = QueueRunSummary
ProcessTaskResult = TaskResult
ProcessQueueRunSummary = QueueRunSummary

__all__ = [
    "ProcessQueueItem",
    "ProcessQueueRunSummary",
    "ProcessTaskGroupHandle",
    "ProcessTaskHandle",
    "ProcessTaskQueue",
    "ProcessTaskResult",
    "FailPolicy",
    "QueueClosedError",
    "QueueExecutionError",
    "QueueItem",
    "QueueRunSummary",
    "TaskHandle",
    "TaskGroupHandle",
    "TaskQueue",
    "TaskQueueError",
    "TaskResult",
    "ThreadQueueItem",
    "ThreadQueueRunSummary",
    "ThreadTaskGroupHandle",
    "ThreadTaskHandle",
    "ThreadTaskQueue",
    "ThreadTaskResult",
]
