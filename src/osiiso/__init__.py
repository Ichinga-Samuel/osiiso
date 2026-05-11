"""osiiso — structured concurrency for asyncio, threading, and multiprocessing.

Quick start::

    import osiiso

    async def main():
        async with osiiso.AsyncQueue(workers=4) as q:
            q.submit(work, 1, retries=3)
            q.submit(work, 2, retries=3)
            return await q.run()

    summary = osiiso.run(main())
"""

from .asyncqueue import AsyncQueue
from .exceptions import ClosedError, ExecutionError, OsiisoError
from .group import SyncTaskGroup, TaskGroup
from .handle import SyncTaskHandle, TaskHandle
from .loop import run
from .options import TaskOptions
from .processqueue import ProcessQueue
from .result import RunSummary, TaskResult
from .threadqueue import ThreadQueue

__all__ = [
    # Core queues
    "AsyncQueue",
    "ThreadQueue",
    "ProcessQueue",
    # Handles
    "TaskHandle",
    "SyncTaskHandle",
    # Groups
    "TaskGroup",
    "SyncTaskGroup",
    # Options & results
    "TaskOptions",
    "TaskResult",
    "RunSummary",
    # Exceptions
    "OsiisoError",
    "ClosedError",
    "ExecutionError",
    # Runner
    "run",
]
