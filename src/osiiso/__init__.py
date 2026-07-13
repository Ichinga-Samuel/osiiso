"""osiiso — structured concurrency and parallelism for asyncio, threading, and multiprocessing.

Quick start::

    import osiiso

    async def main():
        async with osiiso.AsyncQueue(workers=4) as q:
            q.submit(work, 1, retries=3)
            q.submit(work, 2, retries=3)
            return await q.run()

    summary = osiiso.run(main())

    # Or one-shot helpers:
    values = osiiso.tmap(process, items, workers=8)
"""

from .asyncqueue import AsyncQueue
from .exceptions import ClosedError, ExecutionError, OsiisoError, QueueFullError
from .group import SyncTaskGroup, TaskGroup, as_completed, iter_completed
from .handle import SyncTaskHandle, TaskHandle
from .loop import run
from .options import TaskOptions
from .processqueue import ProcessQueue
from .result import RunSummary, TaskResult
from .sharedqueue import SharedQueue
from .shortcuts import amap, pmap, tmap
from .threadqueue import ThreadQueue

__all__ = [
    # Core queues
    "AsyncQueue",
    "SharedQueue",
    "ThreadQueue",
    "ProcessQueue",
    # Handles
    "TaskHandle",
    "SyncTaskHandle",
    # Groups & completion iteration
    "TaskGroup",
    "SyncTaskGroup",
    "as_completed",
    "iter_completed",
    # Options & results
    "TaskOptions",
    "TaskResult",
    "RunSummary",
    # Exceptions
    "OsiisoError",
    "ClosedError",
    "QueueFullError",
    "ExecutionError",
    # Runners & shortcuts
    "run",
    "amap",
    "tmap",
    "pmap",
]
