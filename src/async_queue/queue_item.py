import time
import asyncio
from logging import getLogger
from typing import Coroutine, Callable

logger = getLogger(__name__)


class QueueItem:
    """A class to represent a task item in the queue.
    Attributes:
        - `task_item` (Callable | Coroutine): The task to run.

        - `args` (tuple): The arguments to pass to the task

        - `kwargs` (dict): The keyword arguments to pass to the task

        - `must_complete` (bool): A flag to indicate if the task must complete before the queue stops. Default is False.

        - `time` (int): The time the task was added to the queue.

        - `timeout` (int): An optional timeout for the task
        """
    def __init__(self, task_item: Callable | Coroutine, *args, **kwargs):
        self.task_item = task_item
        self.args = args
        self.kwargs = kwargs
        self.must_complete = False
        self.time = time.time_ns()
        self.timeout = None

    def __repr__(self):
        name = getattr(self.task_item, "__name__", str(self.task_item))
        return f"{self.__class__.__name__}({name})"

    def __hash__(self):
        return id(self)

    def __lt__(self, other):
        return self.time < other.time

    async def __call__(self, timeout: float = None):
        """Run the task asynchronously"""
        start = time.perf_counter()
        try:
            async with asyncio.timeout(timeout or self.timeout):
                if asyncio.iscoroutinefunction(self.task_item):
                    return await self.task_item(*self.args, **self.kwargs)
                else:
                    return await asyncio.to_thread(self.task_item, *self.args, **self.kwargs)

        except asyncio.TimeoutError:
            logger.error("Task %s timed out after %d seconds", self, time.perf_counter() - start)

        except asyncio.CancelledError:
            logger.debug("Task %s was cancelled", self)

        except Exception as err:
            logger.error("Error %s occurred in %s", err, self)

    def set_attributes(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.items()]
