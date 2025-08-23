import time
import asyncio
from logging import getLogger
from functools import partial
from typing import Coroutine, Callable

logger = getLogger(__name__)


class QueueItem:
    """A class to represent a task item in the queue.

        Attributes:
            - `task` (Callable | Coroutine): The task to run.

            - `args` (tuple): The arguments to pass to the task

            - `kwargs` (dict): The keyword arguments to pass to the task

            - `must_complete` (bool): A flag to indicate if the task must complete before the queue stops. Default is False.

            - `time` (int): The time the task was added to the queue.
    """
    must_complete: bool
    def __init__(self, task: Callable | Coroutine, /, *args, **kwargs):
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.time = time.time_ns()

    def __hash__(self):
        return self.time

    def __lt__(self, other):
        return self.time < other.time

    def __eq__(self, other):
        return self.time == other.time

    def __le__(self, other):
        return self.time <= other.time

    async def __call__(self):
        try:
            if asyncio.iscoroutinefunction(self.task):
                return await self.task(*self.args, **self.kwargs)

            elif not asyncio.iscoroutinefunction(self.task):
                loop = asyncio.get_running_loop()
                func = partial(self.task, *self.args, **self.kwargs)
                return await loop.run_in_executor(None, func)
        except asyncio.CancelledError:
            logger.debug("Task %s was cancelled",self.task.__name__)
        except Exception as err:
            logger.error("Error %s occurred in %s",err, self.task.__name__)
