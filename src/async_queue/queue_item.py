import time
import asyncio
from logging import getLogger
from typing import Coroutine, Callable

logger = getLogger(__name__)


class QueueItem:
    def __init__(self, task_item: Callable | Coroutine, *args, **kwargs):
        self.task_item = task_item
        self.args = args
        self.kwargs = kwargs
        self.must_complete = False
        self.time = time.time_ns()

    def __hash__(self):
        return self.time

    def __lt__(self, other):
        return self.time < other.time

    def __eq__(self, other):
        return self.time == other.time

    def __le__(self, other):
        return self.time <= other.time

    async def run(self):
        try:
            if asyncio.iscoroutinefunction(self.task_item):
                return await self.task_item(*self.args, **self.kwargs)
            else:
                return await asyncio.to_thread(self.task_item, *self.args, **self.kwargs)
        except asyncio.CancelledError:
            logger.debug("Task %s with args %s and %s was cancelled",
                           self.task_item.__name__, self.args, self.kwargs)
        except Exception as err:
            logger.error("Error %s occurred in %s with args %s and %s",
                         err, self.task_item.__name__, self.args, self.kwargs)
