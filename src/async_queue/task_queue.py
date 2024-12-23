import asyncio
import random
import time
from logging import getLogger
from signal import SIGINT, signal
from typing import Literal

from .queue_item import QueueItem

logger = getLogger(__name__)


class TaskQueue:
    queue_task: asyncio.Task
    start_time: float
    running_time: float

    def __init__(self, *, size: int = 0, workers: int = 10, timeout: int = 0, queue: asyncio.Queue = None,
                 on_exit: Literal['cancel', 'complete_priority'] = 'complete_priority',
                 mode: Literal['finite', 'infinite'] = 'finite', worker_timeout: int = 1):
        self.queue = queue or asyncio.PriorityQueue(maxsize=size)
        self.workers = workers
        self.worker_tasks = {}
        self.timeout = timeout
        self.stop = False
        self.on_exit = on_exit
        self.mode = mode
        self.worker_timeout = worker_timeout
        signal(SIGINT, self.sigint_handle)

    def add(self, *, item: QueueItem, priority=3, must_complete=False):
        """Add a task to the queue.

        Args:
            item (QueueItem): The task to add to the queue.
            priority (int): The priority of the task. Default is 3.
            must_complete (bool): A flag to indicate if the task must complete before the queue stops. Default is False.
        """
        try:
            if self.stop:
                return
            item.must_complete = must_complete
            if isinstance(self.queue, asyncio.PriorityQueue):
                item = (priority, item)
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.error("Queue is full")

    async def worker(self, wid: int = None):
        """Worker function to run tasks in the queue."""
        while True:
            try:
                if self.mode == 'infinite' and self.queue.qsize() <= 1:
                    dummy = QueueItem(self.dummy_task)
                    self.add(item=dummy)

                if self.timeout and (time.perf_counter() - self.start_time) > self.timeout:
                    if self.on_exit == 'cancel':
                        self.cancel()
                    else:
                        self.stop = True
                        self.set_timer(timeout=0)

                if isinstance(self.queue, asyncio.PriorityQueue):
                    _, item = self.queue.get_nowait()

                else:
                    item = self.queue.get_nowait()

                if self.stop is False or item.must_complete:
                    await item.run()

                self.queue.task_done()

                if self.stop and (self.on_exit == 'cancel' or len(self.worker_tasks) <= 1):
                    self.cancel()

                await self.add_workers()

            except asyncio.QueueEmpty:
                if self.stop:
                    self.remove_worker(wid)
                    break

                if self.mode == 'finite':
                    self.remove_worker(wid)
                    break

            except asyncio.CancelledError:
                self.remove_worker(wid)
                break

            except Exception as err:
                logger.error("%s: Error occurred in worker", err)
                self.remove_worker(wid)
                break

    def set_timer(self, *, timeout: int = 0):
        self.start_time = time.perf_counter()
        self.timeout = timeout

    async def dummy_task(self):
        await asyncio.sleep(self.worker_timeout)

    def remove_worker(self, wid: int):
        self.worker_tasks.pop(wid, None)

    async def add_workers(self, no_of_workers: int = None):
        """Create workers for running queue tasks."""
        if no_of_workers is None:
            queue_size = self.queue.qsize()
            req_workers = queue_size - len(self.worker_tasks)
            if req_workers > 1:
                no_of_workers = req_workers
            else:
                return

        ri = lambda : random.randint(999, 999_999_999) # random id
        ct = lambda ti: asyncio.create_task(self.worker(wid=ti), name=ti) # create task
        wr = range(no_of_workers)
        [self.worker_tasks.setdefault(wi:=ri(), ct(wi)) for _ in wr]

    async def run(self, timeout: int = None):
        """Run the queue until all tasks are completed or the timeout is reached.

        Args:
            timeout (int): The maximum time to wait for the queue to complete. Default is 0.
            This timeout overrides the timeout attribute of the queue instance.
            The queue stops when the timeout is reached, and the remaining tasks are handled based on the
            `on_exit` attribute. If the timeout is 0, the queue will run until all tasks are completed or the queue
            is stopped.
        """
        try:
            timeout = timeout or self.timeout
            self.set_timer(timeout=timeout)
            self.running_time = time.perf_counter()
            await self.add_workers(no_of_workers=self.workers)
            self.queue_task = asyncio.create_task(self.queue.join())
            await self.queue_task

        except asyncio.CancelledError:
            logger.warning("Task Queue Cancelled after %d seconds, %d tasks remaining",
                           time.perf_counter() - self.running_time, self.queue.qsize())

        except Exception as err:
            logger.warning("An error occurred after %d seconds, %d tasks remaining",
                           time.perf_counter() - self.running_time, self.queue.qsize())
            logger.warning("%s: An error occurred in TaskQueue ...exiting.", err)

        finally:
            logger.warning("Tasks completed after %d seconds, %d tasks remaining",
                           time.perf_counter() - self.running_time, self.queue.qsize())


    def cancel(self):
        try:
            self.queue_task.cancel()
            self.worker_tasks.clear()
        except asyncio.CancelledError:
            ...
        except Exception as err:
            logger.error("%s: occurred in canceling all tasks", err)

    def sigint_handle(self, sig, frame):
        if self.stop is False:
            self.stop = True
        else:
            self.cancel()


TaskQueue.__doc__ = """TaskQueue is a class that allows you to queue tasks and run them concurrently with a specified number of workers.

    Attributes:
        - `workers` (int): The number of workers to run concurrently. Default is 10.

        - `timeout` (int): The maximum time to wait for the queue to complete. Default is None. If timeout is provided
            the queue is joined using `asyncio.wait_for` with the timeout.

        - `queue` (asyncio.Queue): The queue to store the tasks. Default is `asyncio.PriorityQueue` with no size limit.

        - `on_exit` (Literal["cancel", "complete_priority"]): The action to take when the queue is stopped.

        - `mode` (Literal["finite", "infinite"]): The mode of the queue. If `finite` the queue will stop when all tasks
            are completed. If `infinite` the queue will continue to run until stopped.

        - `worker_timeout` (int): The time to wait for a task to be added to the queue before stopping the worker or
            adding a dummy sleep task to the queue.

        - `stop` (bool): A flag to stop the queue instance.

        - `worker_task` (dict[int: asyncio.Task]): A dict of the worker tasks running concurrently,
"""
