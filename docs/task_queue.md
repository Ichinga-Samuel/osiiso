# Task Queue

## Table of Contents
- [TaskQueue](#taskqueue.taskqueue)
- [add](#taskqueue.add)
- [worker](#taskqueue.worker)
- [check_timeout](#taskqueue.check_timeout)
- [dummy_task](#taskqueue.dummy_task)
- [add_worker](#taskqueue.add_workers)
- [add_task](#taskqueue.add_task)
- [run](#taskqueue.run)
- [cancel_all_workers](#taskqueue.cancel_all_workers)
- [cancel](#taskqueue.cancel)
- [watch](#taskqueue.watch)

<a id="taskqueue.taskqueue"></a>
### TaskQueue
```python
class TaskQueue
```
TaskQueue is a class that allows you to queue tasks and run them concurrently with a
specified number of workers.

#### Attributes:
| Name              | Type                                   | Description                                                                                                                                      | Default                                    |
|-------------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `workers`         | int                                    | The number of workers to run concurrently.                                                                                                       | 10                                         |
| `queue_timeout`   | int                                    | The maximum time to run the queue, after queue_timeout, new tasks are not added but the queue might be allowed to run until completion.          | None                                       |
| `queue`           | asyncio.Queue                          | The queue to store the tasks.                                                                                                                    | `asyncio.PriorityQueue` with no size limit |
| `on_exit`         | Literal["cancel", "complete_priority"] | The action to take when the queue is stopped.                                                                                                    | complete_priority                          |
| `mode`            | Literal["finite", "infinite"]          | The mode of the queue. If `finite` the queue will stop when all tasks are completed. If `infinite` the queue will continue to run until stopped. | finite                                     |
| `queue_timeout`   | float                                  | If specified the queue will stop accepting tasks at this point, but will try and complete pending tasks.                                         | None                                       |
| `queue_cancelled` | bool                                   | A boolean flag to indicate if the main queue task is still running                                                                               | False                                      |
| `stop`            | bool                                   | A flag to stop the queue instance.                                                                                                               | False                                      |
| `worker_tasks`    | dict[int: asyncio.Task]                | A dict of the worker tasks running concurrently,                                                                                                 |                                            |


<a id="taskqueue.add"></a>
### add
```python
def add(self, *, item: QueueItem, priority=3, must_complete=False, with_new_workers=True)
```
Add a task to the queue.

#### Parameters
| Name               | Type        | Description                                                          | Default |
|--------------------|-------------|----------------------------------------------------------------------|---------|
| `item`             | `QueueItem` | The task to add to the queue.                                        |         |
| `priority`         | `int`       | The priority of the task.                                            | `3`     |
| `must_complete`    | `bool`      | A flag to indicate if the task must complete before the queue stops. | `False` |
| `with_new_workers` | `bool`      | Whether to add new workers when adding a new item to the queue       | `True`  |


<a id="taskqueue.add_task"</a>
### add_task
```python
def add_task(self, *, task: Callable | Coroutine, *args, must_complete=False, priority=3,  **kwargs)
```
Add a task to the queue by specifying a callable or coroutine and its arguments.
#### Parameters
| Name            | Type                      | Description                                                          | Default |
|-----------------|---------------------------|----------------------------------------------------------------------|---------|
| `task`          | `Callable` or `Coroutine` | The callable or coroutine to add to the queue.                       |         |                                                            |         |
| `*args`         | `Any`                     | Positional arguments to pass to the callable or coroutine.           |         |
| `must_complete` | `bool`                    | A flag to indicate if the task must complete before the queue stops. | `False` |   
| `priority`      | `int`                     | The priority of the task.                                            | `3`     |
| `**kwargs`      | `Any`                     | Keyword arguments to pass to the callable or coroutine.              |         |


<a id="taskqueue.worker"></a>
### worker
```python
async def worker(self, wid: int = None):
```
Worker function to run tasks in the queue.

#### Parameters
| Name  | Type  | Description | Default |
|-------|-------|-------------|---------|
| `wid` | `int` | Worker ID.  | `None`  |


### check_timeout
```python
async def check_timeout(self)
```
Checks if the queue has timed out.


<a id="taskqueue.dummy_task"></a>
### dummy_task
```python
@staticmethod
async def dummy_task():
```
A dummy task for worker to execute when queue is empty in infinite mode.


<a id="taskqueue.remove_worker"></a>
### remove_worker
```python
def remove_worker(self, wid: int):
```
Removes a worker from the worker tasks and cancels the worker task.

#### Parameters
| Name  | Type  | Description                     | Default |
|-------|-------|---------------------------------|---------|
| `wid` | `int` | The ID of the worker to remove. |         |


<a id="taskqueue.add_workers"></a>
### add_workers
```python
async def add_workers(self, no_of_workers: int = None):
```
Create workers for running queue tasks.

#### Parameters
| Name            | Type  | Description                            | Default |
|-----------------|-------|----------------------------------------|---------|
| `no_of_workers` | `int` | Number of workers to add to the queue. | `None`  |


<a id="taskqueue.run"></a>
### run
```python
async def run(self, queue_timeout: int = None):
```
Run the queue until all tasks are completed or the timeout is reached.

#### Parameters
| Name               | Type  | Description                                | Default |
|--------------------|-------|--------------------------------------------|---------|
| `queue_timeout`    | `int` | The maximum time for the task queue to run | `None`  |


<a id="taskqueue.cancel_all_workers"></a>
### cancel_all_workers
```python
def cancel_all_workers(self):
```
Cancels all running worker tasks.


<a id="taskqueue.cancel"></a>
### cancel
```python
def cancel(self):
```
Cancels the queue task and all worker tasks.

<a id="taskqueue.watch"></a>
### watch
```python
async def watch(self):
```
Watches the queue if there is a timeout set and cancels the queue if the timeout is reached.
