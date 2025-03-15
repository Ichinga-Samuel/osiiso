# Task Queue

## Table of Contents
- [TaskQueue](#taskqueue.taskqueue)
- [add](#taskqueue.add)
- [worker](#taskqueue.worker)
- [start_timer](#taskqueue.start_timer)
- [check_timeout](#taskqueue.check_timeout)
- [dummy_task](#taskqueue.dummy_task)
- [add_worker](#taskqueue.add_workers)
- [run](#taskqueue.run)
- [cancel_all_workers](#taskqueue.cancel_all_workers)
- [cancel](#taskqueue.cancel)

<a id="taskqueue.taskqueue"></a>
### TaskQueue
```python
class TaskQueue
```
TaskQueue is a class that allows you to queue tasks and run them concurrently with a
specified number of workers.

#### Attributes:
| Name                   | Type                                   | Description                                                                                                                                      | Default                                    |
|------------------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `workers`              | int                                    | The number of workers to run concurrently.                                                                                                       | 10                                         |
| `absolute_timeout`     | int                                    | The maximum time to wait for the queue to complete.                                                                                              | None                                       |
| `queue_timeout`        | int                                    | The maximum time to run the queue, after queue_timeout, new tasks are not added but the queue might be allowed to run until completion.          | None                                       |
| `queue`                | asyncio.Queue                          | The queue to store the tasks.                                                                                                                    | `asyncio.PriorityQueue` with no size limit |
| `on_exit`              | Literal["cancel", "complete_priority"] | The action to take when the queue is stopped.                                                                                                    | complete_priority                          |
| `mode`                 | Literal["finite", "infinite"]          | The mode of the queue. If `finite` the queue will stop when all tasks are completed. If `infinite` the queue will continue to run until stopped. | finite                                     |
| `worker_timeout`       | float                                  | The time to wait for a task to be added to the queue before stopping the worker or adding a dummy sleep task to the queue.                       | 1                                          |
| `absolute_timeout`     | float                                  | The absolute length of time the queue will run. If specified the queue will stop running No matter the number of pending tasks remaining.        | None                                       |
| `queue_timeout`        | float                                  | If specified the queue will stop accepting tasks at this point, but will try and complete pending tasks.                                         | None                                       |
| `task_timeout`         | float                                  | Specific time for each item in the queue to run                                                                                                  | None                                       |
| `queue_task_cancelled` | bool                                   | A boolean flag to indicate if the main queue task is still running                                                                               | False                                      |
| `stop`                 | bool                                   | A flag to stop the queue instance.                                                                                                               | False                                      |
| `worker_tasks`         | dict[int: asyncio.Task]                | A dict of the worker tasks running concurrently,                                                                                                 |                                            |


<a id="taskqueue.add"></a>
### add
```python
def add(*, item: QueueItem, priority=3, must_complete=False, timeout=0):
```
Add a task to the queue.

#### Parameters
| Name            | Type        | Description                                                          | Default |
|-----------------|-------------|----------------------------------------------------------------------|---------|
| `item`          | `QueueItem` | The task to add to the queue.                                        |         |
| `priority`      | `int`       | The priority of the task.                                            | `3`     |
| `must_complete` | `bool`      | A flag to indicate if the task must complete before the queue stops. | `False` |
| `timeout`       | `int`       | An optional timeout for the task.                                    | `0`     |


<a id="taskqueue.worker"></a>
### worker
```python
async def worker(wid: int = None):
```
Worker function to run tasks in the queue.

#### Parameters
| Name  | Type  | Description | Default |
|-------|-------|-------------|---------|
| `wid` | `int` | Worker ID.  | `None`  |


<a id="taskqueue.start_timer"></a>
### start_timer
```python
def start_timer(*, queue_timeout: int = None, absolute_timeout: int = None, start=False):
```
Starts the queue timers.

#### Parameters:
| Name               | Type   | Description                                                                                                                             | Default |
|--------------------|--------|-----------------------------------------------------------------------------------------------------------------------------------------|---------|
| `queue_timeout`    | `int`  | The maximum time to run the queue, after queue_timeout, new tasks are not added but the queue might be allowed to run until completion. | `None`  |
| `absolute_timeout` | `int`  | The maximum time to wait for the queue to complete.                                                                                     | `None`  |
| `start`            | `bool` | A flag to indicate if the timer should start immediately.                                                                               | `False` |


<a id="taskqueue.check_timeout"></a>
### check_timeout
```python
async def check_timeout()
```
Checks if the queue has timed out.


<a id="taskqueue.dummy_task"></a>
### dummy_task
A dummy task for worker to execute when queue is empty in infinite mode.


<a id="taskqueue.remove_worker"></a>
### remove_worker
```python
def remove_worker(wid: int):
```
Removes a worker from the worker tasks and cancels the worker task.

#### Parameters
| Name  | Type  | Description                     | Default |
|-------|-------|---------------------------------|---------|
| `wid` | `int` | The ID of the worker to remove. |         |


<a id="taskqueue.add_workers"></a>
### add_workers
```python
async def add_workers(no_of_workers: int = None):
```
Create workers for running queue tasks.

#### Parameters
| Name            | Type  | Description                            | Default |
|-----------------|-------|----------------------------------------|---------|
| `no_of_workers` | `int` | Number of workers to add to the queue. | `None`  |


<a id="taskqueue.run"></a>
### run
```python
async def run(queue_timeout: int = None, absolute_timeout: int = None):
```
Run the queue until all tasks are completed or the timeout is reached.

#### Parameters
| Name               | Type  | Description                                                                                                                                                                                                                                                                                                              | Default |
|--------------------|-------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `queue_timeout`    | `int` | The maximum time to wait for the queue to add new tasks.                                                                                                                                                                                                                                                                 | `0`     |
| `absolute_timeout` | `int` | The maximum time to run the queue. This timeout overrides the timeout attribute of the queue instance. The queue stops when the timeout is reached, and the remaining tasks are handled based on the `on_exit` attribute. If the timeout is 0, the queue will run until all tasks are completed or the queue is stopped. |         |


<a id="taskqueue.cancel_all_workers"></a>
### cancel_all_workers
```python
def cancel_all_workers():
```
Cancels all running worker tasks.


<a id="taskqueue.cancel"></a>
### cancel
```python
def cancel():
```
Cancels the queue task and all worker tasks.
