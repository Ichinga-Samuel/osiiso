# QueueItem

## Table of Contents
- [QueueItem](#queueitem.queueitem)
- [__call__](#queueitem.__call__)


### QueueItem
<a id="queueitem.queueitem"></a>
```python
class QueueItem
```
A class to represent a task item in the queue

#### Attributes
| Name            | Type                    | Description                                                          | Default |
|-----------------|-------------------------|----------------------------------------------------------------------|---------|
| `task_item`     | `Callable \| Coroutine` | The task to run.                                                     |         |
| `args`          | `tuple`                 | The arguments to pass to the task                                    |         |
| `kwargs`        | `dict`                  | The keyword arguments to pass to the task                            |         |
| `must_complete` | `bool`                  | A flag to indicate if the task must complete before the queue stops. | `False` |
| `time`          | `int`                   | The time the task was added to the queue.                            |         |
| `timeout`       | `int`                   | An optional timeout for the task                                     | `None`  |


<a id="queueitem.__call__"></a>
```python
async def __call__()
```
Run the task asynchronously, non-async coroutines are run with `asyncio.to_thread`.
Tasks are run with a timeout if specified.
