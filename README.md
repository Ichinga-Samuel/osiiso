# Asynchronous Task Queue Manager

![PyPI](https://img.shields.io/pypi/v/async-queue-manager)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An efficient and robust asynchronous task queue for Python, built on top of `asyncio`. It's designed for handling concurrent tasks with support for prioritization, dynamic worker scaling, and graceful shutdowns.

## Key Features

-   **Asynchronous First:** Built from the ground up using Python's modern `asyncio` library for high-performance I/O-bound tasks.
-   **Priority Queues:** Supports task prioritization out of the box. Lower priority numbers are processed first.
-   **Dynamic Worker Management:** Automatically scales worker tasks based on queue size to efficiently process jobs.
-   **Sync & Async Task Support:** Seamlessly handles both `async` coroutines and regular synchronous functions.
-e   **Timeout Control:** Set a global timeout for the queue to prevent it from running indefinitely.
-   **Graceful Shutdown:** Configure how the queue handles pending tasks on exit—either cancel them immediately or complete high-priority ones before stopping.
-   **Multiple Modes:** Run in `finite` mode for a set batch of tasks or `infinite` mode for long-running services that continuously process tasks.

## Installation

You can install the package from PyPI:

```bash
pip install async-queue-manager
```

## Basic Usage

Here's how to get started with the `TaskQueue` in just a few lines of code.

```python
import asyncio
import time

# 1. Import the TaskQueue
from async_queue_manager import TaskQueue

# 2. Define some tasks (can be async or regular functions)
async def async_task(duration, name):
    """An example asynchronous task."""
    print(f"Starting async task: {name}")
    await asyncio.sleep(duration)
    print(f"✅ Finished async task: {name}")

def sync_task(duration, name):
    """An example synchronous task."""
    print(f"Starting sync task: {name}")
    time.sleep(duration)
    print(f"✅ Finished sync task: {name}")

async def main():
    # 3. Create a TaskQueue instance
    task_queue = TaskQueue()

    # 4. Add tasks to the queue
    print("Adding tasks to the queue...")
    task_queue.add_task(async_task, 1, "A (Low Prio)")
    task_queue.add_task(sync_task, 2, "B (Sync)")
    task_queue.add_task(async_task, 0.5, "C (High Prio)")

    # 5. Run the queue and wait for all tasks to complete
    print("🚀 Starting the queue...")
    start_time = time.monotonic()
    await task_queue.run()
    end_time = time.monotonic()
    
    print(f"🎉 All tasks completed in {end_time - start_time:.2f} seconds!")

if __name__ == "__main__":
    asyncio.run(main())
```

## Advanced Usage

### Task Prioritization

You can assign a `priority` to each task. Tasks with a lower number have a higher priority and will be executed first.

```python
import asyncio
from async_queue_manager import TaskQueue

async def my_task(name):
    print(f"Executing task: {name}")
    await asyncio.sleep(0.1)

async def main():
    queue = TaskQueue()

    # Add tasks with different priorities
    queue.add_task(my_task, "Task A (Priority 5)", priority=5)
    queue.add_task(my_task, "Task B (Priority 10)", priority=10)
    queue.add_task(my_task, "Task C (Priority 1)", priority=1) # Highest priority

    # The queue will execute tasks in this order: C, A, B
    await queue.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### Timeout and Shutdown Policy

You can control how the queue behaves when it times out using `queue_timeout` and `on_exit`. You can also mark critical tasks with `must_complete=True` to ensure they finish even if the queue times out.

-   `on_exit='complete_priority'` (default): When the timeout is reached, the queue stops accepting new tasks but will wait for any tasks marked `must_complete=True` to finish. Other tasks are cancelled.
-   `on_exit='cancel'`: When the timeout is reached, the queue immediately cancels all running and pending tasks.

```python
import asyncio
from async_queue_manager import TaskQueue

async def long_running_task(duration, name):
    print(f"Starting task: {name} (will run for {duration}s)")
    await asyncio.sleep(duration)
    print(f"✅ Finished task: {name}")

async def main():
    # Initialize with the 'complete_priority' shutdown policy
    queue = TaskQueue(on_exit='complete_priority')

    # This task will likely be cancelled by the timeout
    queue.add_task(long_running_task, 4, "Normal Task")

    # This task will be allowed to finish because must_complete is True
    queue.add_task(long_running_task, 4, "Critical Task", must_complete=True)

    print("Running queue with a 2-second timeout...")
    await queue.run(queue_timeout=2)
    print("Queue has finished or timed out.")
    # Expected output will show "Critical Task" finishing after the timeout is announced.

if __name__ == "__main__":
    asyncio.run(main())
```

## API Reference

### `TaskQueue(...)`

The main class for managing the queue.

| Parameter       | Type                                | Description                                                                     | Default               |
|-----------------|-------------------------------------|---------------------------------------------------------------------------------|-----------------------|
| `size`          | `int`                               | The maximum size of the queue. `0` means infinite.                              | `0`                   |
| `queue_timeout` | `int`                               | Default timeout in seconds for the queue when `run()` is called.                | `0` (no timeout)      |
| `on_exit`       | `'cancel'` or `'complete_priority'` | The policy for handling tasks on shutdown or timeout.                           | `'complete_priority'` |
| `mode`          | `'finite'` or `'infinite'`          | `'finite'` stops when empty; `'infinite'` keeps the queue running indefinitely. | `'finite'`            |

### `TaskQueue.add_task(...)`

Adds a new task to the queue.

| Parameter           | Type                   | Description                                                                                | Default |
|---------------------|------------------------|--------------------------------------------------------------------------------------------|---------|
| `task`              | `Callable` `Coroutine` | The async or sync function to execute.                                                     |         |
| `*args`, `**kwargs` | `Any`                  | Arguments to pass to the task function.                                                    |         |
| `must_complete`     | `bool`                 | If `True`, task is completed even if queue times out (with `on_exit='complete_priority'`). | `False` |
| `priority`          | `int`                  | The priority of the task. A lower number means higher priority.                            | `3`     |

### `await TaskQueue.run(...)`

Starts the queue workers and waits for tasks to complete.

| Parameter       | Type  | Description                                          | Default |
|-----------------|-------|------------------------------------------------------|---------|
| `queue_timeout` | `int` | Overrides the default timeout for this specific run. | `None`  |


## Contributing

Contributions are welcome! If you'd like to contribute, please follow these steps:

1.  Fork the repository.
2.  Clone your fork and set up the development environment:
    ```bash
    git clone [https://github.com/YOUR_USERNAME/taskqueue.git](https://github.com/YOUR_USERNAME/taskqueue.git)
    cd taskqueue
    pip install -e .[dev]
    ```
3.  Make your changes and add tests for them.
4.  Run the tests to ensure everything is working:
    ```bash
    pytest
    ```
5.  Submit a pull request with a clear description of your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.