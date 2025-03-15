# AsyncQueue

## Overview

`async-queue-manager` is a Python library designed to manage and execute tasks concurrently using asyncio. 
It supports task prioritization, robust error handling, and efficient queue management for both finite 
and infinite task processing modes.

---

## Features

- **QueueItem**:
  - Represents individual tasks with arguments and prioritization support.
  - Supports both synchronous and asynchronous task execution.
  - Optional timeout argument

- **TaskQueue**:
  - Manages tasks with configurable workers and prioritization.
  - Can take different type of asyncio queues
  - Run for a specific number of seconds
  - Modes:
    - `finite`: Process tasks and stop.
    - `infinite`: Continuously process tasks until stopped.
  - Handles graceful shutdowns, including signal handling (e.g., SIGINT).

---

## Installation

Ensure you have Python 3.10+ installed.

```bash
pip install async-queue-manager  
```

### Example

```python
import asyncio
from async_queue.task_queue import TaskQueue
from async_queue.queue_item import QueueItem

async def my_task(name, duration):
    print(f"{name} is starting...")
    await asyncio.sleep(duration)
    print(f"{name} is finished.")

async def main():
    queue = TaskQueue(workers=3, mode='finite')

    # Add tasks
    for i in range(5):
        task = QueueItem(my_task, name=f"Task-{i}", duration=2)
        queue.add(item=task, priority=i)

    # Run the queue
    await queue.run(queue_timeout=10)

# Run the program
asyncio.run(main())
```
---
## Project Structure

- **`queue_item.py`**: Implements the `QueueItem` class.
- **`task_queue.py`**: Contains the `TaskQueue` class for task management.

---

## Contributions

Contributions are welcome!

- Report bugs or issues.
- Submit feature requests or enhancements via pull requests.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Documentation

See the [documentation](./docs) for more details.
