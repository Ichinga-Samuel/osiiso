# Async Queue Manager

`async-queue-manager` provides both asyncio and thread-based task queues for real applications. It keeps the API simple, but adds the pieces most teams need before using a queue in production: priorities, retries, per-task timeouts, task handles, results, graceful shutdown, and continuous worker mode.

## Highlights

- Run async callables, sync callables, or already-created awaitables
- Prioritize work with an `asyncio.PriorityQueue`
- Retry idempotent tasks with optional backoff
- Apply per-task timeouts and queue-level timeouts
- Await individual task handles or collect a run summary
- Cancel pending tasks, cancel running coroutine tasks, or keep `must_complete` tasks alive during timeout handling
- Use it as a one-shot batch runner or as a long-lived background service
- Submit tasks and trigger cancellation safely from other threads
- Use `ThreadTaskQueue` for non-async applications with equivalent queue features

## Installation

```bash
pip install async-queue-manager
```

The canonical import is `async_queue`. A compatibility alias for `async_queue_manager` is also included.

```python
from async_queue import TaskQueue
```

For non-async applications:

```python
from async_queue import ThreadTaskQueue
```

## Quick Start

```python
import asyncio

from async_queue import TaskQueue


async def fetch(delay: float, name: str) -> str:
    await asyncio.sleep(delay)
    return f"finished {name}"


async def main() -> None:
    queue = TaskQueue(max_workers=2)

    first = queue.add_task(fetch, 0.2, "low-priority", priority=5)
    second = queue.add_task(fetch, 0.1, "important", priority=1)

    summary = await queue.run()

    print(summary.succeeded)
    print(second.value())
    print(first.value())


asyncio.run(main())
```

### Threaded quick start (non-async)

```python
from async_queue import ThreadTaskQueue


def process(delay: float, name: str) -> str:
    import time

    time.sleep(delay)
    return f"finished {name}"


queue = ThreadTaskQueue(max_workers=2)
first = queue.add_task(process, 0.2, "low-priority", priority=5)
second = queue.add_task(process, 0.1, "important", priority=1)
summary = queue.run()

print(summary.succeeded)
print(second.value())
print(first.value())
```

## More Useful Features

### Retries and timeouts

```python
import asyncio
from async_queue import TaskQueue


attempts = {"count": 0}


async def flaky_call() -> str:
    attempts["count"] += 1
    if attempts["count"] < 3:
        raise RuntimeError("temporary failure")
    await asyncio.sleep(0.05)
    return "ok"


async def main() -> None:
    queue = TaskQueue(max_workers=1)
    handle = queue.add_task(
        flaky_call,
        retries=2,
        retry_delay=0.1,
        backoff=2.0,
        timeout=1.0,
        name="flaky-call",
    )

    await queue.run()
    print(handle.attempts)
    print(handle.value())


asyncio.run(main())
```

### Long-lived service mode

```python
import asyncio
from async_queue import TaskQueue


def double(value: int) -> int:
    return value * 2


async def main() -> None:
    async with TaskQueue(mode="infinite", max_workers=2) as queue:
        handles = queue.map(double, [1, 2, 3, 4])
        await queue.join()
        print([handle.value() for handle in handles])


asyncio.run(main())
```

### Queue timeout policy

`on_exit="cancel"` cancels pending work and requests cancellation for running work when the queue timeout expires.

`on_exit="complete_priority"` keeps only tasks marked with `must_complete=True` and drops the rest.

```python
queue = TaskQueue(on_exit="complete_priority")
queue.add_task(send_invoice, must_complete=True)
queue.add_task(send_metrics)
summary = await queue.run(queue_timeout=5.0)
```

## Public API

### `TaskQueue(...)`

- `size`: queue capacity. `0` means unbounded.
- `max_workers`: fixed worker count. Leave as `None` to auto-scale up to a safe cap.
- `queue_timeout`: default timeout for `run()`.
- `on_exit`: `"cancel"` or `"complete_priority"`.
- `mode`: `"finite"` for batch runs or `"infinite"` for service mode.
- `raise_on_error`: raise `QueueExecutionError` when `run()` finishes with failures.

### `TaskQueue.add_task(...)` and `TaskQueue.submit(...)`

- Accept async callables, sync callables, or awaitables
- Return a `TaskHandle`
- Support `priority`, `must_complete`, `timeout`, `retries`, `retry_delay`, `backoff`, and `name`

### `TaskQueue.map(task, iterable, ...)`

- Submit a batch in one call
- Tuple entries are expanded as positional args
- Mapping entries are expanded as keyword args

### `TaskHandle`

- `await handle` or `await handle.wait()` for the final `TaskResult`
- `handle.value()` for the successful return value
- `handle.cancel()` to cancel an individual pending or running task
- `handle.status`, `handle.attempts`, `handle.done()`, and `handle.cancelled()`

### `QueueRunSummary`

- `total_submitted`
- `succeeded`
- `failed`
- `cancelled`
- `timed_out`
- `duration`
- `results`

### `ThreadTaskQueue(...)`

- Same queue options and task options as `TaskQueue`
- Synchronous lifecycle methods: `start()`, `join()`, `run()`, `shutdown()`
- Use with a standard context manager: `with ThreadTaskQueue(...) as queue: ...`

### `ThreadTaskHandle` and `ThreadQueueRunSummary`

- `ThreadTaskHandle.wait(timeout=None)` blocks for completion
- `ThreadTaskHandle.value()` returns the successful value
- `ThreadTaskHandle.cancel()` cancels pending work and requests cancellation for running work
- `ThreadQueueRunSummary` mirrors `QueueRunSummary` fields

## Development

```bash
pip install -e .[dev]
ruff check .
pytest
python -m build
```

## Notes

- Retries are safest for idempotent tasks.
- Cancelling a sync callable that is already running in a thread is best-effort only; the Python task is cancelled immediately, but the underlying thread cannot be force-stopped safely.
- In `ThreadTaskQueue`, cancellation and timeout for already-running tasks are cooperative; queue state is updated immediately while underlying work may still finish in the background.

## License

MIT. See [LICENSE](LICENSE).
