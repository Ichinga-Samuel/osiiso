# Async Queue Manager

`async-queue-manager` provides queue-driven structured concurrency for asyncio,
threading, and multiprocessing workloads. It is built around the same ergonomic
API across all backends: submit independent tasks, schedule timed work, group
related tasks, collect structured results, and choose whether failures should
stop the queue or be collected and reported.

## Installation

```bash
pip install async-queue-manager
```

```python
from async_queue import ProcessTaskQueue, TaskQueue, ThreadTaskQueue
```

## Quick Start

```python
import asyncio

from async_queue import TaskQueue


async def fetch(name: str) -> str:
    await asyncio.sleep(0.1)
    return f"done {name}"


async def main() -> None:
    queue = TaskQueue(max_workers=4, fail_policy="fail_first")

    group = queue.group(fetch, ["a", "b", "c"], group_id="fetches")
    queue.fire_and_forget(fetch, "audit", delay=0.25)

    await queue.start()
    summary = await group.wait()
    await queue.shutdown()

    print(summary.ok)
    print(summary.values)
    print(summary.by_group()["fetches"])


asyncio.run(main())
```

## Backends

### `TaskQueue`

Use `TaskQueue` for asyncio applications. It accepts async callables, sync
callables, and awaitable objects. Sync callables run via `asyncio.to_thread`.

### `ThreadTaskQueue`

Use `ThreadTaskQueue` from non-async code when you want parallel I/O-bound or
blocking work with the same queue semantics.

### `ProcessTaskQueue`

Use `ProcessTaskQueue` for CPU-bound or process-isolated work. Process tasks and
arguments must be pickleable for the active multiprocessing start method. On
Windows and macOS, define process tasks at module scope so child processes can
import them.

## Core Features

- `submit(...)` / `add_task(...)`: enqueue independent work and get a task handle
- `map(...)`: submit a batch from scalar, tuple, or mapping inputs
- `group(...)` / `submit_group(...)`: submit related work and wait on it as a unit
- `fire_and_forget(...)`: submit detached background work while still preserving errors in queue results
- `background(...)`: start the queue if needed and submit detached work immediately
- `delay=...` / `run_at=...`: schedule timed tasks
- `timeout=...`: apply per-task execution timeouts
- `queue_timeout=...`: bound a whole queue run
- `retries=...`, `retry_delay=...`, `backoff=...`: retry idempotent work
- `fail_policy="continue"`: collect failures and keep processing
- `fail_policy="fail_first"`: cancel pending/running work after the first failure
- `on_exit="cancel"` or `"complete_priority"`: choose timeout shutdown behavior
- `mode="finite"` or `"infinite"`: use the queue as a batch runner or service

## Results

Every backend returns the shared `TaskResult` and `QueueRunSummary` types.

```python
summary = queue.run()

summary.ok
summary.values
summary.errors
summary.by_task_id()
summary.by_name()
summary.by_group()
```

Task handles expose:

```python
handle.status
handle.attempts
handle.done()
handle.cancel()
handle.result()
handle.value()
```

Async handles can be awaited directly. Thread and process handles use
`handle.wait(timeout=None)`.

## Development

```bash
pip install -e .[dev]
ruff check src tests
pytest
python -m build
```

## License

MIT. See [LICENSE](LICENSE).
