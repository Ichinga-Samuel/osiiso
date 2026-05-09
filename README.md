# taskqueue

Structured concurrency and parallelism for Python — using **asyncio**, **threading**, and **multiprocessing** through a unified, compact queue API.

```python
import taskqueue

async def main():
    async with taskqueue.TaskQueue(workers=4) as q:
        q.submit(fetch, "https://a.com", retries=3)
        q.submit(fetch, "https://b.com", retries=3)
        summary = await q.run()
        print(summary.values)

taskqueue.run(main())  # uses uvloop if installed
```

## Features

- **Three execution backends** — `TaskQueue` (asyncio), `ThreadTaskQueue` (threads), `ProcessTaskQueue` (processes)
- **Priority scheduling** — tasks execute in priority order
- **Automatic retries** — with configurable delay and exponential backoff
- **Per-task timeouts** and global queue timeouts
- **Graceful shutdown** — with must-complete task protection
- **`TaskOptions`** — reusable config bundles to eliminate verbose kwargs
- **`@q.task()` decorator** — bind functions to a queue with preset options
- **Event hooks** — `on_task_start`, `on_task_complete`, `on_task_retry`
- **`taskqueue.run()`** — auto-detects and uses uvloop when available
- **Fully typed** — ships with `py.typed` and complete type annotations

## Installation

```bash
pip install taskqueue

# With uvloop support (Linux/macOS):
pip install taskqueue[uvloop]
```

## Quick Start

### Asyncio

```python
import asyncio
import taskqueue

async def fetch(url):
    await asyncio.sleep(0.1)  # simulate I/O
    return f"fetched {url}"

async def main():
    async with taskqueue.TaskQueue(workers=4) as q:
        q.submit(fetch, "https://a.com")
        q.submit(fetch, "https://b.com")
        summary = await q.run()

    print(summary.values)   # ('fetched https://a.com', 'fetched https://b.com')
    print(summary.ok)       # True

taskqueue.run(main())
```

### Threading

```python
import taskqueue
import time

def process(data):
    time.sleep(0.1)
    return data.upper()

with taskqueue.ThreadTaskQueue(workers=4) as q:
    q.map(process, ["hello", "world", "foo"])
    summary = q.run()

print(summary.values)  # ('HELLO', 'WORLD', 'FOO')
```

### Multiprocessing

```python
import taskqueue

def cpu_work(n):
    return sum(range(n))

with taskqueue.ProcessTaskQueue(workers=4) as q:
    q.map(cpu_work, [10**6, 10**7, 10**8])
    summary = q.run()

print(summary.values)
```

## API Reference

### TaskOptions

Reusable, immutable configuration bundle. Pass as `opts=` or use inline kwargs.

```python
from taskqueue import TaskOptions

# Create reusable options
retry_opts = TaskOptions(retries=3, retry_delay=1.0, backoff=2.0, timeout=30)

# Use them
q.submit(fetch, url, opts=retry_opts)

# Or inline — same effect:
q.submit(fetch, url, retries=3, retry_delay=1.0, backoff=2.0, timeout=30)

# Derive new options from existing ones:
urgent = retry_opts.replace(priority=1)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `priority` | `int` | `3` | Lower = higher priority |
| `must_complete` | `bool` | `False` | Protected from cancellation during shutdown |
| `timeout` | `float \| None` | `None` | Per-task timeout in seconds |
| `retries` | `int` | `0` | Max retry attempts after failure |
| `retry_delay` | `float` | `0.0` | Delay before first retry (seconds) |
| `backoff` | `float` | `1.0` | Multiplier applied to retry_delay after each retry |
| `delay` | `float \| None` | `None` | Delay before execution starts |
| `run_at` | `float \| None` | `None` | Absolute time (epoch) to start execution |
| `name` | `str \| None` | `None` | Custom task name (auto-detected from function) |
| `group_id` | `str \| None` | `None` | Group identifier |
| `detached` | `bool` | `False` | Fire-and-forget mode |

### Queue Constructors

All three queue types share these constructor parameters:

```python
TaskQueue(
    workers=None,        # Max concurrent workers (auto-scaled if None)
    size=0,              # Queue capacity (0 = unbounded)
    timeout=None,        # Default run() timeout
    mode="finite",       # "finite" (drain & stop) or "infinite" (run until shutdown)
    fail_policy="continue",   # "continue" or "fail_first"
    on_exit="complete_priority",  # "cancel" or "complete_priority"
    on_task_start=None,       # Callback(handle)
    on_task_complete=None,    # Callback(result)
    on_task_retry=None,       # Callback(handle, exception)
)
```

`ThreadTaskQueue` and `ProcessTaskQueue` additionally accept `poll_interval` (default `0.05`).
`ProcessTaskQueue` additionally accepts `context` for multiprocessing context.

### Submit Methods

```python
# Single task
handle = q.submit(fn, arg1, arg2, retries=3)

# Map over iterable
handles = q.map(fn, [1, 2, 3], opts=retry_opts)

# Group (returns a group handle)
group = q.group(fn, [1, 2, 3], group_id="batch-1")
```

For task keyword arguments, use `functools.partial`:

```python
from functools import partial
q.submit(partial(fetch, headers={"Auth": "token"}), url)
```

### Handles

**`TaskHandle`** (async) — returned by `TaskQueue.submit()`:
```python
result = await handle          # await completion
result = await handle.wait()   # same, explicit
handle.value()                 # return value or raise
handle.cancel()                # request cancellation
handle.done()                  # check completion
handle.status                  # "pending" | "running" | "retrying" | "succeeded" | "failed" | "cancelled"
```

**`SyncTaskHandle`** (blocking) — returned by `ThreadTaskQueue.submit()` and `ProcessTaskQueue.submit()`:
```python
result = handle.wait()         # block until complete
result = handle.wait(timeout=5)
handle.value()
handle.cancel()
```

### Groups

```python
# Async
group = q.group(fn, items)
summary = await group.wait()
values = await group.values()  # raises on errors
group.cancel()

# Sync (thread/process)
summary = group.wait(timeout=30)
values = group.values()
```

### Lifecycle

```python
# Context manager (recommended)
async with TaskQueue(workers=4) as q:
    q.submit(work, 1)
    summary = await q.run()

# Manual lifecycle
q = TaskQueue(workers=4)
await q.start()
q.submit(work, 1)
summary = await q.run()
await q.shutdown()

# Reset for reuse
q.reset()
```

### `@q.task()` Decorator

Bind a function to a queue with preset options:

```python
q = TaskQueue(workers=4)

@q.task(retries=3, timeout=10)
async def fetch(url):
    ...

# Submit single task
handle = fetch("https://example.com")

# Map over iterable
handles = fetch.map(["url1", "url2", "url3"])

# Group
group = fetch.group(["url1", "url2"])

# Override options at call time
handle = fetch("url", priority=1)
```

### `taskqueue.run()`

Convenience runner with automatic uvloop detection:

```python
import taskqueue

# Auto-detect uvloop (use if installed, fallback to stdlib)
result = taskqueue.run(main())

# Force uvloop (raises ImportError if not installed)
result = taskqueue.run(main(), use_uvloop=True)

# Force stdlib asyncio
result = taskqueue.run(main(), use_uvloop=False)
```

### Event Hooks

```python
q = TaskQueue(
    workers=4,
    on_task_start=lambda handle: print(f"▶ {handle.name}"),
    on_task_complete=lambda result: print(f"✓ {result.name}: {result.status}"),
    on_task_retry=lambda handle, exc: print(f"⟳ {handle.name}: {exc}"),
)
```

### QueueRunSummary

Returned by `q.run()`:

```python
summary = await q.run()

summary.ok              # True if no failures/cancellations/timeouts
summary.succeeded       # count
summary.failed          # count
summary.cancelled       # count
summary.timed_out       # bool
summary.duration        # seconds
summary.values          # tuple of successful return values
summary.errors          # tuple of failed TaskResults

summary.by_name()       # dict[name, tuple[TaskResult, ...]]
summary.by_group()      # dict[group_id, tuple[TaskResult, ...]]
summary.by_task_id()    # dict[task_id, TaskResult]
summary.raise_for_errors()  # raises QueueExecutionError if any failures
```

## License

MIT
