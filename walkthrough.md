# Osiiso — Comprehensive Tutorial

A complete guide to **osiiso**, a structured concurrency library providing unified task queues for **asyncio**, **threading**, and **multiprocessing**.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Installation](#installation)
3. [Core Concepts](#core-concepts)
4. [AsyncQueue (Asyncio)](#asyncqueue-asyncio)
5. [ThreadQueue](#threadqueue)
6. [ProcessQueue](#processqueue)
7. [TaskOptions](#taskoptions)
8. [Handles & Results](#handles--results)
9. [Groups](#groups)
10. [The @task Decorator](#the-task-decorator)
11. [Retries & Backoff](#retries--backoff)
12. [Timeouts](#timeouts)
13. [Fail Policies](#fail-policies)
14. [Shutdown & Lifecycle](#shutdown--lifecycle)
15. [Event Hooks](#event-hooks)
16. [Streaming with as_completed](#streaming-with-as_completed)
17. [RunSummary Deep Dive](#runsummary-deep-dive)
18. [osiiso.run() and uvloop](#osiisorun-and-uvloop)
19. [Error Handling](#error-handling)
20. [Real-World Patterns](#real-world-patterns)

---

## Architecture Overview

```
src/osiiso/
├── __init__.py        → Public API (12 exports)
├── asyncqueue.py      → AsyncQueue         (asyncio backend)
├── threadqueue.py     → ThreadQueue        (threading backend)
├── processqueue.py    → ProcessQueue       (multiprocessing backend)
├── handle.py          → TaskHandle + SyncTaskHandle
├── group.py           → TaskGroup + SyncTaskGroup
├── options.py         → TaskOptions dataclass + resolve_opts()
├── result.py          → TaskResult + RunSummary + make_result()
├── items.py           → AsyncItem, ThreadItem, ProcessItem (internal)
├── loop.py            → run() with uvloop auto-detection
├── exceptions.py      → OsiisoError, ClosedError, ExecutionError
└── py.typed           → PEP 561 marker
```

### How the pieces fit together

```
┌─────────────────────────────────────────────────────────┐
│                     User Code                           │
│  q.submit(fn, arg, retries=3)                           │
└────────────┬────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────┐     ┌──────────────┐
│  resolve_opts()     │◄────│ TaskOptions   │
│  merges opts+kwargs │     │ (frozen DC)   │
└────────┬────────────┘     └──────────────┘
         │
         ▼
┌─────────────────────┐     ┌──────────────┐
│  _enqueue()         │────►│ TaskHandle   │  ◄── returned to user
│  creates Item+Handle│     │ (awaitable)  │
└────────┬────────────┘     └──────────────┘
         │
         ▼
┌─────────────────────┐
│  PriorityQueue      │  entries sorted by (scheduled_key, priority, seq)
│  (_Entry objects)    │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐     ┌──────────────┐
│  Worker pool        │────►│ _execute()   │
│  (auto-scaled or    │     │ retry loop   │
│   fixed count)      │     │ timeout mgmt │
└─────────────────────┘     └──────┬───────┘
                                   │
                                   ▼
                            ┌──────────────┐
                            │ TaskResult   │  → stored in _results[]
                            │ (frozen DC)  │  → handle notified
                            └──────┬───────┘
                                   │
                                   ▼
                            ┌──────────────┐
                            │ RunSummary   │  ◄── returned by q.run()
                            └──────────────┘
```

**Key principle:** All three queue types (`AsyncQueue`, `ThreadQueue`, `ProcessQueue`) share the same API surface — `submit()`, `map()`, `group()`, `task()`, `run()`, `shutdown()`, `reset()`. The only difference is the execution backend and whether methods are `async` or blocking.

---

## Installation

```bash
pip install osiiso

# With uvloop acceleration (Linux/macOS only):
pip install osiiso[uvloop]
```

---

## Core Concepts

| Concept | Description |
|---------|-------------|
| **Queue** | The execution engine. You submit tasks, then call `run()` to execute them. |
| **Task** | A callable (function/coroutine) + arguments, submitted via `submit()`. |
| **Handle** | Returned by `submit()`. Used to await/wait, cancel, or inspect a task. |
| **TaskOptions** | Immutable config bundle (priority, retries, timeout, etc). |
| **TaskResult** | Immutable record of a completed task (status, value, exception, timing). |
| **RunSummary** | Aggregate summary returned by `run()` with counts and all results. |
| **Group** | A batch of handles created by `group()`, waited on together. |

---

## AsyncQueue (Asyncio)

The primary queue for I/O-bound async work: HTTP requests, database queries, file I/O.

### Basic usage

```python
import osiiso

async def fetch(url):
    # simulate async I/O
    import asyncio
    await asyncio.sleep(0.1)
    return f"fetched {url}"

async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(fetch, "https://api.example.com/users")
        q.submit(fetch, "https://api.example.com/posts")
        q.submit(fetch, "https://api.example.com/comments")
        summary = await q.run()

    print(summary.ok)      # True
    print(summary.values)  # ('fetched ...', 'fetched ...', 'fetched ...')

osiiso.run(main())
```

### Constructor parameters

```python
AsyncQueue(
    workers=None,           # Fixed worker count, or None for auto-scaling
    size=0,                 # Max queue capacity (0 = unbounded)
    timeout=None,           # Default timeout for run()
    mode="finite",          # "finite" = drain and stop, "infinite" = run until shutdown
    fail_policy="continue", # "continue" or "fail_first"
    on_exit="complete_priority",  # What to do on timeout/shutdown
    on_start=None,          # Callback fired when each task starts
    on_complete=None,       # Callback fired when each task completes
    on_retry=None,          # Callback fired before each retry
)
```

### Auto-scaling workers

When `workers=None` (the default), osiiso automatically scales the worker pool based on backlog size, bounded by `min(32, cpu_count * 4)`. This means you rarely need to tune worker counts manually.

### Submitting sync functions to async queue

`AsyncQueue` transparently handles sync callables — they're run via `asyncio.to_thread()`:

```python
import time

def slow_sync_work(n):
    time.sleep(0.5)
    return n * 2

async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(slow_sync_work, 10)  # runs in thread automatically
        summary = await q.run()
    print(summary.values)  # (20,)
```

### Map — submit one function across many inputs

```python
async def main():
    async with osiiso.AsyncQueue(workers=8) as q:
        # Scalar args — each element passed as single argument
        q.map(fetch, ["url1", "url2", "url3"])

        # Tuple args — unpacked as positional arguments
        async def download(url, dest):
            return f"{url} -> {dest}"

        q.map(download, [
            ("https://a.com", "/tmp/a"),
            ("https://b.com", "/tmp/b"),
        ])

        # Dict args — applied as keyword arguments via functools.partial
        async def request(method="GET", url=""):
            return f"{method} {url}"

        q.map(request, [
            {"method": "GET", "url": "https://a.com"},
            {"method": "POST", "url": "https://b.com"},
        ])

        summary = await q.run()
```

---

## ThreadQueue

For blocking I/O or mixed sync workloads. Same API but synchronous.

```python
import osiiso
import time

def process(data):
    time.sleep(0.1)  # simulate work
    return data.upper()

# Context manager — starts workers, shuts down cleanly
with osiiso.ThreadQueue(workers=4) as q:
    q.submit(process, "hello")
    q.submit(process, "world")
    summary = q.run()

print(summary.values)  # ('HELLO', 'WORLD')
```

### Extra parameter: `poll`

Thread and process queues accept `poll` (default `0.05` seconds) — the interval at which they check for cancellation and timeout during task execution.

```python
# Faster cancellation response at the cost of more CPU polling
q = osiiso.ThreadQueue(workers=4, poll=0.01)
```

---

## ProcessQueue

For CPU-bound work that benefits from true parallelism across cores.

```python
import osiiso

def cpu_intensive(n):
    """Must be pickleable — top-level function, no closures."""
    return sum(i * i for i in range(n))

with osiiso.ProcessQueue(workers=4) as q:
    q.map(cpu_intensive, [10**6, 10**7, 10**5])
    summary = q.run()

print(summary.values)
```

### Extra parameter: `context`

You can pass a specific multiprocessing context:

```python
import multiprocessing

ctx = multiprocessing.get_context("spawn")
q = osiiso.ProcessQueue(workers=2, context=ctx)
```

> **Important:** Tasks submitted to `ProcessQueue` must be pickleable. This means top-level functions only — no lambdas, closures, or local functions. Awaitables are explicitly rejected.

---

## TaskOptions

`TaskOptions` is a frozen dataclass that bundles all per-task configuration. Create once, reuse everywhere.

### Creating and reusing options

```python
from osiiso import TaskOptions

# Define reusable presets
retry_opts = TaskOptions(retries=3, retry_delay=1.0, backoff=2.0)
urgent_opts = TaskOptions(priority=1, timeout=5.0)
background_opts = TaskOptions(priority=9, detached=True)

# Use with submit
q.submit(fetch, url, opts=retry_opts)
q.submit(critical_task, data, opts=urgent_opts)
```

### Inline kwargs — same effect, less reuse

```python
# These are equivalent:
q.submit(fetch, url, opts=TaskOptions(retries=3, timeout=10))
q.submit(fetch, url, retries=3, timeout=10)
```

### Deriving options with replace()

```python
base = TaskOptions(retries=3, retry_delay=1.0, backoff=2.0)
urgent = base.replace(priority=1, timeout=5.0)
# urgent has retries=3, retry_delay=1.0, backoff=2.0, priority=1, timeout=5.0
```

### All fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `priority` | `int` | `3` | Lower number = higher priority |
| `must_complete` | `bool` | `False` | Protected from cancellation during shutdown |
| `timeout` | `float\|None` | `None` | Per-task timeout in seconds |
| `retries` | `int` | `0` | Max retry attempts |
| `retry_delay` | `float` | `0.0` | Seconds before first retry |
| `backoff` | `float` | `1.0` | Multiplier applied to delay after each retry |
| `delay` | `float\|None` | `None` | Delay before task starts executing |
| `run_at` | `float\|None` | `None` | Absolute epoch time to start (mutually exclusive with delay) |
| `name` | `str\|None` | `None` | Custom name (auto-detected from function if None) |
| `group_id` | `str\|None` | `None` | Group identifier |
| `detached` | `bool` | `False` | Fire-and-forget flag |

### Validation

TaskOptions validates on creation:

```python
TaskOptions(timeout=-1)        # ValueError: timeout must be > 0
TaskOptions(retries=-1)        # ValueError: retries must be >= 0
TaskOptions(backoff=0)         # ValueError: backoff must be > 0
TaskOptions(delay=1, run_at=1) # ValueError: delay and run_at are mutually exclusive
```

### Typo protection

Unknown keyword arguments to `submit()` raise immediately:

```python
q.submit(fn, arg, retrise=3)  # TypeError: Unknown task option(s): retrise
```

---

## Handles & Results

### TaskHandle (async)

Returned by `AsyncQueue.submit()`. It's **awaitable**.

```python
async with osiiso.AsyncQueue(workers=2) as q:
    handle = q.submit(fetch, "https://example.com")

    # Await directly
    result = await handle

    # Or explicitly
    result = await handle.wait()

    # Inspect
    print(handle.status)     # "succeeded" | "failed" | "cancelled" | ...
    print(handle.done())     # True
    print(handle.attempts)   # 1
    print(handle.name)       # "fetch"
    print(handle.task_id)    # "a1b2c3d4..."

    # Get return value (raises if failed/cancelled)
    value = handle.value()

    # Cancel a running/pending task
    handle.cancel()
```

### SyncTaskHandle (blocking)

Returned by `ThreadQueue.submit()` and `ProcessQueue.submit()`.

```python
with osiiso.ThreadQueue(workers=2) as q:
    handle = q.submit(work, data)

    # Block until complete
    result = handle.wait()
    result = handle.wait(timeout=5.0)  # with timeout

    # Same inspection API
    print(handle.status)
    print(handle.value())
    handle.cancel()
```

### TaskResult

Both handle types resolve to a `TaskResult` — a frozen dataclass:

```python
result = await handle.wait()  # or handle.wait() for sync

result.task_id        # unique ID
result.name           # function name
result.status         # "succeeded" | "failed" | "cancelled"
result.value          # return value (None if failed)
result.exception      # exception object (None if succeeded)
result.attempts       # how many times it ran
result.priority       # priority level
result.must_complete  # was it protected?
result.group_id       # group membership
result.created_at     # when submitted (perf_counter)
result.started_at     # when first executed
result.finished_at    # when completed
result.duration       # execution time in seconds
result.message        # human-readable status message
```

---

## Groups

Groups let you submit a batch of **heterogeneous** tasks and wait on them collectively. Each entry is a tuple of `(callable, *args)`, so different functions can be grouped together.

### Async groups

```python
async def fetch(url): ...
async def parse(html): ...
async def save(record, db): ...

async with osiiso.AsyncQueue(workers=8) as q:
    group = q.group([
        (fetch, "https://api.example.com/data"),
        (parse, raw_html),
        (save, record, "postgres"),
    ])

    print(len(group))       # 3
    print(group.group_id)   # auto-generated or custom

    # Wait for all tasks in the group
    summary = await group.wait()
    print(summary.ok)
    print(summary.values)

    # Or get values directly (raises ExecutionError if any fail)
    values = await group.values()
```

### Sync groups

```python
with osiiso.ThreadQueue(workers=4) as q:
    group = q.group([
        (process, "a"),
        (validate, "b"),
        (save, "c", db),
    ])
    summary = group.wait(timeout=30)
    values = group.values()
```

### Same-function groups

When all tasks share the same callable, each entry is still `(fn, *args)`:

```python
group = q.group([
    (fetch, "url1"),
    (fetch, "url2"),
    (fetch, "url3"),
])
```

For the single-callable pattern, `map()` is more concise — see below.

### Custom group IDs

```python
group = q.group([
    (fetch, url) for url in urls
], group_id="batch-2025-05-09")
```

### Cancel an entire group

```python
group.cancel()  # returns count of successfully cancelled tasks
```

### Groups vs. map()

| | `map()` | `group()` |
|---|---|---|
| **Callables** | Single function across all inputs | Different functions per entry |
| **Input format** | Iterable of args | Iterable of `(fn, *args)` tuples |
| **Returns** | `list[Handle]` | `TaskGroup` / `SyncTaskGroup` |
| **Collective wait** | No | Yes — `wait()`, `values()`, `cancel()` |

---

## The @task Decorator

Bind a function to a queue with preset options:

```python
q = osiiso.AsyncQueue(workers=4)

@q.task(retries=3, timeout=10, priority=2)
async def fetch(url):
    import asyncio
    await asyncio.sleep(0.1)
    return f"fetched {url}"

# Now calling fetch() submits to the queue and returns a handle:
handle = fetch("https://example.com")

# Map over inputs:
handles = fetch.map(["url1", "url2", "url3"])

# Create a group (bound task wraps args automatically):
group = fetch.group(["url1", "url2"])

# Override options at call time:
handle = fetch("url", priority=1)  # override priority for this call

# Run the queue
summary = await q.run()
```

Works identically for sync queues:

```python
q = osiiso.ThreadQueue(workers=4)

@q.task(retries=2)
def process(data):
    return data.upper()

handle = process("hello")
summary = q.run()
```

---

## Retries & Backoff

### Basic retries

```python
q.submit(flaky_api_call, url, retries=3)
# Attempts: 1 (initial) + up to 3 retries = 4 total attempts max
```

### Retry with delay

```python
q.submit(flaky_api_call, url, retries=3, retry_delay=1.0)
# Wait 1 second between each retry
```

### Exponential backoff

```python
q.submit(flaky_api_call, url, retries=5, retry_delay=0.5, backoff=2.0)
# Delays: 0.5s, 1.0s, 2.0s, 4.0s, 8.0s
```

### Monitoring retries with on_retry

```python
q = osiiso.AsyncQueue(
    workers=4,
    on_retry=lambda handle, exc: print(f"⟳ Retrying {handle.name} (attempt {handle.attempts}): {exc}")
)
```

---

## Timeouts

### Per-task timeout

```python
q.submit(slow_work, data, timeout=5.0)
# Task fails with TimeoutError after 5 seconds
```

### Queue-level timeout (on run)

```python
summary = await q.run(timeout=30.0)
# Entire run() limited to 30 seconds
print(summary.timed_out)  # True if time ran out
```

### Constructor-level default timeout

```python
q = osiiso.AsyncQueue(workers=4, timeout=60.0)
# All run() calls default to 60s unless overridden
```

### What happens on timeout

Controlled by `on_exit`:

- `"complete_priority"` (default): Cancel non-`must_complete` tasks, let `must_complete` tasks finish
- `"cancel"`: Cancel everything immediately

```python
q = osiiso.AsyncQueue(workers=4, on_exit="cancel")
q.submit(work, data, must_complete=True)  # still protected even with "cancel" on_exit
```

---

## Fail Policies

### `"continue"` (default)

Failed tasks are recorded but other tasks keep running:

```python
q = osiiso.AsyncQueue(workers=4, fail_policy="continue")
q.submit(always_fails)
q.submit(always_succeeds)
summary = await q.run()
# summary.failed == 1, summary.succeeded == 1
```

### `"fail_first"`

First failure cancels all remaining tasks:

```python
q = osiiso.AsyncQueue(workers=4, fail_policy="fail_first")
q.submit(always_fails)
q.submit(slow_work, 10)  # cancelled when first task fails
summary = await q.run()
```

### Override per-run

```python
summary = await q.run(fail_policy="fail_first")  # override just for this run
```

---

## Shutdown & Lifecycle

### Context manager (recommended)

```python
# Async
async with osiiso.AsyncQueue(workers=4) as q:
    q.submit(work, 1)
    summary = await q.run()
# shutdown() called automatically — force=True if exception occurred

# Sync
with osiiso.ThreadQueue(workers=4) as q:
    q.submit(work, 1)
    summary = q.run()
```

### Manual lifecycle

```python
q = osiiso.AsyncQueue(workers=4)
await q.start()
q.submit(work, 1)
summary = await q.run()
await q.shutdown()        # graceful: wait for all tasks
await q.shutdown(force=True)  # immediate: cancel everything
```

### Reset for reuse

```python
q = osiiso.AsyncQueue(workers=4)
q.submit(work, 1)
await q.run()
q.reset()       # clears results, reopens for submissions
q.submit(work, 2)
await q.run()
```

### clear_results()

```python
# Free memory without full reset
q.clear_results()
```

### cancel()

Emergency cancellation from any context:

```python
q.cancel()  # triggers force shutdown
```

### Queue stats

```python
print(q.stats)
# {'pending': 5, 'active': 2, 'completed': 10, 'workers': 4, 'closed': False}
print(q.active_count)
print(q.pending_count)
print(q.closed)
```

---

## Event Hooks

Three hooks for observability:

```python
q = osiiso.AsyncQueue(
    workers=4,
    on_start=lambda handle: print(f"▶ Started: {handle.name}"),
    on_complete=lambda result: print(f"{'✓' if result.status == 'succeeded' else '✗'} {result.name}: {result.status}"),
    on_retry=lambda handle, exc: print(f"⟳ {handle.name} retry #{handle.attempts}: {exc}"),
)
```

Hooks are called synchronously. Exceptions in hooks are logged but don't crash the queue.

### Use case: progress tracking

```python
import threading

completed = 0
lock = threading.Lock()

def track(result):
    global completed
    with lock:
        completed += 1
        print(f"\rProgress: {completed}/100", end="", flush=True)

q = osiiso.ThreadQueue(workers=8, on_complete=track)
q.map(process, range(100))
q.run()
```

---

## Streaming with as_completed

Process results as they arrive (async only):

```python
async with osiiso.AsyncQueue(workers=8) as q:
    handles = q.map(fetch, urls)
    await q.start()

    async for handle in osiiso.AsyncQueue.as_completed(handles):
        result = handle.result()
        if result.status == "succeeded":
            print(f"Got: {result.value}")
        else:
            print(f"Failed: {result.exception}")

    await q.shutdown()
```

---

## RunSummary Deep Dive

`run()` returns a `RunSummary`:

```python
summary = await q.run()

# Quick checks
summary.ok           # True if no failures, cancellations, or timeouts
summary.timed_out    # True if run() hit its timeout
summary.duration     # Total wall-clock time

# Counts
summary.total_submitted
summary.succeeded
summary.failed
summary.cancelled

# Access results
summary.values          # tuple of return values from succeeded tasks
summary.errors          # tuple of TaskResults with status "failed"
summary.results         # all TaskResults
summary.successes()     # tuple of succeeded TaskResults
summary.cancellations() # tuple of cancelled TaskResults

# Group/filter results
summary.by_name()       # dict[name, tuple[TaskResult, ...]]
summary.by_group()      # dict[group_id, tuple[TaskResult, ...]]
summary.by_task_id()    # dict[task_id, TaskResult]

# Strict mode — raise on errors
summary.raise_for_errors()  # raises ExecutionError if any failures
```

### Pretty-print with display()

```python
summary.display()
```

Outputs a clean, readable summary:

```
────────────────────────────────────────
  Run Summary: PASSED
────────────────────────────────────────
  Total tasks : 5
  Succeeded   : 5
  Failed      : 0
  Cancelled   : 0
  Duration    : 1.234s
────────────────────────────────────────
```

When there are failures, a details section is appended:

```
────────────────────────────────────────
  Run Summary: COMPLETED WITH ERRORS
────────────────────────────────────────
  Total tasks : 5
  Succeeded   : 3
  Failed      : 2
  Cancelled   : 0
  Duration    : 2.017s
────────────────────────────────────────
  Failures:
    • fetch (3 attempts)
      Connection refused
    • parse
      Invalid JSON at line 42
────────────────────────────────────────
```

The status line shows `PASSED`, `COMPLETED WITH ERRORS`, or `TIMED OUT`.

### Strict run

```python
# These are equivalent:
summary = await q.run(strict=True)  # raises ExecutionError on failures
# vs.
summary = await q.run()
summary.raise_for_errors()
```

---

## osiiso.run() and uvloop

A convenience runner that auto-detects uvloop:

```python
import osiiso

async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(work, 1)
        return await q.run()

# Auto-detect: uses uvloop if installed, stdlib otherwise
summary = osiiso.run(main())

# Force uvloop (raises ImportError if not installed)
summary = osiiso.run(main(), use_uvloop=True)

# Force stdlib asyncio
summary = osiiso.run(main(), use_uvloop=False)

# Enable debug mode
summary = osiiso.run(main(), debug=True)
```

---

## Error Handling

### Exception hierarchy

```
OsiisoError (base)
├── ClosedError      — submitted to a closed/shutting-down queue
└── ExecutionError   — one or more tasks failed (raised by strict/raise_for_errors)
    └── .results     — tuple of failed TaskResults
```

### Handling errors

```python
from osiiso import ClosedError, ExecutionError

# Catch submission errors
try:
    q.submit(work, data)
except ClosedError:
    print("Queue is closed!")

# Catch execution errors
try:
    summary = await q.run(strict=True)
except ExecutionError as e:
    for result in e.results:
        print(f"  {result.name}: {result.exception}")

# Or inspect without exceptions
summary = await q.run()
if not summary.ok:
    for err in summary.errors:
        print(f"Failed: {err.name} — {err.exception}")
```

---

## Real-World Patterns

### Web scraper with retries

```python
import osiiso
from functools import partial

async def scrape(url, headers=None):
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers or {}) as resp:
            return await resp.text()

async def main():
    urls = [f"https://example.com/page/{i}" for i in range(100)]
    opts = osiiso.TaskOptions(retries=3, retry_delay=1.0, backoff=2.0, timeout=30)

    async with osiiso.AsyncQueue(workers=10) as q:
        q.map(scrape, urls, opts=opts)
        summary = await q.run()

    print(f"Scraped {summary.succeeded}/{summary.total_submitted} pages")
    if summary.errors:
        print(f"Failed URLs: {[e.name for e in summary.errors]}")

osiiso.run(main())
```

### ETL pipeline with groups

```python
import osiiso

def extract(source):
    return f"data from {source}"

def transform(data):
    return data.upper()

def load(record):
    return f"loaded: {record}"

with osiiso.ThreadQueue(workers=8) as q:
    # Phase 1: Extract from multiple sources
    extract_group = q.group([
        (extract, "db"),
        (extract, "api"),
        (extract, "file"),
    ])
    phase1 = q.run()

    # Phase 2: Transform extracted data
    q.reset()
    transform_group = q.group([
        (transform, val) for val in phase1.values
    ])
    phase2 = q.run()

    # Phase 3: Load transformed data
    q.reset()
    load_group = q.group([
        (load, val) for val in phase2.values
    ])
    phase3 = q.run()

phase3.display()
```

### Heterogeneous pipeline in a single group

```python
with osiiso.ThreadQueue(workers=8) as q:
    # Mix different operations in one group
    pipeline = q.group([
        (extract, "db"),
        (transform, raw_data),
        (load, processed_record),
    ], group_id="etl-batch-1")

    summary = q.run()
    summary.display()
```

### Priority-based task scheduling

```python
async with osiiso.AsyncQueue(workers=2) as q:
    q.submit(work, "low-priority", priority=9)
    q.submit(work, "normal", priority=5)
    q.submit(work, "urgent", priority=1)       # runs first
    q.submit(work, "critical", priority=0)     # runs first

    summary = await q.run()
```

### Protected critical tasks

```python
async with osiiso.AsyncQueue(workers=4, timeout=5.0) as q:
    q.submit(optional_work, 1)
    q.submit(optional_work, 2)
    q.submit(save_to_database, data, must_complete=True)  # survives timeout

    summary = await q.run()
    # Even if timeout fires, save_to_database will complete
```

### Delayed / scheduled execution

```python
import time

async with osiiso.AsyncQueue(workers=4) as q:
    # Execute after 5 seconds
    q.submit(work, data, delay=5.0)

    # Execute at a specific time
    q.submit(work, data, run_at=time.time() + 60)  # 1 minute from now

    summary = await q.run()
```

### Parallel CPU work with process queue

```python
import osiiso

def factorize(n):
    factors = []
    d = 2
    while d * d <= n:
        while n % d == 0:
            factors.append(d)
            n //= d
        d += 1
    if n > 1:
        factors.append(n)
    return factors

if __name__ == "__main__":
    numbers = [2**61 - 1, 2**31 - 1, 10**12 + 39, 10**9 + 7]

    with osiiso.ProcessQueue(workers=4) as q:
        q.map(factorize, numbers)
        summary = q.run()

    for num, result in zip(numbers, summary.results):
        print(f"{num} = {result.value}")
```
