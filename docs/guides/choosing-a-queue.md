# Choosing a Queue

osiiso provides three queue backends with intentionally similar APIs. After you
learn one, switching between execution models requires minimal changes.

---

## Decision Matrix

| Workload | Queue | When to Use |
|----------|-------|-------------|
| Coroutine-based I/O | **`AsyncQueue`** | HTTP clients, async databases, websockets, API fan-out |
| Coroutine I/O fed from threads | **`SharedQueue`** | One event loop serving producers on other threads |
| Blocking synchronous work | **`ThreadQueue`** | File operations, blocking SDKs, SQLite writes, sync integrations |
| CPU-heavy computation | **`ProcessQueue`** | Ranking, parsing, scoring, transformations, analytics |

!!! tip "Just mapping a function over inputs?"
    Each backend has a one-shot helper — `amap()` (async), `tmap()` (threads),
    `pmap()` (processes) — that builds the queue, runs it, and returns values
    in input order. See the [Shortcuts reference](../reference/shortcuts.md).

---

## AsyncQueue

Best for **coroutine-heavy I/O** where you want many concurrent tasks sharing
a single event loop.

```python
async with osiiso.AsyncQueue(workers=8) as q:
    q.submit(fetch_user, "ada", retries=3, timeout=5)
    q.submit(fetch_user, "grace", priority=0)
    summary = await q.run()
```

**Key behaviors:**

- Coroutine functions are awaited directly
- Regular sync functions are automatically offloaded via `asyncio.to_thread()`
- Handles are **awaitable**: `result = await handle`
- Supports `as_completed()` for streaming results

Use `osiiso.run()` as your top-level entry point:

```python
result = osiiso.run(main(), use_uvloop=False)
```

---

## SharedQueue

An `AsyncQueue` whose **submission plane is thread-safe**. Work still executes on
the one event loop the queue is bound to — only `submit()`, `map()`, and
`group()` change, marshaling onto that loop from whatever thread calls them.

Reach for it when a long-lived loop should serve producers that are plain
threads:

```python
q = osiiso.SharedQueue(workers=4, mode="infinite")

def producer():                      # runs on any thread
    for item in source:
        q.submit(work, item, retries=2)

async def main():
    async with q:                    # binds the shared loop
        threading.Thread(target=producer).start()
        await q.run()                # serve until shutdown()
```

**Key behaviors:**

- A cross-thread submission blocks the caller for one loop round-trip and returns a fully registered handle
- `ClosedError` and `QueueFullError` raise at the call site, exactly as on the loop thread
- Producer threads observe completion with `handle.add_done_callback()` or `handle.done()`
- `run()`, `join()`, `shutdown()`, and `reset()` still belong to the owning loop

!!! warning "Hand it to producers only after it has started"
    Thread-safe submission holds once the queue has started and its loop is
    running. Before `start()` / `__aenter__`, submission is single-threaded just
    like `AsyncQueue`. Avoid submitting from another *event loop's* thread — the
    round-trip would block that loop briefly.

See the [`SharedQueue` reference](../reference/sharedqueue.md) for full details.

---

## ThreadQueue

Best for **blocking synchronous functions** — SDKs, filesystem operations,
SQLite writes, and code that blocks but doesn't need process-level parallelism.

```python
with osiiso.ThreadQueue(workers=4) as q:
    q.submit(write_row, row, must_complete=True)
    q.map(read_file, ["a.txt", "b.txt"])
    summary = q.run()
```

**Key behaviors:**

- Only accepts sync callables (raises `TypeError` for coroutines)
- Handles are **blocking**: `result = handle.wait(timeout=5)`
- Additional constructor options: `initializer=None, initargs=()` (run once per worker thread)

---

## ProcessQueue

Best for **CPU-bound work** that benefits from separate subprocesses and true
parallelism.

```python
def parse_document(path: str) -> dict[str, int]:
    ...

if __name__ == "__main__":
    with osiiso.ProcessQueue(workers=4) as q:
        q.map(parse_document, paths, timeout=30)
        summary = q.run()
```

**Key behaviors:**

- Runs work in **persistent** subprocesses — each worker's process is reused
  across tasks and respawned automatically if it crashes or is cancelled
- Supports coroutine functions (executed via `asyncio.run()` in the subprocess)
- Handles are **blocking**: same as `ThreadQueue`
- Additional constructor options: `context=None`, `initializer=None, initargs=()`
  (the initializer runs inside each subprocess)

!!! important "Pickling requirements"
    Process functions and arguments **must be pickleable**. Use top-level
    functions and plain data types. Lambdas, closures, and nested functions
    will fail.

---

## Shared Constructor Options

All three queues accept these constructor parameters:

```python
queue = osiiso.AsyncQueue(
    workers=4,              # Number of worker coroutines/threads/processes
    size=0,                 # Max outstanding tasks (0 = unbounded)
    timeout=None,           # Per-run time limit in seconds
    mode="finite",          # "finite" or "infinite"
    fail_policy="continue", # "continue" or "fail_first"
    on_timeout="complete",  # "complete" or "cancel"
    rate=None,              # Max task attempts per second (None = unlimited)
    burst=1,                # Attempts that may start back-to-back after idle
    on_start=None,          # Callback: (handle) -> None
    on_complete=None,       # Callback: (result) -> None
    on_retry=None,          # Callback: (handle, exception) -> None
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | `int \| None` | `None` | Fixed worker count. `None` = auto-scale |
| `size` | `int` | `0` | Max outstanding tasks (`0` = unbounded). Sync queues block `submit()` when full; `AsyncQueue` raises `QueueFullError` |
| `timeout` | `float \| None` | `None` | Queue-level run timeout |
| `mode` | `str` | `"finite"` | `"finite"` drains and stops; `"infinite"` runs until shutdown |
| `fail_policy` | `str` | `"continue"` | `"continue"` or `"fail_first"` (spares `must_complete` tasks) |
| `on_timeout` | `str` | `"complete"` | On run timeout: `"complete"` lets `must_complete` tasks finish; `"cancel"` stops all |
| `rate` | `float \| None` | `None` | Max task attempts per second across all workers |
| `burst` | `int` | `1` | With `rate`, attempts that may start back-to-back after idle |
| `on_start` | `callable` | `None` | Called when a task begins |
| `on_complete` | `callable` | `None` | Called when a task finishes |
| `on_retry` | `callable` | `None` | Called before a retry attempt |

---

## Next Steps

- [Task Submission](task-submission.md) — Learn `submit()`, `map()`, and `group()`
- [Task Options](task-options.md) — Configure retries, timeouts, and priorities
