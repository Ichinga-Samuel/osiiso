# Choosing a Queue

osiiso provides three queue backends with intentionally similar APIs. After you
learn one, switching between execution models requires minimal changes.

---

## Decision Matrix

| Workload | Queue | When to Use |
|----------|-------|-------------|
| Coroutine-based I/O | **`AsyncQueue`** | HTTP clients, async databases, websockets, API fan-out |
| Blocking synchronous work | **`ThreadQueue`** | File operations, blocking SDKs, SQLite writes, sync integrations |
| CPU-heavy computation | **`ProcessQueue`** | Ranking, parsing, scoring, transformations, analytics |

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
- Additional constructor option: `poll=0.05` (cancellation check interval)

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

- Runs work in subprocesses using `multiprocessing`
- Supports coroutine functions (executed via `asyncio.run()` in the subprocess)
- Handles are **blocking**: same as `ThreadQueue`
- Additional constructor options: `poll=0.05`, `context=None`

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
    size=0,                 # Max items in priority queue (0 = unbounded)
    timeout=None,           # Per-run time limit in seconds
    mode="finite",          # "finite" or "infinite"
    fail_policy="continue", # "continue" or "fail_first"
    on_exit="complete_priority",  # "complete_priority" or "cancel"
    on_start=None,          # Callback: (handle) -> None
    on_complete=None,       # Callback: (result) -> None
    on_retry=None,          # Callback: (handle, exception) -> None
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | `int \| None` | `None` | Fixed worker count. `None` = auto-scale |
| `size` | `int` | `0` | Max queue capacity (`0` = unbounded) |
| `timeout` | `float \| None` | `None` | Queue-level run timeout |
| `mode` | `str` | `"finite"` | `"finite"` drains and stops; `"infinite"` runs until shutdown |
| `fail_policy` | `str` | `"continue"` | `"continue"` or `"fail_first"` |
| `on_exit` | `str` | `"complete_priority"` | Shutdown behavior on timeout |
| `on_start` | `callable` | `None` | Called when a task begins |
| `on_complete` | `callable` | `None` | Called when a task finishes |
| `on_retry` | `callable` | `None` | Called before a retry attempt |

---

## Next Steps

- [Task Submission](task-submission.md) — Learn `submit()`, `map()`, and `group()`
- [Task Options](task-options.md) — Configure retries, timeouts, and priorities
