# ProcessQueue

`osiiso.ProcessQueue` — process-based task queue for CPU-heavy work in
subprocesses.

```python
from osiiso import ProcessQueue
```

---

## Constructor

```python
ProcessQueue(
    *,
    workers: int | None = None,
    size: int = 0,
    timeout: float | None = None,
    mode: Literal["finite", "infinite"] = "finite",
    fail_policy: Literal["continue", "fail_first"] = "continue",
    on_exit: Literal["complete_priority", "cancel"] = "complete_priority",
    on_start: Callable[[SyncTaskHandle], Any] | None = None,
    on_complete: Callable[[TaskResult], Any] | None = None,
    on_retry: Callable[[SyncTaskHandle, BaseException], Any] | None = None,
    poll: float = 0.05,
    context: Any = None,
)
```

Accepts all the same parameters as [`ThreadQueue`](threadqueue.md), plus:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `poll` | `float` | `0.05` | Seconds between cancellation/timeout checks |
| `context` | `Any` | `None` | Custom `multiprocessing` context (e.g., `multiprocessing.get_context("spawn")`) |

---

## Pickling Requirements

!!! important "Functions and arguments must be pickleable"
    - Use **top-level module functions** (not lambdas, closures, or nested functions)
    - Use **plain data types** as arguments (strings, numbers, dicts, lists)
    - Always guard with `if __name__ == "__main__":` on Windows

```python
# ✅ Top-level function — pickleable
def compute(n: int) -> int:
    return n * n

# ❌ Lambda — not pickleable
compute = lambda n: n * n

# ❌ Closure — not pickleable
def make_compute():
    def compute(n):
        return n * n
    return compute
```

### Coroutine Functions

`ProcessQueue` supports coroutine functions — they are executed with
`asyncio.run()` in the subprocess:

```python
async def async_compute(data: list) -> dict:
    # Runs in subprocess via asyncio.run()
    return {"result": sum(data)}
```

---

## Task Submission

### `submit(fn, *args, opts=None, **overrides) -> SyncTaskHandle`

Submit a single task. Returns a [`SyncTaskHandle`](handles.md#synctaskhandle).

### `map(fn, iterable, *, opts=None, **overrides) -> list[SyncTaskHandle]`

Submit `fn` once per element.

### `group(tasks, iterable=None, *, group_id=None, opts=None, **overrides) -> SyncTaskGroup`

Submit a batch and return a [`SyncTaskGroup`](groups.md#synctaskgroup).

### `task(opts=None, **overrides) -> Callable`

Decorator that binds a function to this queue.

---

## Lifecycle

### `start() -> ProcessQueue`

Start worker processes. Called automatically by `run()` and `__enter__`.

### `run(timeout=None, *, strict=False, fail_policy=None) -> RunSummary`

Execute all pending tasks and return a [`RunSummary`](runsummary.md). **Blocks
the calling thread.**

### `shutdown(*, force=False) -> None`

Stop the queue and terminate all subprocesses.

### `reset() -> None`

Clear results and state for reuse.

### `clear_results() -> None`

Discard accumulated results.

### `cancel() -> None`

Request immediate cancellation. Thread-safe.

---

## Context Manager

```python
if __name__ == "__main__":
    with ProcessQueue(workers=4) as q:
        q.map(compute, [1, 2, 3, 4])
        summary = q.run()
```

---

## Properties

| Property | Type | Description |
|----------|------|-------------|
| `active_count` | `int` | Tasks currently executing |
| `pending_count` | `int` | Tasks waiting in the queue |
| `closed` | `bool` | `True` after shutdown completes |
| `results` | `tuple[TaskResult, ...]` | Snapshot of all accumulated results |
| `stats` | `dict` | `{"pending", "active", "completed", "workers", "closed"}` |

---

## Platform Notes

- **Windows**: Always use `if __name__ == "__main__":` guard
- **macOS**: Default start method is `spawn` (same as Windows)
- **Linux**: Default start method is `fork` (faster, but `spawn` is safer)

Use the `context` parameter to control the start method:

```python
import multiprocessing

ctx = multiprocessing.get_context("spawn")
q = ProcessQueue(workers=4, context=ctx)
```
