# Groups

Task groups collect multiple handles under a shared identifier.

```python
from osiiso import TaskGroup, SyncTaskGroup
```

---

## TaskGroup

Returned by `AsyncQueue.group()`. Async group with awaitable methods.

| Method | Returns | Description |
|--------|---------|-------------|
| `await wait()` | `RunSummary` | Await all handles (results in submission order) |
| `await values()` | `tuple[Any, ...]` | Values in submission order; raises `ExecutionError` if any task failed or was cancelled |
| `as_completed()` | `AsyncIterator[TaskHandle]` | Yield handles in completion order |
| `cancel()` | `int` | Cancel all; returns count |

| Property | Type | Description |
|----------|------|-------------|
| `group_id` | `str` | Group identifier |
| `handles` | `tuple[TaskHandle, ...]` | Immutable tuple of handles |

Supports `len(group)` and `for h in group:` iteration.

---

## SyncTaskGroup

Returned by `ThreadQueue.group()` and `ProcessQueue.group()`. Blocking methods.

| Method | Returns | Description |
|--------|---------|-------------|
| `wait(timeout=None)` | `RunSummary` | Block until all finish (results in submission order) |
| `values(timeout=None)` | `tuple[Any, ...]` | Values in submission order; raises `ExecutionError` if any task failed or was cancelled |
| `as_completed(timeout=None)` | `Iterator[SyncTaskHandle]` | Yield handles in completion order |
| `cancel()` | `int` | Cancel all; returns count |

The `timeout` budget is shared across handles sequentially.

**Raises:** `TimeoutError` if budget exhausted. `ExecutionError` from `values()` on failure or cancellation.

---

## Module-level completion iteration

Works on any collection of handles, not just groups:

```python
async for handle in osiiso.as_completed(handles):      # TaskHandle
    print(handle.value())

for handle in osiiso.iter_completed(handles, timeout=30):  # SyncTaskHandle
    print(handle.value())
```
