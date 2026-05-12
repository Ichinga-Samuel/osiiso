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
| `await wait()` | `RunSummary` | Await all handles |
| `await values()` | `tuple[Any, ...]` | Return values; raises on failure |
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
| `wait(timeout=None)` | `RunSummary` | Block until all finish |
| `values(timeout=None)` | `tuple[Any, ...]` | Return values; raises on failure |
| `cancel()` | `int` | Cancel all; returns count |

The `timeout` budget is shared across handles sequentially.

**Raises:** `TimeoutError` if budget exhausted. `ExecutionError` from `values()` on failure.
