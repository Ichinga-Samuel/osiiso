# Handles

Task handles are lightweight objects for awaiting, inspecting, and cancelling
individual submitted tasks.

```python
from osiiso import TaskHandle, SyncTaskHandle
```

---

## TaskHandle

Returned by [`AsyncQueue.submit()`](asyncqueue.md). **Awaitable** — use
`await handle` or `await handle.wait()`.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `await handle` | `TaskResult` | Await until the task finishes |
| `await wait()` | `TaskResult` | Same as `await handle`; safe for concurrent callers |
| `result()` | `TaskResult` | Get result immediately; raises `InvalidStateError` if pending |
| `value()` | `Any` | Get return value; re-raises exception on failure, `CancelledError` on cancel |
| `cancel()` | `bool` | Request cancellation; `False` if already done |
| `done()` | `bool` | `True` if task has a final result |
| `cancelled()` | `bool` | `True` if status is `"cancelled"` |
| `exception()` | `BaseException \| None` | The exception, or `None`; raises if pending |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `task_id` | `str` | UUID hex identifier |
| `name` | `str` | Human-readable name |
| `priority` | `int` | Scheduling priority |
| `must_complete` | `bool` | Protected from shutdown cancellation |
| `created_at` | `float` | `perf_counter` submission timestamp |
| `group_id` | `str \| None` | Group identifier |
| `detached` | `bool` | Excluded from `run()` aggregation |
| `scheduled_for` | `float \| None` | Absolute `perf_counter` target start time |
| `status` | `str` | `"pending"`, `"running"`, `"retrying"`, `"succeeded"`, `"failed"`, or `"cancelled"` |
| `attempts` | `int` | Number of execution attempts |

### Example

```python
async with osiiso.AsyncQueue(workers=2) as q:
    handle = q.submit(fetch, "https://example.com", retries=2)

    # Await the result
    result = await handle
    print(result.status)   # "succeeded"
    print(handle.value())  # Return value of fetch()
```

---

## SyncTaskHandle

Returned by [`ThreadQueue.submit()`](threadqueue.md) and
[`ProcessQueue.submit()`](processqueue.md). All methods **block** the calling
thread.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `wait(timeout=None)` | `TaskResult` | Block until done; raises `TimeoutError` on expiry |
| `result()` | `TaskResult` | Get result immediately; raises `RuntimeError` if pending |
| `value()` | `Any` | Get return value; re-raises exception or `CancelledError` |
| `cancel()` | `bool` | Request cancellation; `False` if already done |
| `done()` | `bool` | `True` if task has a final result |
| `cancelled()` | `bool` | `True` if status is `"cancelled"` |
| `exception()` | `BaseException \| None` | The exception, or `None`; raises if pending |

### Properties

Same as `TaskHandle` (except `SyncTaskHandle` is not awaitable).

### Example

```python
with osiiso.ThreadQueue(workers=2) as q:
    handle = q.submit(write_file, path, must_complete=True)

    # Block until done
    result = handle.wait(timeout=5)
    print(result.status)
    print(handle.value())
```

---

## Status Values

Both handle types expose a `status` property with these possible values:

| Status | Meaning |
|--------|---------|
| `"pending"` | Submitted but not yet started |
| `"running"` | Currently executing |
| `"retrying"` | Failed, waiting for retry |
| `"succeeded"` | Completed successfully |
| `"failed"` | All attempts exhausted |
| `"cancelled"` | Cancelled before or during execution |
