# Handles & Groups

Every `submit()` call returns a **handle** — a lightweight object for awaiting,
inspecting, and cancelling individual tasks. Groups collect multiple handles
under a shared identifier.

---

## Async Handles — `TaskHandle`

Returned by `AsyncQueue.submit()`. Async handles are **awaitable**:

```python
async with osiiso.AsyncQueue(workers=2) as q:
    handle = q.submit(fetch, "https://example.com")

    # Await the handle directly
    result = await handle       # Returns TaskResult

    # Or use explicit methods
    result = await handle.wait()
    value = handle.value()      # Returns the callable's return value
```

### Methods and Properties

| Member | Returns | Description |
|--------|---------|-------------|
| `await handle` | `TaskResult` | Await until the task finishes |
| `await handle.wait()` | `TaskResult` | Same as `await handle` |
| `handle.result()` | `TaskResult` | Get result (raises `InvalidStateError` if pending) |
| `handle.value()` | `Any` | Get return value (re-raises exceptions) |
| `handle.cancel()` | `bool` | Request cancellation |
| `handle.done()` | `bool` | `True` if task has a final result |
| `handle.cancelled()` | `bool` | `True` if task was cancelled |
| `handle.exception()` | `BaseException \| None` | The exception, or `None` |
| `handle.status` | `str` | `"pending"`, `"running"`, `"retrying"`, `"succeeded"`, `"failed"`, or `"cancelled"` |
| `handle.attempts` | `int` | Number of execution attempts |
| `handle.task_id` | `str` | Unique hex identifier |
| `handle.name` | `str` | Human-readable task name |

---

## Sync Handles — `SyncTaskHandle`

Returned by `ThreadQueue.submit()` and `ProcessQueue.submit()`. All methods
**block** the calling thread:

```python
with osiiso.ThreadQueue(workers=2) as q:
    handle = q.submit(write_file, path)
    result = handle.wait(timeout=5)  # Blocks up to 5 seconds
    value = handle.value()
```

The API mirrors `TaskHandle` except:

- **Not awaitable** — use `handle.wait(timeout=...)` instead
- `result()` raises `RuntimeError` (not `InvalidStateError`) if pending
- `value()` raises `concurrent.futures.CancelledError` on cancellation

---

## Streaming Completions

For `AsyncQueue`, process results **as they complete** using `as_completed()`:

```python
q = osiiso.AsyncQueue(workers=4)
handles = q.map(fetch, urls)
await q.start()

try:
    async for handle in osiiso.AsyncQueue.as_completed(handles):
        print(handle.name, handle.value())
finally:
    await q.shutdown()
```

Handles are yielded in **completion order** (fastest first), not submission order.

---

## Task Groups

### Creating Groups

Use `group()` to submit a batch of tasks under a shared identifier:

=== "Heterogeneous tasks"

    ```python
    group = q.group(
        [
            (fetch_user, "ada"),
            (fetch_user, "grace"),
            (compute_stats, data),
        ],
        group_id="pipeline",
    )
    ```

=== "Homogeneous tasks"

    ```python
    group = q.group(fetch, ["users", "posts", "comments"], group_id="api")
    ```

### Async Groups — `TaskGroup`

```python
summary = await q.run()

# Wait for just this group
group_summary = await group.wait()    # Returns RunSummary
values = await group.values()         # Returns tuple, raises on failure
count = group.cancel()                # Cancel remaining tasks
```

### Sync Groups — `SyncTaskGroup`

```python
summary = q.run()

group_summary = group.wait(timeout=30)  # Blocks with timeout
values = group.values()                 # Returns tuple, raises on failure
count = group.cancel()                  # Cancel remaining tasks
```

### Group Properties

| Member | Returns | Description |
|--------|---------|-------------|
| `group.group_id` | `str` | The group identifier |
| `group.handles` | `tuple` | Immutable tuple of handles |
| `len(group)` | `int` | Number of tasks in the group |
| `iter(group)` | iterator | Iterate over handles |

---

## Grouped Reporting

After a run, use `RunSummary.by_group()` to inspect results by group:

```python
summary = await q.run()
by_group = summary.by_group()

for group_id, results in by_group.items():
    print(f"{group_id}: {len(results)} tasks")
```

See also: `summary.by_name()` and `summary.by_task_id()` for alternative
lookup shapes.

---

## Next Steps

- [Results & Summaries](results-and-summaries.md) — Detailed result inspection
- [Lifecycle & Policies](lifecycle-and-policies.md) — Shutdown, modes, and hooks
