# ThreadQueue

`osiiso.ThreadQueue` — thread-based task queue for blocking synchronous work.

```python
from osiiso import ThreadQueue
```

---

## Constructor

```python
ThreadQueue(
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
)
```

Accepts all the same parameters as [`AsyncQueue`](asyncqueue.md), plus:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `poll` | `float` | `0.05` | Seconds between cancellation/timeout checks during sync execution |

!!! note "Sync callables only"
    `ThreadQueue` raises `TypeError` if you submit an awaitable or coroutine
    function. Use [`AsyncQueue`](asyncqueue.md) for async work.

---

## Task Submission

### `submit(fn, *args, opts=None, **overrides) -> SyncTaskHandle`

Submit a single sync task. Returns a [`SyncTaskHandle`](handles.md#synctaskhandle).

### `map(fn, iterable, *, opts=None, **overrides) -> list[SyncTaskHandle]`

Submit `fn` once per element. Same input interpretation as `AsyncQueue.map()`.

### `group(tasks, iterable=None, *, group_id=None, opts=None, **overrides) -> SyncTaskGroup`

Submit a batch and return a [`SyncTaskGroup`](groups.md#synctaskgroup).

### `task(opts=None, **overrides) -> Callable`

Decorator that binds a sync function to this queue.

---

## Lifecycle

### `start() -> ThreadQueue`

Start worker threads. Called automatically by `run()` and `__enter__`.

### `run(timeout=None, *, strict=False, fail_policy=None) -> RunSummary`

Execute all pending tasks and return a [`RunSummary`](runsummary.md). **Blocks
the calling thread.**

### `shutdown(*, force=False) -> None`

Stop the queue and join all threads.

### `reset() -> None`

Clear results and state for reuse.

### `clear_results() -> None`

Discard accumulated results.

### `cancel() -> None`

Request immediate cancellation. Thread-safe.

---

## Context Manager

```python
with ThreadQueue(workers=4) as q:
    q.submit(work, 1)
    summary = q.run()
# Automatic shutdown
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
