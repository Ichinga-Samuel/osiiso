# API Reference

This page summarizes the public API exported by `osiiso`.

## Queues

### `AsyncQueue`

Asyncio-based task queue.

```python
AsyncQueue(
    workers=None,
    size=0,
    timeout=None,
    mode="finite",
    fail_policy="continue",
    on_exit="complete_priority",
    on_start=None,
    on_complete=None,
    on_retry=None,
)
```

Important methods:

- `submit(fn, *args, opts=None, **options) -> TaskHandle`
- `map(fn, iterable, opts=None, **options) -> list[TaskHandle]`
- `group(tasks, group_id=None, opts=None, **options) -> TaskGroup`
- `task(opts=None, **options)`
- `start()`
- `run(timeout=None, strict=False, fail_policy=None) -> RunSummary`
- `shutdown(force=False)`
- `reset()`
- `clear_results()`
- `cancel()`
- `as_completed(handles)`

### `ThreadQueue`

Thread-based queue for sync blocking work.

Additional constructor option: `poll=0.05`.

Returns `SyncTaskHandle` and `SyncTaskGroup`.

### `ProcessQueue`

Process-based queue for CPU-heavy work.

Additional constructor options:

- `poll=0.05`
- `context=None`

Process functions should be importable, pickleable top-level callables.

## `TaskOptions`

```python
TaskOptions(
    priority=3,
    must_complete=False,
    timeout=None,
    retries=0,
    retry_delay=0.0,
    backoff=1.0,
    delay=None,
    run_at=None,
    name=None,
    group_id=None,
    detached=False,
)
```

Use `replace()` to derive a new immutable options object:

```python
retrying = TaskOptions(retries=3)
urgent = retrying.replace(priority=0)
```

## Handles

### `TaskHandle`

Returned by `AsyncQueue.submit()`.

- `await handle`
- `await handle.wait()`
- `handle.result()`
- `handle.value()`
- `handle.cancel()`
- `handle.done()`
- `handle.cancelled()`
- `handle.exception()`
- `handle.status`
- `handle.attempts`

### `SyncTaskHandle`

Returned by `ThreadQueue.submit()` and `ProcessQueue.submit()`.

- `handle.wait(timeout=None)`
- `handle.result()`
- `handle.value()`
- `handle.cancel()`
- `handle.done()`
- `handle.cancelled()`
- `handle.exception()`

## Groups

### `TaskGroup`

Async group:

- `await group.wait()`
- `await group.values()`
- `group.cancel()`
- `group.group_id`
- `group.handles`

### `SyncTaskGroup`

Sync group:

- `group.wait(timeout=None)`
- `group.values(timeout=None)`
- `group.cancel()`

## Results

### `TaskResult`

Fields:

- `task_id`
- `name`
- `status`
- `value`
- `exception`
- `attempts`
- `priority`
- `must_complete`
- `group_id`
- `detached`
- `scheduled_for`
- `created_at`
- `started_at`
- `finished_at`
- `duration`
- `message`

### `RunSummary`

Fields and helpers:

- `total_submitted`
- `succeeded`
- `failed`
- `cancelled`
- `timed_out`
- `duration`
- `results`
- `ok`
- `values`
- `errors`
- `successes()`
- `cancellations()`
- `by_task_id()`
- `by_name()`
- `by_group()`
- `raise_for_errors()`
- `display()`

## Exceptions

- `OsiisoError`
- `ClosedError`
- `ExecutionError`

`ExecutionError.results` contains the failed `TaskResult` objects.

## Runner

```python
osiiso.run(coro, use_uvloop=None, debug=False)
```

`use_uvloop=None` uses `uvloop` when installed. Pass `False` to force stdlib
asyncio or `True` to require `uvloop`.
