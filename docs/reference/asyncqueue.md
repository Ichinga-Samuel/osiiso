# AsyncQueue

`osiiso.AsyncQueue` — asyncio-native task queue with priorities, retries, and
structured concurrency.

```python
from osiiso import AsyncQueue
```

---

## Constructor

```python
AsyncQueue(
    *,
    workers: int | None = None,
    size: int = 0,
    timeout: float | None = None,
    mode: Literal["finite", "infinite"] = "finite",
    fail_policy: Literal["continue", "fail_first"] = "continue",
    on_exit: Literal["complete_priority", "cancel"] = "complete_priority",
    on_start: Callable[[TaskHandle], Any] | None = None,
    on_complete: Callable[[TaskResult], Any] | None = None,
    on_retry: Callable[[TaskHandle, BaseException], Any] | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | `int \| None` | `None` | Fixed worker count. `None` auto-scales up to `min(32, cpu_count × 4)` |
| `size` | `int` | `0` | Max priority queue capacity (`0` = unbounded) |
| `timeout` | `float \| None` | `None` | Queue-level run timeout in seconds |
| `mode` | `str` | `"finite"` | `"finite"` drains and stops; `"infinite"` runs until shutdown |
| `fail_policy` | `str` | `"continue"` | `"continue"` records failures; `"fail_first"` cancels on first failure |
| `on_exit` | `str` | `"complete_priority"` | `"complete_priority"` protects must-complete tasks; `"cancel"` stops all |
| `on_start` | `callable` | `None` | Callback `(handle) -> None` before task execution |
| `on_complete` | `callable` | `None` | Callback `(result) -> None` after task completion |
| `on_retry` | `callable` | `None` | Callback `(handle, exc) -> None` before retry |

**Raises:** `ValueError` if `size < 0`, `workers <= 0`, or `timeout <= 0`.

---

## Task Submission

### `submit(fn, *args, opts=None, **overrides) -> TaskHandle`

Submit a single task. Returns a [`TaskHandle`](handles.md#taskhandle).

- Positional `*args` are forwarded to `fn`
- `opts`: Optional base [`TaskOptions`](taskoptions.md)
- `**overrides`: Field overrides applied on top of `opts`

**Raises:** [`ClosedError`](exceptions.md#closederror) if the queue is not accepting tasks.

### `map(fn, iterable, *, opts=None, **overrides) -> list[TaskHandle]`

Submit `fn` once per element. Returns a list of [`TaskHandle`](handles.md#taskhandle) objects.

Element interpretation:

- **tuple** → unpacked as positional args
- **dict** → passed as keyword args via `functools.partial`
- **other** → single positional arg

### `group(tasks, iterable=None, *, group_id=None, opts=None, **overrides) -> TaskGroup`

Submit a batch and return a [`TaskGroup`](groups.md#taskgroup).

- `group([(fn, *args), ...])` — heterogeneous tasks
- `group(fn, iterable)` — homogeneous tasks (like `map()` with a group handle)

### `task(opts=None, **overrides) -> Callable`

Decorator that binds a function to this queue. See [Bound Tasks](../guides/bound-tasks.md).

---

## Lifecycle

### `await start() -> AsyncQueue`

Bind the event loop and start workers. Called automatically by `run()` and
`__aenter__`. Safe to call multiple times.

**Raises:** [`ClosedError`](exceptions.md#closederror) if shutdown has completed.

### `await run(timeout=None, *, strict=False, fail_policy=None) -> RunSummary`

Execute all pending tasks and return a [`RunSummary`](runsummary.md).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timeout` | `float \| None` | `None` | Override queue timeout for this run |
| `strict` | `bool` | `False` | Raise [`ExecutionError`](exceptions.md#executionerror) on failure |
| `fail_policy` | `str \| None` | `None` | Override queue fail policy for this run |

**Raises:** `RuntimeError` if `run()` is already in progress.

### `await shutdown(*, force=False) -> None`

Stop the queue. `force=True` cancels all work immediately.

### `await join() -> None`

Block until all queued tasks complete. Starts the queue if needed.

### `reset() -> None`

Clear results, handles, and internal state for reuse. Cannot be called during `run()`.

### `clear_results() -> None`

Discard accumulated [`TaskResult`](taskresult.md) objects to free memory.

### `cancel() -> asyncio.Task | None`

Request immediate cancellation from any thread. Thread-safe.

---

## Async Context Manager

```python
async with AsyncQueue(workers=4) as q:
    q.submit(work, 1)
    summary = await q.run()
# Automatic shutdown on exit
```

On normal exit: graceful drain. On exception: `force=True` shutdown.

---

## Static Methods

### `async as_completed(handles) -> AsyncIterator[TaskHandle]`

Yield handles in completion order (fastest first):

```python
async for handle in AsyncQueue.as_completed(handles):
    print(handle.value())
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
