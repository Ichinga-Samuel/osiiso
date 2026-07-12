# Exceptions

All exceptions raised by osiiso descend from `OsiisoError`.

```python
from osiiso import OsiisoError, ClosedError, QueueFullError, ExecutionError
```

---

## OsiisoError

Base exception for the osiiso package. Catch this to handle all library errors:

```python
try:
    summary = await q.run()
except osiiso.OsiisoError:
    ...
```

---

## ClosedError

Raised when a task is submitted to a queue that is closed or shutting down.

Thrown by `submit()`, `map()`, and `group()` after `shutdown()` has been called
or after the context manager block exits.

```python
await q.shutdown()
q.submit(work, 1)  # raises ClosedError
```

**Inherits from:** `OsiisoError`

---

## QueueFullError

Raised by `AsyncQueue.submit()` when a bounded queue (`size > 0`) already has
`size` outstanding tasks.  Sync queues block on `submit()` instead of raising.

```python
q = osiiso.AsyncQueue(size=100)
q.submit(work, 1)  # raises QueueFullError once 100 tasks are outstanding
```

**Inherits from:** `OsiisoError`

---

## ExecutionError

Raised when one or more tasks fail (or are cancelled) during queue execution.

| Attribute | Type | Description |
|-----------|------|-------------|
| `results` | `tuple[TaskResult, ...]` | The offending TaskResult objects |

Raised by:

- `q.run(strict=True)`
- `summary.raise_for_errors()`
- `group.values()` and `amap()`/`tmap()`/`pmap()` (when any task failed **or was cancelled**)

```python
try:
    summary = await q.run(strict=True)
except osiiso.ExecutionError as e:
    print(f"{len(e.results)} task(s) failed")
    for r in e.results:
        print(f"  {r.name}: {r.message}")
```

**Inherits from:** `OsiisoError`
