# Exceptions

All exceptions raised by osiiso descend from `OsiisoError`.

```python
from osiiso import OsiisoError, ClosedError, ExecutionError
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

## ExecutionError

Raised when one or more tasks fail during queue execution.

| Attribute | Type | Description |
|-----------|------|-------------|
| `results` | `tuple[TaskResult, ...]` | The failed TaskResult objects |

Raised by:

- `q.run(strict=True)`
- `summary.raise_for_errors()`
- `group.values()` (when any task in the group failed)

```python
try:
    summary = await q.run(strict=True)
except osiiso.ExecutionError as e:
    print(f"{len(e.results)} task(s) failed")
    for r in e.results:
        print(f"  {r.name}: {r.message}")
```

**Inherits from:** `OsiisoError`
