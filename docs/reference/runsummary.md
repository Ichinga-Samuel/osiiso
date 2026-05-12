# RunSummary

`osiiso.RunSummary` — aggregate summary of a completed queue run.

```python
from osiiso import RunSummary
```

---

## Fields

| Field | Type | Description |
|-------|------|-------------|
| `total_submitted` | `int` | Total tasks in this run |
| `succeeded` | `int` | Succeeded count |
| `failed` | `int` | Failed count |
| `cancelled` | `int` | Cancelled count |
| `timed_out` | `bool` | `True` if run hit a timeout |
| `duration` | `float` | Wall-clock seconds |
| `results` | `tuple[TaskResult, ...]` | All results in order |

---

## Properties

| Property | Type | Description |
|----------|------|-------------|
| `ok` | `bool` | `True` if no failures, cancellations, or timeouts |
| `values` | `tuple[Any, ...]` | Return values of succeeded tasks |
| `errors` | `tuple[TaskResult, ...]` | Failed TaskResult objects |

---

## Methods

### Filtering

| Method | Returns | Description |
|--------|---------|-------------|
| `successes()` | `tuple[TaskResult, ...]` | All succeeded results |
| `cancellations()` | `tuple[TaskResult, ...]` | All cancelled results |

### Lookup

| Method | Returns | Description |
|--------|---------|-------------|
| `by_task_id()` | `dict[str, TaskResult]` | Index by task ID |
| `by_name()` | `dict[str, tuple[TaskResult, ...]]` | Group by name |
| `by_group()` | `dict[str \| None, tuple[TaskResult, ...]]` | Group by group ID |

### Error Handling

| Method | Description |
|--------|-------------|
| `raise_for_errors()` | Raise `ExecutionError` if any task failed |
| `display()` | Print human-readable summary to stdout |
