# TaskResult

`osiiso.TaskResult` — immutable record of a single completed task.

```python
from osiiso import TaskResult
```

---

## Fields

`TaskResult` is a frozen dataclass with `__slots__`.

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | `str` | UUID hex identifier |
| `name` | `str` | Human-readable name |
| `status` | `str` | `"succeeded"`, `"failed"`, or `"cancelled"` |
| `value` | `Any` | Return value (succeeded only) |
| `exception` | `BaseException \| None` | Exception (failed only) |
| `attempts` | `int` | Total execution attempts |
| `priority` | `int` | Priority level |
| `must_complete` | `bool` | Whether protected from cancellation |
| `group_id` | `str \| None` | Group identifier |
| `detached` | `bool` | Excluded from `run()` aggregation |
| `scheduled_for` | `float \| None` | Absolute `perf_counter` target |
| `created_at` | `float` | `perf_counter` submission timestamp |
| `started_at` | `float \| None` | `perf_counter` first execution timestamp |
| `finished_at` | `float` | `perf_counter` completion timestamp |
| `duration` | `float` | Wall-clock seconds from start to finish |
| `message` | `str` | Short outcome description |
