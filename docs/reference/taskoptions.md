# TaskOptions

`osiiso.TaskOptions` — immutable, reusable configuration for task submission.

```python
from osiiso import TaskOptions
```

---

## Constructor

```python
TaskOptions(
    priority: int = 3,
    must_complete: bool = False,
    timeout: float | None = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    backoff: float = 1.0,
    delay: float | None = None,
    run_at: float | None = None,
    name: str | None = None,
    group_id: str | None = None,
    detached: bool = False,
)
```

`TaskOptions` is a **frozen dataclass** with `__slots__`. It cannot be modified
after construction.

---

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `priority` | `int` | `3` | Scheduling priority. Lower numbers run first. |
| `must_complete` | `bool` | `False` | Protected from cancellation during graceful shutdown. |
| `timeout` | `float \| None` | `None` | Per-task time limit in seconds. |
| `retries` | `int` | `0` | Additional attempts after the initial failure. |
| `retry_delay` | `float` | `0.0` | Seconds before the first retry. |
| `backoff` | `float` | `1.0` | Multiplier applied to delay after each retry. |
| `delay` | `float \| None` | `None` | Delay execution by this many seconds. |
| `run_at` | `float \| None` | `None` | Execute at this `time.time()` timestamp. |
| `name` | `str \| None` | `None` | Custom name for results and hooks. |
| `group_id` | `str \| None` | `None` | Associate with a named group. |
| `detached` | `bool` | `False` | Exclude from `run()` result aggregation. |

---

## Methods

### `replace(**overrides) -> TaskOptions`

Return a new `TaskOptions` with the given fields replaced:

```python
base = TaskOptions(retries=3, timeout=10)
urgent = base.replace(priority=0, name="urgent")
```

---

## Validation

`TaskOptions.__post_init__()` validates all fields immediately:

| Condition | Exception |
|-----------|-----------|
| `timeout <= 0` | `ValueError` |
| `retries < 0` | `ValueError` |
| `retry_delay < 0` | `ValueError` |
| `backoff <= 0` | `ValueError` |
| `delay < 0` | `ValueError` |
| `delay` and `run_at` both set | `ValueError` |

Unknown keyword arguments passed to `submit()` raise `TypeError`:

```python
q.submit(work, retrise=3)
# TypeError: Unknown task option(s): retrise
```
