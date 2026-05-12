# Task Options

`TaskOptions` is an **immutable, reusable** configuration object for submitted
tasks. Create one and share it across many submissions, or pass option fields
directly as keyword arguments.

---

## Two Ways to Configure

=== "Inline Options"

    ```python
    q.submit(fetch, url, retries=3, retry_delay=0.5, backoff=2, timeout=10)
    ```

=== "TaskOptions Object"

    ```python
    from osiiso import TaskOptions

    retrying = TaskOptions(retries=3, retry_delay=0.5, backoff=2, timeout=10)
    q.submit(fetch, url, opts=retrying)
    ```

Both approaches produce identical behavior. Use `TaskOptions` when you want to
**reuse** the same configuration across multiple submissions.

---

## Deriving New Options

`TaskOptions` is frozen — you cannot modify it after creation. Use `replace()`
to create a copy with overrides:

```python
retrying = TaskOptions(retries=3, retry_delay=0.5, backoff=2, timeout=10)
urgent = retrying.replace(priority=0, name="urgent-api-call")

q.submit(fetch, url1, opts=retrying)   # priority=3 (default)
q.submit(fetch, url2, opts=urgent)     # priority=0
```

---

## Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `priority` | `int` | `3` | Scheduling priority. **Lower numbers run first.** |
| `must_complete` | `bool` | `False` | If `True`, task is protected from cancellation during graceful shutdown. |
| `timeout` | `float \| None` | `None` | Per-task time limit in seconds. `None` = no limit. |
| `retries` | `int` | `0` | Number of retry attempts after the initial failure. |
| `retry_delay` | `float` | `0.0` | Seconds to wait before the first retry. |
| `backoff` | `float` | `1.0` | Multiplier applied to the delay after each retry. |
| `delay` | `float \| None` | `None` | Delay execution by this many seconds from submission. |
| `run_at` | `float \| None` | `None` | Execute at this absolute `time.time()` timestamp. |
| `name` | `str \| None` | `None` | Custom name used in results, hooks, and display. |
| `group_id` | `str \| None` | `None` | Associate the task with a named group. |
| `detached` | `bool` | `False` | If `True`, the task's result is excluded from `run()` aggregation. |

---

## Retries and Backoff

```python
q.submit(call_api, retries=4, retry_delay=0.25, backoff=2)
```

This produces up to **5 total attempts** (1 initial + 4 retries) with increasing delays:

| Attempt | Delay Before |
|---------|-------------|
| 1st (initial) | — |
| 2nd (retry 1) | 0.25s |
| 3rd (retry 2) | 0.50s |
| 4th (retry 3) | 1.00s |
| 5th (retry 4) | 2.00s |

The `on_retry` hook fires before each retry attempt:

```python
q = osiiso.AsyncQueue(
    on_retry=lambda handle, exc: print(f"Retrying {handle.name}: {exc}")
)
```

---

## Scheduling

### Relative Delay

Run a task after a specified number of seconds:

```python
q.submit(send_email, message, delay=5)
```

### Absolute Timestamp

Run a task at a specific wall-clock time:

```python
import time
q.submit(compact_database, run_at=time.time() + 60)
```

!!! warning "Mutually exclusive"
    `delay` and `run_at` cannot be used together. `TaskOptions` raises
    `ValueError` if both are set.

---

## Priority Scheduling

Tasks are executed in **priority order** — lower numbers run first:

```python
q.submit(critical_task, priority=0)   # Runs first
q.submit(normal_task, priority=3)     # Default priority
q.submit(background_task, priority=9) # Runs last
```

When tasks have the same priority, they execute in **submission order** (FIFO).

---

## Validation

`TaskOptions` validates all fields immediately on construction:

```python
TaskOptions(timeout=0)          # ValueError: timeout must be > 0
TaskOptions(retries=-1)         # ValueError: retries must be >= 0
TaskOptions(delay=1, run_at=2)  # ValueError: delay and run_at are mutually exclusive
TaskOptions(backoff=0)          # ValueError: backoff must be > 0
```

Unknown keyword arguments passed to `submit()` also raise immediately:

```python
q.submit(work, retrise=3)  # TypeError: Unknown task option(s): retrise
```

---

## Next Steps

- [Handles & Groups](handles-and-groups.md) — Awaiting and cancelling tasks
- [Results & Summaries](results-and-summaries.md) — Inspecting execution outcomes
