# Task Options

`TaskOptions` is an immutable configuration object for submitted tasks. Pass it
as `opts=` or provide the same fields directly as keyword arguments.

```python
from osiiso import TaskOptions

retrying = TaskOptions(retries=3, retry_delay=0.5, backoff=2, timeout=10)
urgent = retrying.replace(priority=0, name="urgent-api-call")

q.submit(fetch, url, opts=urgent)
q.submit(fetch, other_url, retries=3, retry_delay=0.5, backoff=2)
```

## Fields

| Field | Default | Meaning |
| --- | --- | --- |
| `priority` | `3` | Lower numbers run first. |
| `must_complete` | `False` | Protected during graceful shutdown. |
| `timeout` | `None` | Per-task timeout in seconds. |
| `retries` | `0` | Retry attempts after the first failure. |
| `retry_delay` | `0.0` | Delay before the first retry. |
| `backoff` | `1.0` | Multiplier applied after each retry. |
| `delay` | `None` | Run after this many seconds. |
| `run_at` | `None` | Run at an absolute epoch timestamp. |
| `name` | `None` | Custom name used in results and hooks. |
| `group_id` | `None` | Group label for summaries. |
| `detached` | `False` | Metadata flag for fire-and-forget style tasks. |

## Retries and backoff

```python
q.submit(call_api, retries=4, retry_delay=0.25, backoff=2)
```

This can attempt the task up to five times total: the first attempt plus four
retries.

## Delays and scheduling

```python
q.submit(send_email, message, delay=5)
q.submit(compact_database, run_at=time.time() + 60)
```

Use either `delay` or `run_at`, not both.

## Validation

`TaskOptions` rejects invalid values immediately:

```python
TaskOptions(timeout=0)          # ValueError
TaskOptions(retries=-1)         # ValueError
TaskOptions(delay=1, run_at=2)  # ValueError
```

Unknown task option names also raise:

```python
q.submit(work, retrise=3)  # TypeError: Unknown task option(s)
```
