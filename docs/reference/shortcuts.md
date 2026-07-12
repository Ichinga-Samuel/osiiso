# Shortcuts

`osiiso.amap()`, `osiiso.tmap()`, `osiiso.pmap()` — one-shot mapping helpers.
Each builds a queue, maps a callable over an iterable, runs it, and returns
the values **in input order**.

```python
from osiiso import amap, tmap, pmap
```

Because every helper raises [`ExecutionError`](exceptions.md#executionerror)
if any task failed **or was cancelled**, a returned tuple always lines up 1:1
with the input iterable.

---

## Signatures

```python
await amap(fn, iterable, *, workers=None, rate=None, timeout=None, **options) -> tuple
tmap(fn, iterable, *, workers=None, rate=None, timeout=None, **options) -> tuple
pmap(fn, iterable, *, workers=None, rate=None, timeout=None,
     context=None, initializer=None, initargs=(), **options) -> tuple
```

| Helper | Queue | Notes |
|--------|-------|-------|
| `amap` | [`AsyncQueue`](asyncqueue.md) | Awaitable; call from async code |
| `tmap` | [`ThreadQueue`](threadqueue.md) | Blocks the calling thread |
| `pmap` | [`ProcessQueue`](processqueue.md) | Blocks; standard pickling/spawn rules apply |

| Parameter | Description |
|-----------|-------------|
| `fn` | The callable to run once per element |
| `iterable` | Elements interpreted like [`map()`](asyncqueue.md#task-submission): tuple → positional args, mapping → keyword args, else a single arg |
| `workers` | Forwarded to the queue (`None` = auto-scale) |
| `rate` | Max task attempts per second (`None` = unlimited) |
| `timeout` | Overall run limit in seconds |
| `context` / `initializer` / `initargs` | `pmap` only — forwarded to `ProcessQueue` |
| `**options` | Any [`TaskOptions`](taskoptions.md) fields (`retries=3`, `priority=0`, ...) |

**Raises:** [`ExecutionError`](exceptions.md#executionerror) if any task failed or was cancelled.

---

## Examples

```python
import osiiso

# Async — fan out HTTP fetches
pages = await osiiso.amap(fetch, urls, workers=8, retries=2)

# Threads — blocking I/O
sizes = osiiso.tmap(stat_file, paths, workers=8)

# Processes — CPU-bound work
scores = osiiso.pmap(rank, datasets, workers=4, timeout=60)
```

When you need handles, groups, priorities across mixed workloads, or a
reusable queue, use the queue classes directly — the shortcuts are for the
"just run this over these inputs" case.
