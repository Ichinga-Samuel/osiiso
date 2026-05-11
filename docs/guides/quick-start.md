# Quick Start

Install the package:

```bash
pip install osiiso
```

The package currently targets Python 3.13 and newer.

## AsyncQueue

Use `AsyncQueue` for coroutine-based I/O.

```python
import asyncio
import osiiso

async def fetch(name: str) -> str:
    await asyncio.sleep(0.1)
    return f"fetched {name}"

async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(fetch, "users", priority=0)
        q.submit(fetch, "posts", retries=2, retry_delay=0.25)
        summary = await q.run(strict=True)
        print(summary.values)

osiiso.run(main())
```

## ThreadQueue

Use `ThreadQueue` for blocking functions.

```python
import time
import osiiso

def resize(name: str) -> str:
    time.sleep(0.1)
    return f"resized {name}"

with osiiso.ThreadQueue(workers=4) as q:
    q.map(resize, ["a.png", "b.png", "c.png"], name="resize")
    summary = q.run(strict=True)

print(summary.values)
```

## ProcessQueue

Use `ProcessQueue` for CPU-bound functions. Keep process tasks at module top
level so multiprocessing can pickle them.

```python
import osiiso

def score(n: int) -> int:
    return sum(i * i for i in range(n))

if __name__ == "__main__":
    with osiiso.ProcessQueue(workers=4) as q:
        q.map(score, [10_000, 20_000, 30_000], name="score")
        summary = q.run(strict=True)

    print(summary.values)
```

## What `run()` returns

Each queue returns a `RunSummary`:

```python
summary.ok
summary.succeeded
summary.failed
summary.cancelled
summary.timed_out
summary.values
summary.errors
summary.by_group()
summary.raise_for_errors()
```

Use `strict=True` when failures should raise `ExecutionError` immediately.
