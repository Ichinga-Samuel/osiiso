# Quick Start

Get up and running with osiiso in under a minute.

---

## Install

```bash
pip install osiiso
```

The package targets **Python 3.13+** and has zero runtime dependencies.

---

## Your First Queue

### Async Tasks

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

??? tip "What does `osiiso.run()` do?"
    `osiiso.run()` is a thin wrapper around `asyncio.run()` that automatically
    uses [uvloop](https://github.com/MagicStack/uvloop) when installed. Pass
    `use_uvloop=False` to force the stdlib event loop.

### Thread Tasks

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

### Process Tasks

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

!!! warning "ProcessQueue on Windows"
    Always guard `ProcessQueue` usage with `if __name__ == "__main__":` on
    Windows. Keep task functions at module top level so they can be pickled.

---

## What `run()` Returns

Every queue `run()` returns a [`RunSummary`](../reference/runsummary.md):

```python
summary.ok           # True if no failures, cancellations, or timeouts
summary.succeeded    # Count of succeeded tasks
summary.failed       # Count of failed tasks
summary.cancelled    # Count of cancelled tasks
summary.timed_out    # True if the run hit a timeout
summary.values       # Tuple of return values from succeeded tasks
summary.errors       # Tuple of failed TaskResult objects
summary.display()    # Print a human-readable report
```

Use `strict=True` to raise [`ExecutionError`](../reference/exceptions.md) automatically when any task fails:

```python
summary = await q.run(strict=True)  # raises on failure
```

---

## Next Steps

- [Choosing a Queue](choosing-a-queue.md) — Pick the right backend for your workload
- [Task Submission](task-submission.md) — Learn `submit()`, `map()`, and `group()`
- [Task Options](task-options.md) — Configure retries, timeouts, priorities, and more
