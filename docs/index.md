---
hide:
  - navigation
---

# osiiso

**Structured task queues for Python — one API across asyncio, threads, and processes.**

<p>
  <a href="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/action.yml"><img alt="CI" src="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/action.yml/badge.svg"></a>
  <a href="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/docs.yml"><img alt="Docs" src="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/docs.yml/badge.svg"></a>
  <img alt="Python 3.13+" src="https://img.shields.io/badge/python-3.13%2B-3776AB?logo=python&logoColor=white">
  <img alt="Typed" src="https://img.shields.io/badge/typed-py.typed-blue">
  <a href="https://github.com/Ichinga-Samuel/osiiso/blob/master/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green.svg"></a>
</p>

---

## What is osiiso?

`osiiso` gives you **one compact queue API** for three execution backends:

| Backend | Class | Best for |
|---------|-------|----------|
| **Asyncio** | `AsyncQueue` | HTTP clients, async databases, websockets, API fan-out |
| **Threads** | `ThreadQueue` | Blocking I/O, synchronous SDKs, filesystem work, SQLite |
| **Processes** | `ProcessQueue` | CPU-heavy computation, parsing, scoring, analytics |

All three queues share the same shape: **submit tasks → configure options → run → inspect results**.

```python
import asyncio
import osiiso


async def fetch(name: str) -> str:
    await asyncio.sleep(0.1)
    return f"fetched {name}"


async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(fetch, "users", priority=0)
        q.submit(fetch, "posts", retries=2, retry_delay=0.25, timeout=5)
        summary = await q.run(strict=True)
        return summary.values


print(osiiso.run(main()))
```

---

## Key Features

- :material-swap-horizontal: **Unified API** — Same interface for async, threaded, and process queues
- :material-sort-ascending: **Priority scheduling** — Lower priority numbers execute first
- :material-refresh: **Retries with backoff** — Configurable retry count, delay, and exponential backoff
- :material-timer-outline: **Timeouts** — Per-task and queue-level time limits
- :material-shield-check: **Graceful shutdown** — `must_complete` tasks are protected during shutdown
- :material-group: **Batch workflows** — `submit()`, `map()`, and `group()` for flexible task submission
- :material-clipboard-check: **Structured results** — `RunSummary` and immutable `TaskResult` records
- :material-hook: **Lifecycle hooks** — `on_start`, `on_complete`, and `on_retry` callbacks
- :material-speedometer: **uvloop support** — Optional acceleration through `osiiso.run()`
- :material-package-variant: **Zero dependencies** — No runtime dependencies; typed with `py.typed`

---

## Quick Example

=== "AsyncQueue"

    ```python
    import asyncio
    import osiiso

    async def fetch(name: str) -> str:
        await asyncio.sleep(0.1)
        return f"fetched {name}"

    async def main():
        async with osiiso.AsyncQueue(workers=4) as q:
            q.map(fetch, ["users", "posts", "comments"], retries=2, timeout=5)
            summary = await q.run(strict=True)
            print(summary.values)

    osiiso.run(main())
    ```

=== "ThreadQueue"

    ```python
    import time
    import osiiso

    def resize(path: str) -> str:
        time.sleep(0.1)
        return f"resized {path}"

    with osiiso.ThreadQueue(workers=4) as q:
        q.map(resize, ["a.png", "b.png", "c.png"], name="resize")
        summary = q.run(strict=True)

    print(summary.values)
    ```

=== "ProcessQueue"

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

---

## Architecture

```mermaid
graph LR
    A["submit() / map() / group()"] --> B["Priority Queue"]
    B --> C["Worker Pool"]
    C --> D["TaskHandle"]
    D --> E["TaskResult"]
    E --> F["RunSummary"]

    style A fill:#6366f1,color:#fff,stroke:none
    style B fill:#8b5cf6,color:#fff,stroke:none
    style C fill:#a855f7,color:#fff,stroke:none
    style D fill:#c084fc,color:#fff,stroke:none
    style E fill:#d8b4fe,color:#1e1b4b,stroke:none
    style F fill:#ede9fe,color:#1e1b4b,stroke:none
```

---

## Next Steps

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Get Started**

    ---

    Install osiiso and run your first task queue in under a minute.

    [:octicons-arrow-right-24: Quick Start](guides/quick-start.md)

-   :material-book-open-variant:{ .lg .middle } **User Guide**

    ---

    Learn about task submission, options, handles, groups, and lifecycle policies.

    [:octicons-arrow-right-24: Task Submission](guides/task-submission.md)

-   :material-code-braces:{ .lg .middle } **API Reference**

    ---

    Complete reference for every public class, method, and attribute.

    [:octicons-arrow-right-24: API Reference](reference/index.md)

-   :material-flask:{ .lg .middle } **Examples**

    ---

    See osiiso in action with the feature gallery and Hacker News pipeline.

    [:octicons-arrow-right-24: Examples](examples/feature-gallery.md)

</div>
