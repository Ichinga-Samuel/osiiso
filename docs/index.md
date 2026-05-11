# osiiso

`osiiso` is a small Python package for structured task queues across three
execution backends:

- `AsyncQueue` for async I/O and coroutine-heavy work.
- `ThreadQueue` for blocking I/O and synchronous integrations.
- `ProcessQueue` for CPU-heavy work that benefits from separate processes.

All three queues share the same core shape: submit tasks, configure task
behavior with `TaskOptions`, run the queue, then inspect a `RunSummary`.

```python
import osiiso

async def fetch(url: str) -> str:
    return f"fetched {url}"

async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(fetch, "https://example.com", retries=3, timeout=10)
        summary = await q.run()
        return summary.values

print(osiiso.run(main()))
```

## Why use it?

- One API for async, thread, and process queues.
- Priority scheduling with lower numbers running first.
- Retries with optional delay and exponential backoff.
- Per-task timeouts and queue-level run timeouts.
- Graceful shutdown with `must_complete` task protection.
- Groups, handles, summaries, and event hooks for observability.
- Typed package with `py.typed`.

## Choose a queue

| Workload | Queue | Example |
| --- | --- | --- |
| HTTP APIs, async database clients, websockets | `AsyncQueue` | Fetch 1,000 URLs with retries |
| Blocking SDKs, file work, SQLite writes | `ThreadQueue` | Persist records from many tasks |
| CPU-bound Python functions | `ProcessQueue` | Score, rank, transform, or parse data |

## Example project

The repository includes a complete Hacker News style pipeline in
`examples/hackernews_showcase`. It uses:

- `AsyncQueue` to fetch feeds, items, and users.
- `ThreadQueue` to write results into SQLite.
- `ProcessQueue` to rank stories and compute keywords.

Run it:

```bash
uv run python -m examples.hackernews_showcase --limit 6
```

Next: [Quick Start](guides/quick-start.md).
