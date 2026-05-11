# Hacker News Showcase

This is a complete, runnable example project based on the original `test project`
folder. It keeps the Hacker News data shape but defaults to local fixtures, so it
can be used in docs, CI, and workshops without depending on the network.

Run it from the repository root:

```bash
uv run python -m examples.hackernews_showcase --limit 6
```

Use the live Hacker News API:

```bash
uv run python -m examples.hackernews_showcase --limit 20 --online
```

The project uses all three queue backends:

- `AsyncQueue` fetches feeds, items, and users with priorities, retries, groups,
  maps, hooks, timeouts, and `TaskOptions`.
- `ThreadQueue` persists fetched records into SQLite with `must_complete`
  writes, delayed metrics, groups, handles, and strict summaries.
- `ProcessQueue` ranks items and builds aggregate analytics in subprocesses.

The modules are intentionally small:

- `api.py` is the async Hacker News client.
- `fixtures.py` provides deterministic offline data.
- `models.py` normalizes item and user records.
- `store.py` writes to SQLite safely from worker threads.
- `analytics.py` contains top-level process-safe CPU functions.
- `workflows.py` composes the queues into a pipeline.
