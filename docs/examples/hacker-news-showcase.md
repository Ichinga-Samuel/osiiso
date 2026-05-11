# Hacker News Showcase

The `examples/hackernews_showcase` project is based on the original
`test project` folder. It keeps the Hacker News API shape but defaults to
fixtures so the example is repeatable.

Run it:

```bash
uv run python -m examples.hackernews_showcase --limit 6
```

Use the live Hacker News API:

```bash
uv run python -m examples.hackernews_showcase --limit 20 --online
```

## Pipeline

1. `AsyncQueue` fetches feeds, items, and users.
2. `ThreadQueue` writes items, users, and metrics to SQLite.
3. `ProcessQueue` computes score summaries, keyword counts, and story rankings.

## Async fetch stage

```python
feed_group = q.group(
    [(client.feed, name) for name in ["top", "new", "best"]],
    group_id="feeds",
    opts=feed_opts,
)
await q.run(strict=True)
feed_values = await feed_group.values()
```

This stage demonstrates priorities, retries, backoff, task timeouts, groups,
maps, hooks, and queue reset.

## Thread persistence stage

```python
item_group = q.group(
    [(store.save_item, item) for item in items],
    group_id="items",
    opts=save_item,
)
q.submit(store.save_metric, "last_batch_items", len(items), must_complete=True)
summary = q.run(strict=True)
```

This stage demonstrates blocking sync work, `must_complete`, delayed work,
group values, and strict summaries.

## Process analytics stage

```python
analytics_group = q.group(
    [(summarize_scores, items), (keyword_counts, items)],
    group_id="analytics",
)
q.map(rank_item, [(item,) for item in items], group_id="rankings")
summary = q.run(strict=True)
```

The tuple-wrapped dicts are important: `map()` treats dictionaries as keyword
argument mappings. Use `(item,)` when the dictionary itself should be the single
positional argument.

## Files

- `api.py`: async Hacker News client.
- `fixtures.py`: offline data.
- `models.py`: record normalization.
- `store.py`: thread-safe SQLite writer.
- `analytics.py`: process-safe top-level CPU functions.
- `workflows.py`: queue orchestration.
