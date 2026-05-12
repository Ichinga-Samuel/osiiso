# Hacker News Pipeline

The `examples/hackernews_showcase` project demonstrates a complete multi-backend
pipeline using all three osiiso queue types. It fetches Hacker News data,
persists it to SQLite, and runs CPU-bound analytics.

---

## Run It

### Offline (Fixtures)

```bash
uv run python -m examples.hackernews_showcase --limit 6
```

### Online (Live API)

```bash
uv run python -m examples.hackernews_showcase --limit 20 --online
```

---

## Pipeline Architecture

```mermaid
graph LR
    A["AsyncQueue<br/>Fetch feeds, items, users"] --> B["ThreadQueue<br/>Write to SQLite"]
    B --> C["ProcessQueue<br/>Score, rank, keywords"]

    style A fill:#6366f1,color:#fff,stroke:none
    style B fill:#8b5cf6,color:#fff,stroke:none
    style C fill:#a855f7,color:#fff,stroke:none
```

### Stage 1: Async Fetch

`AsyncQueue` fetches feeds, items, and user profiles from the Hacker News API
(or local fixtures):

```python
feed_group = q.group(
    [(client.feed, name) for name in ["top", "new", "best"]],
    group_id="feeds",
    opts=feed_opts,
)
await q.run(strict=True)
feed_values = await feed_group.values()
```

**Features demonstrated:** priorities, retries, backoff, task timeouts, groups,
maps, hooks, and queue reset.

### Stage 2: Thread Persistence

`ThreadQueue` writes items, users, and metrics to a SQLite database:

```python
item_group = q.group(
    [(store.save_item, item) for item in items],
    group_id="items",
    opts=save_item,
)
q.submit(store.save_metric, "last_batch_items", len(items), must_complete=True)
summary = q.run(strict=True)
```

**Features demonstrated:** blocking sync work, `must_complete` protection,
delayed tasks, group values, and strict summaries.

### Stage 3: Process Analytics

`ProcessQueue` runs CPU-bound analysis across subprocesses:

```python
analytics_group = q.group(
    [(summarize_scores, items), (keyword_counts, items)],
    group_id="analytics",
)
q.map(rank_item, [(item,) for item in items], group_id="rankings")
summary = q.run(strict=True)
```

**Features demonstrated:** process-safe top-level functions, tuple-wrapped dict
arguments, grouped summaries.

!!! tip "Dict arguments in `map()`"
    `map()` treats dictionaries as keyword argument mappings. When the dict
    itself should be the positional argument, wrap it in a tuple: `(item,)`

---

## Project Files

| File | Description |
|------|-------------|
| `api.py` | Async Hacker News client (HTTP or fixtures) |
| `fixtures.py` | Offline test data for repeatable runs |
| `models.py` | Record normalization and data types |
| `store.py` | Thread-safe SQLite writer |
| `analytics.py` | Process-safe CPU functions (top-level) |
| `workflows.py` | Queue orchestration across all three stages |
| `__main__.py` | CLI entry point with `--limit` and `--online` flags |

---

## Key Patterns

### Queue Reset Between Stages

The async stage resets the queue between fetch rounds:

```python
await q.run(strict=True)
feed_values = await feed_group.values()

q.reset()  # Clear results, ready for next batch

# Submit new tasks...
await q.run(strict=True)
```

### Protected Writes

Critical database operations use `must_complete=True`:

```python
q.submit(store.save_metric, "last_batch_items", len(items), must_complete=True)
```

### Hooks for Observability

The pipeline uses hooks to track execution:

```python
q = osiiso.AsyncQueue(
    workers=8,
    on_start=lambda h: print(f"  → {h.name}"),
    on_complete=lambda r: print(f"  ✓ {r.name} ({r.duration:.3f}s)"),
)
```
