"""End-to-end workflows that exercise all three osiiso queues."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from osiiso import AsyncQueue, ProcessQueue, TaskOptions, ThreadQueue

from .analytics import keyword_counts, rank_item, summarize_scores
from .api import HNClient
from .store import SQLiteStore

logger = logging.getLogger(__name__)


def _complete_label(result: Any) -> str:
    return f"{result.name}:{result.status}"


async def fetch_hacker_news(limit: int, *, offline: bool = True) -> dict[str, Any]:
    """Fetch feeds, items, and users with AsyncQueue."""

    client = HNClient(offline=offline)
    events: list[str] = []

    def on_complete(result: Any) -> None:
        events.append(_complete_label(result))

    def on_retry(handle: Any, exc: BaseException) -> None:
        logger.warning("retrying %s after %s", handle.name, exc)

    feed_opts = TaskOptions(priority=0, timeout=5, retries=2, retry_delay=0.05, backoff=2, name="fetch-feed")
    item_opts = TaskOptions(priority=1, timeout=5, retries=2, retry_delay=0.05, backoff=2, group_id="items", name="fetch-item")
    user_opts = TaskOptions(priority=2, timeout=5, retries=2, retry_delay=0.05, backoff=2, group_id="users", name="fetch-user")

    async with AsyncQueue(workers=8, on_complete=on_complete, on_retry=on_retry, timeout=60) as q:
        feed_group = q.group([(client.feed, name) for name in ["top", "new", "best"]], group_id="feeds", opts=feed_opts)
        await q.run(strict=True)
        feed_values = await feed_group.values()

        item_ids = list(dict.fromkeys(item_id for feed in feed_values for item_id in feed))[:limit]

        q.reset()
        q.map(client.item, item_ids, opts=item_opts)
        item_summary = await q.run(strict=True)
        items = [dict(item) for item in item_summary.values]

        user_ids = sorted({str(item["by"]) for item in items if item.get("by")})
        q.reset()
        q.group([(client.user, user_id) for user_id in user_ids], group_id="users", opts=user_opts)
        user_summary = await q.run(strict=True)
        users = [dict(user) for user in user_summary.values]

    return {
        "item_ids": item_ids,
        "items": items,
        "users": users,
        "events": events,
    }


def persist_hacker_news(database: str | Path, items: list[dict[str, Any]], users: list[dict[str, Any]]) -> dict[str, Any]:
    """Persist records with ThreadQueue."""

    store = SQLiteStore(database)
    store.create_schema()

    completed: list[str] = []

    def on_complete(result: Any) -> None:
        completed.append(_complete_label(result))

    save_item = TaskOptions(priority=0, must_complete=True, timeout=3, retries=1, name="save-item")
    save_user = save_item.replace(priority=1, name="save-user")

    try:
        with ThreadQueue(workers=4, on_complete=on_complete) as q:
            item_group = q.group([(store.save_item, item) for item in items], group_id="items", opts=save_item)
            user_group = q.group([(store.save_user, user) for user in users], group_id="users", opts=save_user)
            q.submit(store.save_metric, "last_batch_items", len(items), must_complete=True, priority=0, delay=0.01, name="save-metric")
            summary = q.run(strict=True)

            item_values = item_group.values()
            user_values = user_group.values()

        return {
            "summary": summary,
            "item_values": item_values,
            "user_values": user_values,
            "counts": store.counts(),
            "events": completed,
        }
    finally:
        store.close()


def analyze_hacker_news(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze fetched records with ProcessQueue."""

    completed: list[str] = []

    def on_complete(result: Any) -> None:
        completed.append(_complete_label(result))

    analytics_opts = TaskOptions(priority=0, timeout=10, retries=1, name="analytics")
    ranking_opts = TaskOptions(priority=1, timeout=10, retries=1, group_id="rankings", name="rank-item")

    with ProcessQueue(workers=2, on_complete=on_complete) as q:
        analytics_group = q.group(
            [(summarize_scores, items), (keyword_counts, items)],
            group_id="analytics",
            opts=analytics_opts,
        )
        q.map(rank_item, [(item,) for item in items], opts=ranking_opts)
        summary = q.run(strict=True)
        analytics_values = analytics_group.values()

    rankings = [value for value in summary.values if isinstance(value, dict) and "hotness" in value]
    rankings.sort(key=lambda row: row["hotness"], reverse=True)

    return {
        "summary": summary,
        "score_summary": analytics_values[0],
        "keywords": analytics_values[1],
        "rankings": rankings,
        "events": completed,
    }


async def run_pipeline(limit: int, *, database: str | Path, offline: bool = True) -> dict[str, Any]:
    fetched = await fetch_hacker_news(limit, offline=offline)
    persisted = persist_hacker_news(database, fetched["items"], fetched["users"])
    analytics = analyze_hacker_news(fetched["items"])
    return {
        "database": str(database),
        "fetched": fetched,
        "persisted": persisted,
        "analytics": analytics,
    }


def print_report(result: dict[str, Any]) -> None:
    fetched = result["fetched"]
    persisted = result["persisted"]
    analytics = result["analytics"]

    print("Osiiso Hacker News showcase")
    print(f"Database: {result['database']}")
    print(f"Fetched items: {len(fetched['items'])}")
    print(f"Fetched users: {len(fetched['users'])}")
    print(f"SQLite counts: {persisted['counts']}")
    print(f"Score summary: {analytics['score_summary']}")
    print(f"Top keywords: {analytics['keywords']}")
    print("Top ranked items:")
    for row in analytics["rankings"][:5]:
        print(f"  {row['id']}: {row['hotness']} - {row['title']}")
