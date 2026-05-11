"""CPU-style analytics functions for ProcessQueue.

These functions are top-level so they can be pickled by multiprocessing.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any

WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*")


def tokenize(text: str) -> list[str]:
    return [match.group(0).lower() for match in WORD_RE.finditer(text)]


def rank_item(item: dict[str, Any]) -> dict[str, Any]:
    text = f"{item.get('title', '')} {item.get('text', '')}"
    words = tokenize(text)
    score = int(item.get("score") or 0)
    comments = int(item.get("descendants") or 0)
    hotness = math.log(score + 2) * (1 + comments / 10) + len(set(words)) / 5
    return {
        "id": item["id"],
        "title": item.get("title") or item.get("text", "")[:60],
        "type": item.get("type", "item"),
        "hotness": round(hotness, 3),
        "word_count": len(words),
    }


def keyword_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    stopwords = {"a", "an", "and", "for", "from", "how", "on", "or", "the", "to", "with", "you"}
    counter: Counter[str] = Counter()
    for item in items:
        text = f"{item.get('title', '')} {item.get('text', '')}"
        counter.update(word for word in tokenize(text) if word not in stopwords and len(word) > 2)
    return dict(counter.most_common(10))


def summarize_scores(items: list[dict[str, Any]]) -> dict[str, float | int]:
    scores = [int(item.get("score") or 0) for item in items]
    if not scores:
        return {"count": 0, "min": 0, "max": 0, "average": 0.0}
    return {
        "count": len(scores),
        "min": min(scores),
        "max": max(scores),
        "average": round(sum(scores) / len(scores), 2),
    }
