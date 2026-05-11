"""Small Hacker News style fixture set used by the showcase.

The original test project talks to the public Hacker News API. These fixtures
keep the example deterministic by default while preserving the same data shape.
"""

from __future__ import annotations

MAX_ITEM = 1007

FEEDS: dict[str, list[int]] = {
    "top": [1001, 1002, 1003, 1004],
    "new": [1005, 1006, 1007],
    "show": [1003, 1006],
    "ask": [1002, 1005],
    "job": [1004],
    "best": [1001, 1007, 1006],
}

ITEMS: dict[int, dict[str, object]] = {
    1001: {
        "id": 1001,
        "type": "story",
        "by": "ada",
        "time": 1_762_000_001,
        "title": "SQLite queues for small teams",
        "text": "A practical note on durable task processing with Python.",
        "score": 243,
        "url": "https://example.com/sqlite-queues",
        "kids": [2001, 2002],
        "descendants": 2,
    },
    1002: {
        "id": 1002,
        "type": "story",
        "by": "grace",
        "time": 1_762_000_212,
        "title": "Ask HN: How do you teach concurrency?",
        "text": "Looking for examples that show async, threads, and processes clearly.",
        "score": 98,
        "kids": [2003],
        "descendants": 1,
    },
    1003: {
        "id": 1003,
        "type": "story",
        "by": "linus",
        "time": 1_762_000_322,
        "title": "Show HN: A typed task queue for Python",
        "text": "A tiny package with retries, priorities, groups, and summaries.",
        "score": 311,
        "url": "https://example.com/osiiso",
        "kids": [],
        "descendants": 0,
    },
    1004: {
        "id": 1004,
        "type": "job",
        "by": "margaret",
        "time": 1_762_000_430,
        "title": "Systems engineer, Python platform",
        "text": "Build internal tooling for data teams.",
        "score": 17,
        "url": "https://example.com/jobs/platform",
        "kids": [],
        "descendants": 0,
    },
    1005: {
        "id": 1005,
        "type": "poll",
        "by": "ada",
        "time": 1_762_000_550,
        "title": "Which queue backend do you reach for first?",
        "text": "Pick the concurrency model you use most often.",
        "score": 44,
        "parts": [3001, 3002, 3003],
        "kids": [],
        "descendants": 0,
    },
    1006: {
        "id": 1006,
        "type": "story",
        "by": "evelyn",
        "time": 1_762_000_690,
        "title": "Backoff strategies that respect users",
        "text": "Retries should help a service recover, not make an outage louder.",
        "score": 156,
        "url": "https://example.com/backoff",
        "kids": [],
        "descendants": 0,
    },
    1007: {
        "id": 1007,
        "type": "story",
        "by": "grace",
        "time": 1_762_000_777,
        "title": "When multiprocessing beats clever asyncio",
        "text": "CPU-heavy text scoring benefits from separate Python processes.",
        "score": 202,
        "url": "https://example.com/processes",
        "kids": [],
        "descendants": 0,
    },
    2001: {
        "id": 2001,
        "type": "comment",
        "by": "evelyn",
        "time": 1_762_000_801,
        "text": "The important bit is measuring where your bottleneck is.",
        "parent": 1001,
        "kids": [],
    },
    2002: {
        "id": 2002,
        "type": "comment",
        "by": "margaret",
        "time": 1_762_000_840,
        "text": "SQLite is a good default until it is not.",
        "parent": 1001,
        "kids": [],
    },
    2003: {
        "id": 2003,
        "type": "comment",
        "by": "linus",
        "time": 1_762_000_900,
        "text": "Start with one queue API, then swap the execution backend.",
        "parent": 1002,
        "kids": [],
    },
    3001: {"id": 3001, "type": "pollopt", "by": "ada", "poll": 1005, "text": "AsyncQueue", "score": 72},
    3002: {"id": 3002, "type": "pollopt", "by": "ada", "poll": 1005, "text": "ThreadQueue", "score": 31},
    3003: {"id": 3003, "type": "pollopt", "by": "ada", "poll": 1005, "text": "ProcessQueue", "score": 18},
}

USERS: dict[str, dict[str, object]] = {
    "ada": {
        "id": "ada",
        "created": 1_500_000_000,
        "karma": 42_000,
        "about": "Builds calm tools for busy systems.",
        "submitted": [1001, 1005, 3001, 3002, 3003],
    },
    "grace": {
        "id": "grace",
        "created": 1_490_000_000,
        "karma": 31_000,
        "about": "Writes about compilers and coordination.",
        "submitted": [1002, 1007],
    },
    "linus": {
        "id": "linus",
        "created": 1_480_000_000,
        "karma": 24_000,
        "about": "Makes systems smaller.",
        "submitted": [1003, 2003],
    },
    "margaret": {
        "id": "margaret",
        "created": 1_470_000_000,
        "karma": 19_000,
        "about": "Operations, databases, and hiring.",
        "submitted": [1004, 2002],
    },
    "evelyn": {
        "id": "evelyn",
        "created": 1_460_000_000,
        "karma": 12_000,
        "about": "Likes reliability work and clear logs.",
        "submitted": [1006, 2001],
    },
}

UPDATES = {"items": [1006, 1007], "profiles": ["ada", "grace"]}
