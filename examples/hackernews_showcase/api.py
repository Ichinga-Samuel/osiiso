"""Async Hacker News client used by the showcase."""

from __future__ import annotations

import asyncio
import http.client
import json
from typing import Any

from .fixtures import FEEDS, ITEMS, MAX_ITEM, UPDATES, USERS


class HNClient:
    """Tiny async client with an offline fixture mode and optional live mode."""

    host = "hacker-news.firebaseio.com"

    def __init__(self, *, offline: bool = True, latency: float = 0.02) -> None:
        self.offline = offline
        self.latency = latency

    async def _offline_get(self, path: str) -> list[int] | int | dict[str, Any]:
        await asyncio.sleep(self.latency)
        if path == "maxitem.json":
            return MAX_ITEM
        if path == "updates.json":
            return dict(UPDATES)
        if path.endswith("stories.json"):
            feed = path.removesuffix("stories.json")
            return list(FEEDS.get(feed, []))
        if path.startswith("item/"):
            item_id = int(path.removeprefix("item/").removesuffix(".json"))
            return dict(ITEMS[item_id])
        if path.startswith("user/"):
            user_id = path.removeprefix("user/").removesuffix(".json")
            return dict(USERS[user_id])
        raise KeyError(f"unknown fixture path: {path}")

    async def _live_get(self, path: str) -> list[int] | int | dict[str, Any]:
        def request() -> list[int] | int | dict[str, Any]:
            connection = http.client.HTTPSConnection(self.host, timeout=10)
            try:
                connection.request("GET", f"/v0/{path}")
                response = connection.getresponse()
                payload = response.read().decode("utf-8")
                return json.loads(payload)
            finally:
                connection.close()

        return await asyncio.to_thread(request)

    async def get(self, path: str) -> list[int] | int | dict[str, Any]:
        if self.offline:
            return await self._offline_get(path)
        return await self._live_get(path)

    async def item(self, item_id: int) -> dict[str, Any]:
        result = await self.get(f"item/{item_id}.json")
        if not isinstance(result, dict):
            raise TypeError(f"item {item_id} returned {type(result).__name__}")
        return result

    async def user(self, user_id: str) -> dict[str, Any]:
        result = await self.get(f"user/{user_id}.json")
        if not isinstance(result, dict):
            raise TypeError(f"user {user_id} returned {type(result).__name__}")
        return result

    async def feed(self, name: str) -> list[int]:
        result = await self.get(f"{name}stories.json")
        if not isinstance(result, list):
            raise TypeError(f"feed {name} returned {type(result).__name__}")
        return [int(item_id) for item_id in result]

    async def updates(self) -> dict[str, Any]:
        result = await self.get("updates.json")
        if not isinstance(result, dict):
            raise TypeError(f"updates returned {type(result).__name__}")
        return result

    async def max_item(self) -> int:
        result = await self.get("maxitem.json")
        if not isinstance(result, int):
            raise TypeError(f"maxitem returned {type(result).__name__}")
        return result
