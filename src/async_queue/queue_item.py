from __future__ import annotations

import asyncio
import inspect
from typing import Any

from ._base import _BaseQueueItem


class QueueItem(_BaseQueueItem):
    """Wrap a callable or awaitable for async queue execution."""

    async def __call__(self) -> Any:
        if inspect.isawaitable(self.task):
            return await self.task

        if inspect.iscoroutinefunction(self.task):
            return await self.task(*self.args, **self.kwargs)

        result = await asyncio.to_thread(self.task, *self.args, **self.kwargs)
        if inspect.isawaitable(result):
            return await result
        return result
