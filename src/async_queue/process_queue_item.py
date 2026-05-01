from __future__ import annotations

import asyncio
import inspect
from typing import Any

from ._base import _BaseQueueItem


class ProcessQueueItem(_BaseQueueItem):
    """Wrap a pickleable callable for process-based queue execution."""

    def __init__(self, task: Any, /, *args: Any, **kwargs: Any) -> None:
        if inspect.isawaitable(task):
            raise TypeError("process queue tasks must be callable, not awaitable objects")
        super().__init__(task, *args, **kwargs)

    def __call__(self) -> Any:
        if inspect.iscoroutinefunction(self.task):
            return asyncio.run(self.task(*self.args, **self.kwargs))

        result = self.task(*self.args, **self.kwargs)
        if inspect.isawaitable(result):
            return asyncio.run(result)
        return result
