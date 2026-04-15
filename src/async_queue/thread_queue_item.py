from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from typing import Any


class ThreadQueueItem:
    """Wrap a callable or awaitable for threaded queue execution."""

    must_complete: bool

    def __init__(self, task: Any, /, *args: Any, **kwargs: Any) -> None:
        if not callable(task) and not inspect.isawaitable(task):
            raise TypeError("task must be a callable or an awaitable object")
        if inspect.isawaitable(task) and (args or kwargs):
            raise TypeError("awaitable tasks cannot receive extra args or kwargs")

        self.task = task
        self.args = args
        self.kwargs = dict(kwargs)
        self.created_at = time.perf_counter()
        self.task_id = uuid.uuid4().hex
        self.name = self._resolve_name(task)
        self.timeout: float | None = None
        self.retries = 0
        self.retry_delay = 0.0
        self.backoff = 1.0
        self.must_complete = False

    def __hash__(self) -> int:
        return hash(self.task_id)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ThreadQueueItem):
            return NotImplemented
        return (self.created_at, self.task_id) < (other.created_at, other.task_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ThreadQueueItem):
            return NotImplemented
        return self.task_id == other.task_id

    @staticmethod
    def _resolve_name(task: Any) -> str:
        if inspect.isawaitable(task):
            code = getattr(task, "cr_code", None)
            if code is not None:
                return code.co_name
            return task.__class__.__name__

        name = getattr(task, "__name__", None)
        if name:
            return name
        return task.__class__.__name__

    def configure(
        self,
        *,
        must_complete: bool = False,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: float = 1.0,
        name: str | None = None,
    ) -> ThreadQueueItem:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        if retries < 0:
            raise ValueError("retries must be greater than or equal to 0")
        if retry_delay < 0:
            raise ValueError("retry_delay must be greater than or equal to 0")
        if backoff <= 0:
            raise ValueError("backoff must be greater than 0")

        self.must_complete = must_complete
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.backoff = backoff
        if name:
            self.name = name
        return self

    def __call__(self) -> Any:
        if inspect.isawaitable(self.task):
            return asyncio.run(self.task)

        if inspect.iscoroutinefunction(self.task):
            return asyncio.run(self.task(*self.args, **self.kwargs))

        result = self.task(*self.args, **self.kwargs)
        if inspect.isawaitable(result):
            return asyncio.run(result)
        return result
