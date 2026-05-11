"""Compact feature gallery for all osiiso queue types."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import osiiso


async def async_fetch(label: str, delay: float = 0.02) -> str:
    await asyncio.sleep(delay)
    return f"async:{label}"


async def async_add(a: int, b: int) -> int:
    await asyncio.sleep(0.01)
    return a + b


def blocking_fetch(label: str, delay: float = 0.02) -> str:
    time.sleep(delay)
    return f"thread:{label}"


def square(n: int) -> int:
    return n * n


def add(a: int, b: int) -> int:
    return a + b


def fail_once_factory() -> Callable[[], str]:
    attempts = {"count": 0}

    def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("first attempt fails")
        return "retry-ok"

    return flaky


async def async_queue_gallery() -> None:
    print("\nAsyncQueue")
    events: list[str] = []

    q = osiiso.AsyncQueue(
        workers=3,
        on_start=lambda h: events.append(f"start:{h.name}"),
        on_complete=lambda r: events.append(f"done:{r.name}:{r.status}"),
        on_retry=lambda h, exc: events.append(f"retry:{h.name}:{exc}"),
    )

    @q.task(retries=1, retry_delay=0.01, timeout=1, name="decorated-async")
    async def decorated(label: str) -> str:
        return await async_fetch(label)

    q.submit(async_fetch, "urgent", priority=0, must_complete=True, name="urgent-fetch")
    q.map(async_fetch, ["map-a", "map-b"], opts=osiiso.TaskOptions(group_id="mapped", priority=2, name="mapped-fetch"))
    q.group([(async_add, 2, 3), (async_fetch, "grouped")], group_id="mixed")
    decorated("decorator-call", priority=1)
    summary = await q.run(strict=True)
    summary.display()
    print("Grouped results:", summary.by_group().keys())
    print("Events:", events[:6])

    q.reset()
    handles = q.map(async_fetch, [(label, delay) for label, delay in [("slow", 0.04), ("fast", 0.01), ("mid", 0.02)]])
    await q.start()
    seen: list[str] = []
    async for handle in osiiso.AsyncQueue.as_completed(handles):
        seen.append(handle.value())
    await q.shutdown()
    print("as_completed order:", seen)


def thread_queue_gallery() -> None:
    print("\nThreadQueue")
    flaky = fail_once_factory()
    with osiiso.ThreadQueue(workers=3) as q:
        q.submit(flaky, retries=1, retry_delay=0.01, name="flaky-thread")
        q.map(blocking_fetch, ["map-a", "map-b"], group_id="mapped", name="mapped-thread")
        group = q.group([(blocking_fetch, "grouped"), (add, 10, 5)], group_id="mixed")
        q.submit(blocking_fetch, "delayed", delay=0.01, priority=0, must_complete=True, name="delayed-thread")
        summary = q.run(strict=True)
        print("Group values:", group.values())
    summary.display()


def process_queue_gallery() -> None:
    print("\nProcessQueue")
    with osiiso.ProcessQueue(workers=2) as q:
        q.map(square, [2, 3, 4], group_id="squares", name="square")
        group = q.group([(add, 20, 22), (square, 9)], group_id="mixed-process")
        summary = q.run(strict=True)
        print("Process group values:", group.values())
    summary.display()


async def main() -> None:
    await async_queue_gallery()
    thread_queue_gallery()
    process_queue_gallery()


if __name__ == "__main__":
    osiiso.run(main())
