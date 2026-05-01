from __future__ import annotations

import asyncio
import time

from async_queue import ProcessTaskQueue, TaskQueue, ThreadTaskQueue


async def async_value(value: int) -> int:
    return value


async def async_fail_fast() -> None:
    raise RuntimeError("fail fast")


async def async_sleep_value(delay: float, value: int) -> int:
    await asyncio.sleep(delay)
    return value


def sync_value(value: int) -> int:
    return value


def sync_fail_fast() -> None:
    raise RuntimeError("fail fast")


def sync_sleep_value(delay: float, value: int) -> int:
    time.sleep(delay)
    return value


class TestAsyncStructuredFeatures:
    async def test_timed_tasks_wait_until_delay(self):
        q = TaskQueue(max_workers=1)
        started = time.perf_counter()
        handle = q.submit(async_value, 10, delay=0.05)
        summary = await q.run()
        assert summary.succeeded == 1
        assert handle.value() == 10
        assert time.perf_counter() - started >= 0.045
        assert summary.results[0].scheduled_for is not None

    async def test_fail_first_stops_pending_work(self):
        q = TaskQueue(max_workers=1, fail_policy="fail_first")
        failed = q.submit(async_fail_fast, priority=1)
        pending = q.submit(async_sleep_value, 0.2, 2, priority=2)
        summary = await q.run()
        assert failed.status == "failed"
        assert pending.status == "cancelled"
        assert summary.failed == 1
        assert summary.cancelled == 1

    async def test_group_wait_and_summary_indexes(self):
        q = TaskQueue(max_workers=2)
        group = q.group(async_value, [1, 2, 3], group_id="numbers")
        await q.start()
        summary = await group.wait()
        assert summary.ok
        assert summary.values == (1, 2, 3)
        assert set(summary.by_task_id()) == {handle.task_id for handle in group}
        assert set(summary.by_group()) == {"numbers"}
        await q.shutdown()

    async def test_background_fire_and_forget(self):
        q = TaskQueue(mode="infinite", max_workers=1)
        handle = await q.background(async_value, 7)
        result = await handle.wait()
        assert result.detached
        assert handle.value() == 7
        await q.shutdown()


class TestThreadStructuredFeatures:
    def test_timed_tasks_wait_until_delay(self):
        q = ThreadTaskQueue(max_workers=1)
        started = time.perf_counter()
        handle = q.submit(sync_value, 10, delay=0.05)
        summary = q.run()
        assert summary.succeeded == 1
        assert handle.value() == 10
        assert time.perf_counter() - started >= 0.045

    def test_fail_first_stops_pending_work(self):
        q = ThreadTaskQueue(max_workers=1, fail_policy="fail_first")
        failed = q.submit(sync_fail_fast, priority=1)
        pending = q.submit(sync_sleep_value, 0.2, 2, priority=2)
        summary = q.run()
        assert failed.status == "failed"
        assert pending.status == "cancelled"
        assert summary.failed == 1
        assert summary.cancelled == 1

    def test_group_wait(self):
        q = ThreadTaskQueue(max_workers=2)
        group = q.group(sync_value, [1, 2, 3], group_id="numbers")
        q.start()
        summary = group.wait(timeout=5.0)
        assert summary.ok
        assert set(summary.values) == {1, 2, 3}
        assert set(summary.by_group()) == {"numbers"}
        q.shutdown()

    def test_background_fire_and_forget(self):
        q = ThreadTaskQueue(mode="infinite", max_workers=1)
        handle = q.background(sync_value, 7)
        result = handle.wait(timeout=5.0)
        assert result.detached
        assert handle.value() == 7
        q.shutdown()


class TestProcessStructuredFeatures:
    def test_timed_tasks_wait_until_delay(self):
        q = ProcessTaskQueue(max_workers=1)
        started = time.perf_counter()
        handle = q.submit(sync_value, 10, delay=0.05)
        summary = q.run()
        assert summary.succeeded == 1
        assert handle.value() == 10
        assert time.perf_counter() - started >= 0.045

    def test_fail_first_stops_pending_work(self):
        q = ProcessTaskQueue(max_workers=1, fail_policy="fail_first")
        failed = q.submit(sync_fail_fast, priority=1)
        pending = q.submit(sync_sleep_value, 0.2, 2, priority=2)
        summary = q.run()
        assert failed.status == "failed"
        assert pending.status == "cancelled"
        assert summary.failed == 1
        assert summary.cancelled == 1

    def test_group_wait(self):
        q = ProcessTaskQueue(max_workers=2)
        group = q.group(sync_value, [1, 2, 3], group_id="numbers")
        q.start()
        summary = group.wait(timeout=10.0)
        assert summary.ok
        assert set(summary.values) == {1, 2, 3}
        assert set(summary.by_group()) == {"numbers"}
        q.shutdown()

    def test_background_fire_and_forget(self):
        q = ProcessTaskQueue(mode="infinite", max_workers=1)
        handle = q.background(sync_value, 7)
        result = handle.wait(timeout=10.0)
        assert result.detached
        assert handle.value() == 7
        q.shutdown()
