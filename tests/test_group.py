"""Tests for osiiso.group — TaskGroup and SyncTaskGroup."""

import asyncio
import threading
import time

import pytest

from osiiso.exceptions import ExecutionError
from osiiso.group import SyncTaskGroup, TaskGroup
from osiiso.handle import SyncTaskHandle, TaskHandle
from osiiso.result import TaskResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _async_handle(
    task_id: str = "h1",
    name: str = "task",
    cancel_fn=None,
) -> TaskHandle:
    """Create a TaskHandle with minimal defaults."""
    if cancel_fn is None:
        cancel_fn = lambda tid: False
    return TaskHandle(
        task_id=task_id,
        name=name,
        priority=3,
        must_complete=False,
        created_at=time.perf_counter(),
        cancel_fn=cancel_fn,
    )


def _sync_handle(
    task_id: str = "s1",
    name: str = "task",
    cancel_fn=None,
) -> SyncTaskHandle:
    """Create a SyncTaskHandle with minimal defaults."""
    if cancel_fn is None:
        cancel_fn = lambda tid: False
    return SyncTaskHandle(
        task_id=task_id,
        name=name,
        priority=3,
        must_complete=False,
        created_at=time.perf_counter(),
        cancel_fn=cancel_fn,
    )


def _succeeded(task_id: str = "h1", value=42) -> TaskResult:
    return TaskResult(task_id=task_id, name="task", status="succeeded", value=value)


def _failed(task_id: str = "h1", exception: BaseException | None = None) -> TaskResult:
    exc = exception or RuntimeError("boom")
    return TaskResult(task_id=task_id, name="task", status="failed", exception=exc)


def _cancelled(task_id: str = "h1") -> TaskResult:
    return TaskResult(task_id=task_id, name="task", status="cancelled")


# ===================================================================
# TestTaskGroup (async)
# ===================================================================

class TestTaskGroup:
    """Async TaskGroup tests."""

    def test_len_returns_handle_count(self):
        """__len__ returns the number of handles in the group."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        h3 = _async_handle(task_id="a3")
        group = TaskGroup("g1", [h1, h2, h3])
        assert len(group) == 3

    def test_len_empty(self):
        """__len__ returns 0 for an empty group."""
        group = TaskGroup("g1", [])
        assert len(group) == 0

    def test_iter_yields_handles(self):
        """__iter__ yields every handle in submission order."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        group = TaskGroup("g1", [h1, h2])
        handles = list(group)
        assert handles == [h1, h2]

    def test_cancel_returns_count(self):
        """cancel() returns the number of handles successfully cancelled."""
        # h1 cancel_fn returns True, h2 returns False (already done), h3 returns True
        h1 = _async_handle(task_id="a1", cancel_fn=lambda tid: True)
        h2 = _async_handle(task_id="a2")
        h2._mark_finished(_succeeded(task_id="a2"))  # already done → cancel returns False
        h3 = _async_handle(task_id="a3", cancel_fn=lambda tid: True)
        group = TaskGroup("g1", [h1, h2, h3])
        assert group.cancel() == 2

    def test_cancel_all_done(self):
        """cancel() returns 0 when all handles are already done."""
        h1 = _async_handle(task_id="a1")
        h1._mark_finished(_succeeded(task_id="a1"))
        h2 = _async_handle(task_id="a2")
        h2._mark_finished(_failed(task_id="a2"))
        group = TaskGroup("g1", [h1, h2])
        assert group.cancel() == 0

    @pytest.mark.asyncio
    async def test_wait_returns_run_summary(self):
        """wait() returns a RunSummary aggregating all handle results."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        h1._mark_finished(_succeeded(task_id="a1", value=10))
        h2._mark_finished(_succeeded(task_id="a2", value=20))

        group = TaskGroup("g1", [h1, h2])
        summary = await group.wait()

        assert summary.total_submitted == 2
        assert summary.succeeded == 2
        assert summary.failed == 0
        assert summary.cancelled == 0
        assert summary.timed_out is False
        assert len(summary.results) == 2

    @pytest.mark.asyncio
    async def test_wait_mixed_statuses(self):
        """wait() correctly counts mixed succeeded/failed/cancelled statuses."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        h3 = _async_handle(task_id="a3")
        h1._mark_finished(_succeeded(task_id="a1"))
        h2._mark_finished(_failed(task_id="a2"))
        h3._mark_finished(_cancelled(task_id="a3"))

        group = TaskGroup("g1", [h1, h2, h3])
        summary = await group.wait()

        assert summary.succeeded == 1
        assert summary.failed == 1
        assert summary.cancelled == 1

    @pytest.mark.asyncio
    async def test_values_returns_values_on_all_success(self):
        """values() returns a tuple of return values when all tasks succeed."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        h1._mark_finished(_succeeded(task_id="a1", value="alpha"))
        h2._mark_finished(_succeeded(task_id="a2", value="beta"))

        group = TaskGroup("g1", [h1, h2])
        vals = await group.values()
        assert vals == ("alpha", "beta")

    @pytest.mark.asyncio
    async def test_values_raises_on_failure(self):
        """values() raises ExecutionError when any task failed."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        h1._mark_finished(_succeeded(task_id="a1", value=1))
        h2._mark_finished(_failed(task_id="a2"))

        group = TaskGroup("g1", [h1, h2])
        with pytest.raises(ExecutionError) as exc_info:
            await group.values()
        assert len(exc_info.value.results) == 1
        assert exc_info.value.results[0].task_id == "a2"

    @pytest.mark.asyncio
    async def test_wait_blocks_until_all_finished(self):
        """wait() blocks until all handles have been marked finished."""
        h1 = _async_handle(task_id="a1")
        h2 = _async_handle(task_id="a2")
        h1._mark_finished(_succeeded(task_id="a1"))

        async def finish_h2_later():
            await asyncio.sleep(0.05)
            h2._mark_finished(_succeeded(task_id="a2", value=99))

        group = TaskGroup("g1", [h1, h2])
        asyncio.create_task(finish_h2_later())
        summary = await group.wait()
        assert summary.succeeded == 2


# ===================================================================
# TestSyncTaskGroup
# ===================================================================

class TestSyncTaskGroup:
    """Blocking SyncTaskGroup tests using threading."""

    def test_len_returns_handle_count(self):
        """__len__ returns the number of handles."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")
        group = SyncTaskGroup("g1", [h1, h2])
        assert len(group) == 2

    def test_iter_yields_handles(self):
        """__iter__ yields handles in order."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")
        group = SyncTaskGroup("g1", [h1, h2])
        assert list(group) == [h1, h2]

    def test_cancel_returns_count(self):
        """cancel() returns the number of successfully cancelled handles."""
        h1 = _sync_handle(task_id="s1", cancel_fn=lambda tid: True)
        h2 = _sync_handle(task_id="s2")
        h2._mark_finished(_succeeded(task_id="s2"))  # already done
        group = SyncTaskGroup("g1", [h1, h2])
        assert group.cancel() == 1

    def test_wait_returns_run_summary(self):
        """wait() returns a RunSummary when all handles are done."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")
        h1._mark_finished(_succeeded(task_id="s1", value=10))
        h2._mark_finished(_succeeded(task_id="s2", value=20))

        group = SyncTaskGroup("g1", [h1, h2])
        summary = group.wait()

        assert summary.total_submitted == 2
        assert summary.succeeded == 2
        assert summary.ok is True

    def test_wait_timeout_distributes_budget(self):
        """wait(timeout=...) distributes the budget; each handle gets remaining time."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")

        # Finish h1 immediately, but h2 never finishes → timeout should fire
        h1._mark_finished(_succeeded(task_id="s1"))

        group = SyncTaskGroup("g1", [h1, h2])
        with pytest.raises(TimeoutError):
            group.wait(timeout=0.1)

    def test_wait_timeout_all_finish_in_time(self):
        """wait(timeout=...) succeeds when all handles finish within budget."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")

        def finish_both():
            time.sleep(0.02)
            h1._mark_finished(_succeeded(task_id="s1", value=1))
            time.sleep(0.02)
            h2._mark_finished(_succeeded(task_id="s2", value=2))

        t = threading.Thread(target=finish_both)
        t.start()
        summary = group_wait_with_thread(h1, h2, timeout=2.0)
        t.join()
        assert summary.succeeded == 2

    def test_values_returns_values(self):
        """values() returns a tuple of values when all succeed."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")
        h1._mark_finished(_succeeded(task_id="s1", value="x"))
        h2._mark_finished(_succeeded(task_id="s2", value="y"))

        group = SyncTaskGroup("g1", [h1, h2])
        assert group.values() == ("x", "y")

    def test_values_raises_on_failure(self):
        """values() raises ExecutionError when any task failed."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")
        h1._mark_finished(_succeeded(task_id="s1", value="ok"))
        h2._mark_finished(_failed(task_id="s2"))

        group = SyncTaskGroup("g1", [h1, h2])
        with pytest.raises(ExecutionError):
            group.values()

    def test_values_with_timeout_raises_on_timeout(self):
        """values(timeout=...) raises TimeoutError when budget is exhausted."""
        h1 = _sync_handle(task_id="s1")
        h1._mark_finished(_succeeded(task_id="s1"))
        h2 = _sync_handle(task_id="s2")
        # h2 never finishes

        group = SyncTaskGroup("g1", [h1, h2])
        with pytest.raises(TimeoutError):
            group.values(timeout=0.1)

    def test_wait_with_threading(self):
        """wait() blocks until all handles are finished via background threads."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")

        def finish_h1():
            time.sleep(0.03)
            h1._mark_finished(_succeeded(task_id="s1", value=1))

        def finish_h2():
            time.sleep(0.06)
            h2._mark_finished(_succeeded(task_id="s2", value=2))

        t1 = threading.Thread(target=finish_h1)
        t2 = threading.Thread(target=finish_h2)
        t1.start()
        t2.start()

        group = SyncTaskGroup("g1", [h1, h2])
        summary = group.wait(timeout=2.0)
        t1.join()
        t2.join()

        assert summary.total_submitted == 2
        assert summary.succeeded == 2
        assert summary.values == (1, 2)

    def test_mixed_statuses(self):
        """wait() correctly counts mixed statuses in SyncTaskGroup."""
        h1 = _sync_handle(task_id="s1")
        h2 = _sync_handle(task_id="s2")
        h3 = _sync_handle(task_id="s3")
        h1._mark_finished(_succeeded(task_id="s1"))
        h2._mark_finished(_failed(task_id="s2"))
        h3._mark_finished(_cancelled(task_id="s3"))

        group = SyncTaskGroup("g1", [h1, h2, h3])
        summary = group.wait()
        assert summary.succeeded == 1
        assert summary.failed == 1
        assert summary.cancelled == 1
        assert summary.ok is False


# ---------------------------------------------------------------------------
# Helper for threaded test
# ---------------------------------------------------------------------------

def group_wait_with_thread(h1, h2, timeout):
    """Wait on a SyncTaskGroup in the current thread."""
    group = SyncTaskGroup("g1", [h1, h2])
    return group.wait(timeout=timeout)
