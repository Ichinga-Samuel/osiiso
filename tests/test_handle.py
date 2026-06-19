"""Tests for osiiso.handle — TaskHandle and SyncTaskHandle."""

import asyncio
import threading
import time
from concurrent.futures import CancelledError as SyncCancelledError

import pytest

from osiiso.handle import SyncTaskHandle, TaskHandle
from osiiso.result import TaskResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_async_handle(
    *,
    task_id: str = "h1",
    name: str = "my_task",
    priority: int = 3,
    must_complete: bool = False,
    created_at: float = 1.0,
    cancel_fn=None,
    group_id: str | None = None,
    detached: bool = False,
    scheduled_for: float | None = None,
) -> TaskHandle:
    """Create a TaskHandle with sensible defaults."""
    if cancel_fn is None:
        cancel_fn = lambda tid: False
    return TaskHandle(
        task_id=task_id,
        name=name,
        priority=priority,
        must_complete=must_complete,
        created_at=created_at,
        cancel_fn=cancel_fn,
        group_id=group_id,
        detached=detached,
        scheduled_for=scheduled_for,
    )


def _make_sync_handle(
    *,
    task_id: str = "s1",
    name: str = "sync_task",
    priority: int = 3,
    must_complete: bool = False,
    created_at: float = 1.0,
    cancel_fn=None,
    group_id: str | None = None,
    detached: bool = False,
    scheduled_for: float | None = None,
) -> SyncTaskHandle:
    """Create a SyncTaskHandle with sensible defaults."""
    if cancel_fn is None:
        cancel_fn = lambda tid: False
    return SyncTaskHandle(
        task_id=task_id,
        name=name,
        priority=priority,
        must_complete=must_complete,
        created_at=created_at,
        cancel_fn=cancel_fn,
        group_id=group_id,
        detached=detached,
        scheduled_for=scheduled_for,
    )


def _succeeded_result(task_id: str = "h1", name: str = "my_task", value=42) -> TaskResult:
    return TaskResult(task_id=task_id, name=name, status="succeeded", value=value)


def _failed_result(
    task_id: str = "h1", name: str = "my_task", exception: BaseException | None = None
) -> TaskResult:
    exc = exception or RuntimeError("boom")
    return TaskResult(task_id=task_id, name=name, status="failed", exception=exc, message=str(exc))


def _cancelled_result(task_id: str = "h1", name: str = "my_task") -> TaskResult:
    return TaskResult(task_id=task_id, name=name, status="cancelled", message="cancelled by user")


# ===================================================================
# TestTaskHandle (async)
# ===================================================================

class TestTaskHandle:
    """Async TaskHandle lifecycle and method tests."""

    def test_done_false_before_finish(self):
        """done() returns False before any result is set."""
        h = _make_async_handle()
        assert h.done() is False

    def test_done_true_after_finish(self):
        """done() returns True after _mark_finished is called."""
        h = _make_async_handle()
        h._mark_finished(_succeeded_result())
        assert h.done() is True

    def test_cancelled_false_when_succeeded(self):
        """cancelled() is False when the task succeeded."""
        h = _make_async_handle()
        h._mark_finished(_succeeded_result())
        assert h.cancelled() is False

    def test_cancelled_true_when_cancelled(self):
        """cancelled() is True when the task was cancelled."""
        h = _make_async_handle()
        h._mark_finished(_cancelled_result())
        assert h.cancelled() is True

    def test_status_transitions(self):
        """status property transitions through lifecycle states."""
        h = _make_async_handle()
        assert h.status == "pending"
        h._mark_running()
        assert h.status == "running"
        h._mark_retrying()
        assert h.status == "retrying"
        h._mark_finished(_succeeded_result())
        assert h.status == "succeeded"

    def test_attempts_increments(self):
        """attempts increments on each _mark_running, not on _mark_retrying."""
        h = _make_async_handle()
        assert h.attempts == 0
        h._mark_running()
        assert h.attempts == 1
        h._mark_retrying()
        assert h.attempts == 1  # retrying does not increment
        h._mark_running()
        assert h.attempts == 2

    def test_result_before_done_raises(self):
        """result() raises asyncio.InvalidStateError before task is done."""
        h = _make_async_handle()
        with pytest.raises(asyncio.InvalidStateError, match="result not ready"):
            h.result()

    def test_result_after_finish(self):
        """result() returns the TaskResult after _mark_finished."""
        h = _make_async_handle()
        r = _succeeded_result()
        h._mark_finished(r)
        assert h.result() is r

    def test_value_on_success(self):
        """value() returns the return value for a succeeded task."""
        h = _make_async_handle()
        h._mark_finished(_succeeded_result(value=99))
        assert h.value() == 99

    def test_value_on_failure_reraises(self):
        """value() re-raises the original exception for a failed task."""
        exc = ValueError("bad input")
        h = _make_async_handle()
        h._mark_finished(_failed_result(exception=exc))
        with pytest.raises(ValueError, match="bad input"):
            h.value()

    def test_value_on_cancelled_raises(self):
        """value() raises asyncio.CancelledError for a cancelled task."""
        h = _make_async_handle()
        h._mark_finished(_cancelled_result())
        with pytest.raises(asyncio.CancelledError):
            h.value()

    def test_exception_on_failed(self):
        """exception() returns the exception for a failed task."""
        exc = TypeError("wrong type")
        h = _make_async_handle()
        h._mark_finished(_failed_result(exception=exc))
        assert h.exception() is exc

    def test_exception_on_success(self):
        """exception() returns None for a succeeded task."""
        h = _make_async_handle()
        h._mark_finished(_succeeded_result())
        assert h.exception() is None

    @pytest.mark.asyncio
    async def test_wait_blocks_until_finished(self):
        """wait() blocks until _mark_finished is called from another task."""
        h = _make_async_handle()
        result = _succeeded_result(value="done")

        async def finish_later():
            await asyncio.sleep(0.05)
            h._mark_finished(result)

        asyncio.create_task(finish_later())
        r = await h.wait()
        assert r is result

    @pytest.mark.asyncio
    async def test_wait_returns_immediately_if_done(self):
        """wait() returns immediately if the handle is already finished."""
        h = _make_async_handle()
        result = _succeeded_result()
        h._mark_finished(result)
        r = await h.wait()
        assert r is result

    def test_cancel_before_done_delegates_to_cancel_fn(self):
        """cancel() delegates to _cancel_fn and returns its boolean result."""
        cancel_fn = lambda tid: True
        h = _make_async_handle(cancel_fn=cancel_fn)
        assert h.cancel() is True

    def test_cancel_before_done_returns_false_when_fn_returns_false(self):
        """cancel() returns False when _cancel_fn returns False."""
        cancel_fn = lambda tid: False
        h = _make_async_handle(cancel_fn=cancel_fn)
        assert h.cancel() is False

    def test_cancel_after_done_returns_false(self):
        """cancel() returns False when the task is already done."""
        h = _make_async_handle()
        h._mark_finished(_succeeded_result())
        assert h.cancel() is False

    def test_mark_running_updates_status(self):
        """_mark_running sets status to 'running'."""
        h = _make_async_handle()
        h._mark_running()
        assert h.status == "running"

    def test_mark_finished_idempotent(self):
        """Calling _mark_finished twice is a no-op for the second call."""
        h = _make_async_handle()
        r1 = _succeeded_result(value=1)
        r2 = _failed_result()
        h._mark_finished(r1)
        h._mark_finished(r2)
        assert h.result() is r1  # first result wins
        assert h.status == "succeeded"

    @pytest.mark.asyncio
    async def test_mark_finished_resolves_all_waiters(self):
        """_mark_finished resolves every pending waiter future."""
        h = _make_async_handle()
        result = _succeeded_result(value="hello")

        # Start two concurrent waiters
        waiter1 = asyncio.create_task(h.wait())
        waiter2 = asyncio.create_task(h.wait())
        await asyncio.sleep(0.01)  # let them register

        h._mark_finished(result)
        r1 = await waiter1
        r2 = await waiter2
        assert r1 is result
        assert r2 is result


# ===================================================================
# TestSyncTaskHandle
# ===================================================================

class TestSyncTaskHandle:
    """Blocking SyncTaskHandle tests using threading."""

    def test_wait_timeout_raises(self):
        """wait(timeout=...) raises TimeoutError when handle is not finished."""
        h = _make_sync_handle()
        with pytest.raises(TimeoutError, match="result not ready"):
            h.wait(timeout=0.05)

    def test_wait_blocks_until_finished(self):
        """wait() blocks the calling thread until _mark_finished is called."""
        h = _make_sync_handle()
        result = _succeeded_result(task_id="s1", name="sync_task", value="ok")

        def finish_later():
            time.sleep(0.05)
            h._mark_finished(result)

        t = threading.Thread(target=finish_later)
        t.start()
        r = h.wait(timeout=2.0)
        t.join()
        assert r is result

    def test_result_before_done_raises(self):
        """result() raises RuntimeError before the task is finished."""
        h = _make_sync_handle()
        with pytest.raises(RuntimeError, match="result not ready"):
            h.result()

    def test_value_on_cancelled_raises(self):
        """value() raises concurrent.futures.CancelledError on cancellation."""
        h = _make_sync_handle()
        h._mark_finished(_cancelled_result(task_id="s1", name="sync_task"))
        with pytest.raises(SyncCancelledError):
            h.value()

    def test_exception_before_done_raises(self):
        """exception() raises RuntimeError if the task is not done yet."""
        h = _make_sync_handle()
        with pytest.raises(RuntimeError, match="result not ready"):
            h.exception()

    def test_mark_finished_notifies_all_waiters(self):
        """_mark_finished wakes up all threads blocked on wait()."""
        h = _make_sync_handle()
        result = _succeeded_result(task_id="s1", name="sync_task", value="done")
        received = []

        def waiter():
            r = h.wait(timeout=2.0)
            received.append(r)

        t1 = threading.Thread(target=waiter)
        t2 = threading.Thread(target=waiter)
        t1.start()
        t2.start()
        time.sleep(0.05)  # let threads block
        h._mark_finished(result)
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)
        assert len(received) == 2
        assert all(r is result for r in received)

    def test_done_false_before_finish(self):
        """done() returns False before _mark_finished."""
        h = _make_sync_handle()
        assert h.done() is False

    def test_done_true_after_finish(self):
        """done() returns True after _mark_finished."""
        h = _make_sync_handle()
        h._mark_finished(_succeeded_result(task_id="s1"))
        assert h.done() is True

    def test_mark_finished_idempotent(self):
        """Second _mark_finished call is a no-op."""
        h = _make_sync_handle()
        r1 = _succeeded_result(task_id="s1", value="first")
        r2 = _failed_result(task_id="s1")
        h._mark_finished(r1)
        h._mark_finished(r2)
        assert h.result() is r1

    def test_cancel_before_done(self):
        """cancel() delegates to _cancel_fn when task is not done."""
        h = _make_sync_handle(cancel_fn=lambda tid: True)
        assert h.cancel() is True

    def test_cancel_after_done(self):
        """cancel() returns False when task is already done."""
        h = _make_sync_handle()
        h._mark_finished(_succeeded_result(task_id="s1"))
        assert h.cancel() is False

    def test_value_on_success(self):
        """value() returns the value for a succeeded task."""
        h = _make_sync_handle()
        h._mark_finished(_succeeded_result(task_id="s1", value=123))
        assert h.value() == 123

    def test_value_on_failure_reraises(self):
        """value() re-raises the original exception on failure."""
        exc = ValueError("sync boom")
        h = _make_sync_handle()
        h._mark_finished(_failed_result(task_id="s1", exception=exc))
        with pytest.raises(ValueError, match="sync boom"):
            h.value()

    def test_exception_on_success_returns_none(self):
        """exception() returns None for a succeeded task."""
        h = _make_sync_handle()
        h._mark_finished(_succeeded_result(task_id="s1"))
        assert h.exception() is None

    def test_exception_on_failure_returns_exception(self):
        """exception() returns the exception for a failed task."""
        exc = TypeError("type error")
        h = _make_sync_handle()
        h._mark_finished(_failed_result(task_id="s1", exception=exc))
        assert h.exception() is exc

    def test_status_transitions(self):
        """status transitions through pending → running → retrying → finished."""
        h = _make_sync_handle()
        assert h.status == "pending"
        h._mark_running()
        assert h.status == "running"
        h._mark_retrying()
        assert h.status == "retrying"
        h._mark_finished(_succeeded_result(task_id="s1"))
        assert h.status == "succeeded"

    def test_attempts_increments(self):
        """attempts increments on _mark_running."""
        h = _make_sync_handle()
        assert h.attempts == 0
        h._mark_running()
        assert h.attempts == 1
        h._mark_running()
        assert h.attempts == 2
