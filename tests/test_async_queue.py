"""Tests for the asyncio AsyncQueue."""

import asyncio
import time
from functools import partial

import pytest

from osiiso import AsyncQueue, ClosedError, ExecutionError, TaskHandle, TaskOptions


async def double(x):
    return x * 2


async def fail_always(msg="boom"):
    raise ValueError(msg)


async def slow(seconds):
    await asyncio.sleep(seconds)
    return "done"


# -- Basic submission & execution ---------------------------------------------


class TestSubmit:
    async def test_submit_and_run(self):
        q = AsyncQueue(workers=2)
        q.submit(double, 5)
        q.submit(double, 10)
        summary = await q.run()
        assert summary.ok
        assert summary.succeeded == 2
        assert set(summary.values) == {10, 20}

    async def test_submit_with_opts(self):
        opts = TaskOptions(priority=1, retries=2)
        q = AsyncQueue(workers=1)
        h = q.submit(double, 7, opts=opts)
        summary = await q.run()
        assert summary.ok
        assert h.priority == 1

    async def test_submit_with_inline_overrides(self):
        q = AsyncQueue(workers=1)
        h = q.submit(double, 3, priority=1, name="custom")
        assert h.name == "custom"
        assert h.priority == 1
        summary = await q.run()
        assert summary.ok

    async def test_submit_unknown_option_raises(self):
        q = AsyncQueue(workers=1)
        with pytest.raises(TypeError, match="Unknown task option"):
            q.submit(double, 3, bogus=True)

    async def test_submit_partial(self):
        """Use functools.partial for task kwargs."""

        async def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        q = AsyncQueue(workers=1)
        q.submit(partial(greet, greeting="Hi"), "World")
        summary = await q.run()
        assert summary.values == ("Hi, World!",)


# -- Map & Group --------------------------------------------------------------


class TestMapAndGroup:
    async def test_map(self):
        q = AsyncQueue(workers=4)
        q.map(double, [1, 2, 3, 4, 5])
        summary = await q.run()
        assert summary.succeeded == 5
        assert set(summary.values) == {2, 4, 6, 8, 10}

    async def test_map_tuples(self):
        async def add(a, b):
            return a + b

        q = AsyncQueue(workers=2)
        q.map(add, [(1, 2), (3, 4)])
        summary = await q.run()
        assert set(summary.values) == {3, 7}

    async def test_group(self):
        q = AsyncQueue(workers=4)
        g = q.group([(double, 10), (double, 20), (double, 30)])
        assert len(g) == 3
        summary = await q.run()
        assert summary.ok
        group_summary = await g.wait()
        assert set(group_summary.values) == {20, 40, 60}

    async def test_group_values(self):
        q = AsyncQueue(workers=2)
        g = q.group([(double, 5), (double, 10)])
        await q.run()
        values = await g.values()
        assert set(values) == {10, 20}

    async def test_group_heterogeneous_tasks(self):
        async def add(a, b):
            return a + b

        q = AsyncQueue(workers=2)
        g = q.group([(double, 5), (add, 2, 3)], group_id="mixed")
        summary = await q.run()
        assert summary.ok
        assert g.group_id == "mixed"
        assert len(summary.by_group()["mixed"]) == 2
        assert set(summary.values) == {10, 5}


# -- Context manager ----------------------------------------------------------


class TestContextManager:
    async def test_async_with(self):
        async with AsyncQueue(workers=2) as q:
            q.submit(double, 1)
            q.submit(double, 2)
            summary = await q.run()
        assert summary.ok
        assert q.closed

    async def test_exception_cancels(self):
        with pytest.raises(RuntimeError):
            async with AsyncQueue(workers=2) as q:
                q.submit(slow, 10)
                raise RuntimeError("abort")
        assert q.closed


# -- Retries ------------------------------------------------------------------


class TestRetries:
    async def test_retry_succeeds_eventually(self):
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        q = AsyncQueue(workers=1)
        q.submit(flaky, retries=3)
        summary = await q.run()
        assert summary.ok
        assert call_count == 3

    async def test_retry_exhausted(self):
        q = AsyncQueue(workers=1)
        q.submit(fail_always, "nope", retries=2)
        summary = await q.run()
        assert summary.failed == 1
        assert not summary.ok


# -- Fail policy ---------------------------------------------------------------


class TestFailPolicy:
    async def test_fail_first_stops_queue(self):
        q = AsyncQueue(workers=1, fail_policy="fail_first")
        q.submit(fail_always)
        q.submit(double, 1)
        q.submit(double, 2)
        summary = await q.run()
        assert summary.failed >= 1

    async def test_continue_policy(self):
        q = AsyncQueue(workers=1, fail_policy="continue")
        q.submit(fail_always)
        q.submit(double, 1)
        summary = await q.run()
        assert summary.failed == 1
        assert summary.succeeded == 1


# -- Timeout -------------------------------------------------------------------


class TestTimeout:
    async def test_task_timeout(self):
        q = AsyncQueue(workers=1)
        q.submit(slow, 10, timeout=0.1)
        summary = await q.run()
        assert summary.failed == 1

    async def test_queue_timeout(self):
        q = AsyncQueue(workers=1)
        q.submit(slow, 10)
        summary = await q.run(timeout=0.2)
        assert summary.timed_out


# -- Handle --------------------------------------------------------------------


class TestHandle:
    async def test_await_handle(self):
        async with AsyncQueue(workers=1) as q:
            h = q.submit(double, 21)
            result = await h
        assert result.status == "succeeded"
        assert result.value == 42

    async def test_handle_value(self):
        async with AsyncQueue(workers=1) as q:
            h = q.submit(double, 5)
            await h
        assert h.value() == 10

    async def test_handle_cancel(self):
        async with AsyncQueue(workers=1) as q:
            h = q.submit(slow, 10)
            await asyncio.sleep(0.05)
            assert h.cancel()

    async def test_handle_done(self):
        async with AsyncQueue(workers=1) as q:
            h = q.submit(double, 1)
            await h.wait()
        assert h.done()


# -- Decorator ----------------------------------------------------------------


class TestDecorator:
    async def test_task_decorator(self):
        q = AsyncQueue(workers=2)

        @q.task(retries=1)
        async def work(x):
            return x + 1

        h = work(10)
        assert isinstance(h, TaskHandle)
        summary = await q.run()
        assert summary.ok
        assert h.value() == 11

    async def test_decorator_map(self):
        q = AsyncQueue(workers=4)

        @q.task()
        async def sq(x):
            return x**2

        sq.map([1, 2, 3])
        summary = await q.run()
        assert summary.ok
        assert set(summary.values) == {1, 4, 9}


# -- Event hooks ---------------------------------------------------------------


class TestHooks:
    async def test_on_complete(self):
        completed = []
        q = AsyncQueue(workers=1, on_complete=lambda r: completed.append(r.status))
        q.submit(double, 1)
        q.submit(double, 2)
        await q.run()
        assert completed == ["succeeded", "succeeded"]

    async def test_on_start(self):
        started = []
        q = AsyncQueue(workers=1, on_start=lambda h: started.append(h.name))
        q.submit(double, 1)
        await q.run()
        assert started == ["double"]

    async def test_on_retry(self):
        retried = []
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("fail")
            return "ok"

        q = AsyncQueue(workers=1, on_retry=lambda h, exc: retried.append(str(exc)))
        q.submit(flaky, retries=2)
        await q.run()
        assert retried == ["fail"]


# -- as_completed --------------------------------------------------------------


class TestAsCompleted:
    async def test_as_completed(self):
        q = AsyncQueue(workers=4)
        handles = q.map(double, [1, 2, 3])

        results = []
        await q.start()
        async for h in AsyncQueue.as_completed(handles):
            results.append(h.value())
        assert set(results) == {2, 4, 6}
        await q.shutdown()


# -- Summary -------------------------------------------------------------------


class TestSummary:
    async def test_summary_properties(self):
        q = AsyncQueue(workers=2)
        q.submit(double, 1)
        q.submit(fail_always)
        summary = await q.run()
        assert summary.total_submitted == 2
        assert summary.succeeded == 1
        assert summary.failed == 1
        assert not summary.ok
        assert len(summary.errors) == 1
        assert len(summary.values) == 1

    async def test_raise_for_errors(self):
        q = AsyncQueue(workers=1)
        q.submit(fail_always)
        summary = await q.run()
        with pytest.raises(Exception, match="1 task"):
            summary.raise_for_errors()

    async def test_by_name(self):
        q = AsyncQueue(workers=2)
        q.submit(double, 1)
        q.submit(double, 2)
        summary = await q.run()
        by_name = summary.by_name()
        assert "double" in by_name
        assert len(by_name["double"]) == 2

    async def test_run_strict(self):
        q = AsyncQueue(workers=1)
        q.submit(fail_always)
        with pytest.raises(ExecutionError):
            await q.run(strict=True)


# -- Reset & clear_results ----------------------------------------------------


class TestResetAndClear:
    async def test_reset(self):
        q = AsyncQueue(workers=1)
        q.submit(double, 1)
        await q.run()
        assert len(q.results) == 1
        q.reset()
        assert len(q.results) == 0
        assert not q.closed

    async def test_clear_results(self):
        q = AsyncQueue(workers=1)
        q.submit(double, 1)
        await q.run()
        q.clear_results()
        assert len(q.results) == 0


# -- Closed queue rejects submissions -----------------------------------------


class TestClosedQueue:
    async def test_closed_rejects_submit(self):
        q = AsyncQueue(workers=1)
        q.submit(double, 1)
        await q.run()
        await q.shutdown()
        with pytest.raises(ClosedError):
            q.submit(double, 2)


# -- Constructor validation ----------------------------------------------------


class TestConstructorValidation:
    def test_negative_size_raises(self):
        """AsyncQueue(size=-1) must reject negative queue sizes."""
        with pytest.raises(ValueError, match="size must be >= 0"):
            AsyncQueue(size=-1)

    def test_zero_workers_raises(self):
        """AsyncQueue(workers=0) must reject zero workers."""
        with pytest.raises(ValueError, match="workers must be > 0"):
            AsyncQueue(workers=0)

    def test_negative_timeout_raises(self):
        """AsyncQueue(timeout=-1) must reject negative timeouts."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            AsyncQueue(timeout=-1)


# -- Map with dict entries -----------------------------------------------------


class TestMapExtended:
    async def test_map_with_dict_entries(self):
        """map() with dict/Mapping entries passes them as keyword args via partial."""

        async def greet(name, greeting="Hi"):
            return f"{greeting}, {name}!"

        q = AsyncQueue(workers=2)
        q.map(greet, [{"name": "Alice", "greeting": "Hello"}, {"name": "Bob"}])
        summary = await q.run()
        assert summary.succeeded == 2
        assert "Hello, Alice!" in summary.values
        assert "Hi, Bob!" in summary.values


# -- Group homogeneous form ---------------------------------------------------


class TestGroupExtended:
    async def test_group_homogeneous_form(self):
        """group(fn, iterable) form maps a single function over many args."""
        q = AsyncQueue(workers=2)
        g = q.group(double, [1, 2, 3])
        summary = await q.run()
        assert summary.ok
        group_summary = await g.wait()
        assert set(group_summary.values) == {2, 4, 6}


# -- Infinite mode -------------------------------------------------------------


class TestInfiniteMode:
    async def test_infinite_mode(self):
        """In infinite mode: start, submit tasks, verify execution, then shutdown."""
        results = []

        async def collect(x):
            results.append(x)
            return x

        q = AsyncQueue(workers=2, mode="infinite")
        await q.start()
        q.submit(collect, 1)
        q.submit(collect, 2)
        # Give workers time to process
        await asyncio.sleep(0.15)
        await q.shutdown()
        assert sorted(results) == [1, 2]


# -- Delay scheduling ---------------------------------------------------------


class TestScheduling:
    async def test_delay_scheduling(self):
        """A task with delay=0.1 should not execute immediately."""
        import time

        start = time.perf_counter()
        executed_at = []

        async def record_time():
            executed_at.append(time.perf_counter())
            return "ok"

        q = AsyncQueue(workers=1)
        q.submit(record_time, delay=0.1)
        summary = await q.run()
        assert summary.ok
        assert len(executed_at) == 1
        elapsed = executed_at[0] - start
        assert elapsed >= 0.08  # Allow small timing variance


# -- Retry mechanics -----------------------------------------------------------


class TestRetryMechanics:
    async def test_retry_with_delay_and_backoff(self):
        """Verify retry_delay and backoff cause increasing delays between retries."""
        import time

        timestamps = []

        async def record_and_fail():
            timestamps.append(time.perf_counter())
            raise ValueError("fail")

        q = AsyncQueue(workers=1)
        # 2 retries => 3 total attempts, retry_delay=0.05, backoff=2.0
        # delays: 0.05s, then 0.1s
        q.submit(record_and_fail, retries=2, retry_delay=0.05, backoff=2.0)
        summary = await q.run()
        assert summary.failed == 1
        assert len(timestamps) == 3

        # First retry delay should be ~0.05s
        gap1 = timestamps[1] - timestamps[0]
        assert gap1 >= 0.04

        # Second retry delay should be ~0.10s (0.05 * 2.0)
        gap2 = timestamps[2] - timestamps[1]
        assert gap2 >= 0.08


# -- Must-complete survives cancel ---------------------------------------------


class TestMustComplete:
    async def test_must_complete_survives_cancel(self):
        """must_complete tasks survive timeout shutdown (default on_exit policy)."""
        q = AsyncQueue(workers=2, on_exit="complete_priority")
        q.submit(slow, 0.1, must_complete=True)
        q.submit(slow, 10)  # long task — will be cancelled on timeout
        # The timeout fires, _handle_timeout calls _cancel_active(force=False)
        # which skips tasks with must_complete=True.
        summary = await q.run(timeout=0.5)
        mc_results = [r for r in summary.results if r.must_complete]
        assert len(mc_results) == 1
        assert mc_results[0].status == "succeeded"


# -- Detached tasks ------------------------------------------------------------


class TestDetached:
    async def test_detached_tasks_excluded(self):
        """Detached tasks have detached=True in results."""
        q = AsyncQueue(workers=2)
        q.submit(double, 5, detached=True)
        q.submit(double, 10)
        summary = await q.run()
        detached_results = [r for r in summary.results if r.detached]
        assert len(detached_results) == 1
        assert detached_results[0].value == 10  # double(5) = 10
        non_detached = [r for r in summary.results if not r.detached]
        assert len(non_detached) == 1
        assert non_detached[0].value == 20  # double(10) = 20


# -- Stats property ------------------------------------------------------------


class TestStatsProperty:
    async def test_stats_returns_dict(self):
        """stats property returns a dict with the expected keys."""
        q = AsyncQueue(workers=1)
        s = q.stats
        assert isinstance(s, dict)
        assert set(s.keys()) == {"pending", "active", "completed", "workers", "closed"}
        assert s["pending"] == 0
        assert s["active"] == 0
        assert s["completed"] == 0
        assert s["closed"] is False


# -- Run guards ----------------------------------------------------------------


class TestRunGuards:
    async def test_run_while_running_raises(self):
        """Calling run() while already running raises RuntimeError."""
        q = AsyncQueue(workers=1)
        q.submit(slow, 0.3)

        async def try_second_run():
            await asyncio.sleep(0.05)
            with pytest.raises(RuntimeError, match="run\\(\\) is already in progress"):
                await q.run()

        _, summary = await asyncio.gather(
            try_second_run(),
            q.run(),
        )
        assert summary.ok

    async def test_start_on_closed_raises(self):
        """start() on a shutdown queue raises ClosedError."""
        q = AsyncQueue(workers=1)
        q.submit(double, 1)
        await q.run()
        await q.shutdown()
        with pytest.raises(ClosedError, match="queue has been shut down"):
            await q.start()


# -- Sync callable offload ----------------------------------------------------


class TestSyncCallableOffload:
    async def test_sync_callable_offloaded(self):
        """A regular (non-async) function executes correctly via asyncio.to_thread."""

        def sync_double(x):
            return x * 2

        q = AsyncQueue(workers=1)
        q.submit(sync_double, 21)
        summary = await q.run()
        assert summary.ok
        assert summary.values == (42,)


# -- Callback error handling ---------------------------------------------------


class TestCallbackErrors:
    async def test_on_start_exception_swallowed(self):
        """on_start callback that raises does not crash the queue."""

        def bad_on_start(handle):
            raise RuntimeError("start callback exploded")

        q = AsyncQueue(workers=1, on_start=bad_on_start)
        q.submit(double, 5)
        summary = await q.run()
        assert summary.ok
        assert summary.values == (10,)

    async def test_on_complete_exception_swallowed(self):
        """on_complete callback that raises does not crash the queue."""

        def bad_on_complete(result):
            raise RuntimeError("complete callback exploded")

        q = AsyncQueue(workers=1, on_complete=bad_on_complete)
        q.submit(double, 5)
        summary = await q.run()
        assert summary.ok
        assert summary.values == (10,)


# -- on_exit="cancel" shutdown policy ------------------------------------------


class TestOnExitCancel:
    async def test_cancel_policy_cancels_long_task(self):
        """on_exit='cancel' cancels all tasks when the run times out."""
        q = AsyncQueue(workers=2, on_exit="cancel")
        h_slow = q.submit(slow, 10)
        h_fast = q.submit(double, 5)
        summary = await q.run(timeout=0.3)
        assert summary.timed_out
        # The fast task should have completed before timeout
        assert h_fast.done()
        # The slow task should have been cancelled
        assert h_slow.done()
        cancelled = [r for r in summary.results if r.status == "cancelled"]
        assert len(cancelled) >= 1


# -- reset() during run raises RuntimeError ------------------------------------


class TestResetDuringRun:
    async def test_reset_while_running_raises(self):
        """Calling reset() while run() is in progress raises RuntimeError."""
        q = AsyncQueue(workers=1)
        q.submit(slow, 0.3)

        async def try_reset():
            await asyncio.sleep(0.05)
            with pytest.raises(RuntimeError, match="cannot reset during run\\(\\)"):
                q.reset()

        _, summary = await asyncio.gather(
            try_reset(),
            q.run(),
        )
        assert summary.ok


# -- join() completes pending work ---------------------------------------------


class TestJoinMethod:
    async def test_join_completes_all_tasks(self):
        """join() blocks until all pending tasks complete."""
        q = AsyncQueue(workers=2)
        h1 = q.submit(double, 5)
        h2 = q.submit(double, 10)
        await q.start()
        h3 = q.submit(double, 15)
        await q.join()
        assert h1.done()
        assert h2.done()
        assert h3.done()
        assert h1.value() == 10
        assert h2.value() == 20
        assert h3.value() == 30
        await q.shutdown()


# -- run_at scheduling ---------------------------------------------------------


class TestRunAtScheduling:
    async def test_run_at_delays_execution(self):
        """A task with run_at in the future waits until the scheduled time."""
        start = time.perf_counter()
        q = AsyncQueue(workers=1)
        h = q.submit(double, 7, run_at=time.time() + 0.3)
        summary = await q.run()
        elapsed = time.perf_counter() - start
        assert summary.ok
        assert h.value() == 14
        assert elapsed >= 0.25
