"""Tests for the asyncio AsyncQueue."""

import asyncio
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
