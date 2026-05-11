"""Tests for the ThreadQueue."""

import time
from functools import partial

import pytest

from osiiso import ClosedError, SyncTaskHandle, TaskOptions, ThreadQueue


def double(x):
    return x * 2


def fail_always(msg="boom"):
    raise ValueError(msg)


def slow(seconds):
    time.sleep(seconds)
    return "done"


# -- Basic submission & execution ---------------------------------------------


class TestSubmit:
    def test_submit_and_run(self):
        q = ThreadQueue(workers=2)
        q.submit(double, 5)
        q.submit(double, 10)
        summary = q.run()
        assert summary.ok
        assert summary.succeeded == 2
        assert set(summary.values) == {10, 20}

    def test_submit_with_opts(self):
        opts = TaskOptions(priority=1, retries=2)
        q = ThreadQueue(workers=1)
        h = q.submit(double, 7, opts=opts)
        summary = q.run()
        assert summary.ok
        assert h.priority == 1

    def test_submit_with_overrides(self):
        q = ThreadQueue(workers=1)
        h = q.submit(double, 3, priority=1, name="custom")
        assert h.name == "custom"
        summary = q.run()
        assert summary.ok

    def test_submit_partial(self):
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        q = ThreadQueue(workers=1)
        q.submit(partial(greet, greeting="Hi"), "World")
        summary = q.run()
        assert summary.values == ("Hi, World!",)


# -- Map & Group --------------------------------------------------------------


class TestMapAndGroup:
    def test_map(self):
        q = ThreadQueue(workers=4)
        q.map(double, [1, 2, 3, 4, 5])
        summary = q.run()
        assert summary.succeeded == 5
        assert set(summary.values) == {2, 4, 6, 8, 10}

    def test_map_tuples(self):
        def add(a, b):
            return a + b

        q = ThreadQueue(workers=2)
        q.map(add, [(1, 2), (3, 4)])
        summary = q.run()
        assert set(summary.values) == {3, 7}

    def test_group(self):
        q = ThreadQueue(workers=4)
        g = q.group([(double, 10), (double, 20), (double, 30)])
        assert len(g) == 3
        summary = q.run()
        assert summary.ok
        group_summary = g.wait()
        assert set(group_summary.values) == {20, 40, 60}

    def test_group_values(self):
        q = ThreadQueue(workers=2)
        g = q.group([(double, 5), (double, 10)])
        q.run()
        values = g.values()
        assert set(values) == {10, 20}

    def test_group_heterogeneous_tasks(self):
        def add(a, b):
            return a + b

        q = ThreadQueue(workers=2)
        g = q.group([(double, 5), (add, 2, 3)], group_id="mixed")
        summary = q.run()
        assert summary.ok
        assert g.group_id == "mixed"
        assert len(summary.by_group()["mixed"]) == 2
        assert set(summary.values) == {10, 5}


# -- Context manager ----------------------------------------------------------


class TestContextManager:
    def test_with(self):
        with ThreadQueue(workers=2) as q:
            q.submit(double, 1)
            q.submit(double, 2)
            summary = q.run()
        assert summary.ok
        assert q.closed

    def test_exception_cancels(self):
        with pytest.raises(RuntimeError):
            with ThreadQueue(workers=2) as q:
                q.submit(slow, 10)
                raise RuntimeError("abort")
        assert q.closed


# -- Retries ------------------------------------------------------------------


class TestRetries:
    def test_retry_succeeds_eventually(self):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        q = ThreadQueue(workers=1)
        q.submit(flaky, retries=3)
        summary = q.run()
        assert summary.ok
        assert call_count == 3

    def test_retry_exhausted(self):
        q = ThreadQueue(workers=1)
        q.submit(fail_always, "nope", retries=2)
        summary = q.run()
        assert summary.failed == 1


# -- Fail policy ---------------------------------------------------------------


class TestFailPolicy:
    def test_fail_first(self):
        q = ThreadQueue(workers=1, fail_policy="fail_first")
        q.submit(fail_always)
        q.submit(double, 1)
        summary = q.run()
        assert summary.failed >= 1

    def test_continue_policy(self):
        q = ThreadQueue(workers=1, fail_policy="continue")
        q.submit(fail_always)
        q.submit(double, 1)
        summary = q.run()
        assert summary.failed == 1
        assert summary.succeeded == 1


# -- Timeout -------------------------------------------------------------------


class TestTimeout:
    def test_task_timeout(self):
        q = ThreadQueue(workers=1)
        q.submit(slow, 10, timeout=0.1)
        summary = q.run()
        assert summary.failed == 1

    def test_queue_timeout(self):
        q = ThreadQueue(workers=1)
        q.submit(slow, 10)
        summary = q.run(timeout=0.2)
        assert summary.timed_out


# -- Handle --------------------------------------------------------------------


class TestHandle:
    def test_handle_wait(self):
        with ThreadQueue(workers=1) as q:
            h = q.submit(double, 21)
            result = h.wait()
        assert result.status == "succeeded"
        assert result.value == 42

    def test_handle_value(self):
        with ThreadQueue(workers=1) as q:
            h = q.submit(double, 5)
            h.wait()
        assert h.value() == 10

    def test_handle_cancel(self):
        with ThreadQueue(workers=1) as q:
            h = q.submit(slow, 10)
            time.sleep(0.1)
            assert h.cancel()

    def test_handle_done(self):
        with ThreadQueue(workers=1) as q:
            h = q.submit(double, 1)
            h.wait()
        assert h.done()


# -- Decorator ----------------------------------------------------------------


class TestDecorator:
    def test_task_decorator(self):
        q = ThreadQueue(workers=2)

        @q.task(retries=1)
        def work(x):
            return x + 1

        h = work(10)
        assert isinstance(h, SyncTaskHandle)
        summary = q.run()
        assert summary.ok
        assert h.value() == 11

    def test_decorator_map(self):
        q = ThreadQueue(workers=4)

        @q.task()
        def sq(x):
            return x**2

        sq.map([1, 2, 3])
        summary = q.run()
        assert set(summary.values) == {1, 4, 9}


# -- Event hooks ---------------------------------------------------------------


class TestHooks:
    def test_on_complete(self):
        completed = []
        q = ThreadQueue(workers=1, on_complete=lambda r: completed.append(r.status))
        q.submit(double, 1)
        q.submit(double, 2)
        q.run()
        assert completed == ["succeeded", "succeeded"]

    def test_on_start(self):
        started = []
        q = ThreadQueue(workers=1, on_start=lambda h: started.append(h.name))
        q.submit(double, 1)
        q.run()
        assert started == ["double"]


# -- Reset & closed queue ------------------------------------------------------


class TestResetAndClosed:
    def test_reset(self):
        q = ThreadQueue(workers=1)
        q.submit(double, 1)
        q.run()
        assert len(q.results) == 1
        q.reset()
        assert len(q.results) == 0

    def test_closed_rejects(self):
        q = ThreadQueue(workers=1)
        q.submit(double, 1)
        q.run()
        q.shutdown()
        with pytest.raises(ClosedError):
            q.submit(double, 2)
