"""Tests for the ProcessQueue."""

import time

import pytest

from osiiso import ClosedError, ProcessQueue, TaskOptions


def double(x):
    return x * 2


def add(a, b):
    return a + b


def fail_always(msg="boom"):
    raise ValueError(msg)


def slow(seconds):
    time.sleep(seconds)
    return "done"


class TestSubmit:
    def test_submit_and_run(self):
        q = ProcessQueue(workers=2)
        q.submit(double, 5)
        q.submit(double, 10)
        summary = q.run()
        assert summary.ok
        assert summary.succeeded == 2
        assert set(summary.values) == {10, 20}

    def test_submit_with_opts(self):
        opts = TaskOptions(priority=1)
        q = ProcessQueue(workers=1)
        h = q.submit(double, 7, opts=opts)
        summary = q.run()
        assert summary.ok
        assert h.priority == 1


class TestMapAndGroup:
    def test_map(self):
        q = ProcessQueue(workers=2)
        q.map(double, [1, 2, 3])
        summary = q.run()
        assert summary.succeeded == 3
        assert set(summary.values) == {2, 4, 6}

    def test_group(self):
        q = ProcessQueue(workers=2)
        g = q.group([(double, 10), (double, 20)])
        summary = q.run()
        assert summary.ok
        group_summary = g.wait()
        assert set(group_summary.values) == {20, 40}

    def test_group_heterogeneous_tasks(self):
        q = ProcessQueue(workers=2)
        g = q.group([(double, 5), (add, 2, 3)], group_id="mixed")
        summary = q.run()
        assert summary.ok
        assert g.group_id == "mixed"
        assert len(summary.by_group()["mixed"]) == 2
        assert set(summary.values) == {10, 5}


class TestContextManager:
    def test_with(self):
        with ProcessQueue(workers=2) as q:
            q.submit(double, 1)
            summary = q.run()
        assert summary.ok
        assert q.closed


class TestRetries:
    def test_retry_exhausted(self):
        q = ProcessQueue(workers=1)
        q.submit(fail_always, "nope", retries=1)
        summary = q.run()
        assert summary.failed == 1


class TestTimeout:
    def test_task_timeout(self):
        q = ProcessQueue(workers=1)
        q.submit(slow, 10, timeout=0.2)
        summary = q.run()
        assert summary.failed == 1

    def test_queue_timeout(self):
        q = ProcessQueue(workers=1)
        q.submit(slow, 10)
        summary = q.run(timeout=0.3)
        assert summary.timed_out


class TestHandle:
    def test_handle_wait(self):
        with ProcessQueue(workers=1) as q:
            h = q.submit(double, 21)
            result = h.wait()
        assert result.value == 42
        assert h.done()


class TestResetAndClosed:
    def test_reset(self):
        q = ProcessQueue(workers=1)
        q.submit(double, 1)
        q.run()
        q.reset()
        assert len(q.results) == 0

    def test_closed_rejects(self):
        q = ProcessQueue(workers=1)
        q.submit(double, 1)
        q.run()
        q.shutdown()
        with pytest.raises(ClosedError):
            q.submit(double, 2)
