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


# ---------------------------------------------------------------------------
# Module-level helper functions for new tests (must be pickleable)
# ---------------------------------------------------------------------------


_ATTEMPTS = 0


def _flaky_task():
    """Fail the first time, succeed on the retry.

    Relies on the persistent pool: the retry runs in the *same* subprocess,
    so this module-global attempt counter survives between attempts.
    """
    global _ATTEMPTS
    _ATTEMPTS += 1
    if _ATTEMPTS < 2:
        raise ValueError("first attempt fails")
    return "recovered"


def _pid():
    import os

    return os.getpid()


def _crash_hard():
    import os

    os._exit(13)


_INIT_TAG = None


def _proc_init(tag):
    global _INIT_TAG
    _INIT_TAG = tag


def _read_init_tag():
    return _INIT_TAG


def _return_lambda():
    return lambda x: x  # not picklable


def _greet(name, greeting="Hello"):
    return f"{greeting}, {name}!"


def _square(x):
    return x ** 2


def _noop():
    return "noop"


def _return_arg(x):
    return x


# -- Retry extended -----------------------------------------------------------


class TestRetryExtended:
    def test_retry_succeeds_eventually(self):
        """Task fails then succeeds on retry (same pooled subprocess)."""
        q = ProcessQueue(workers=1)
        q.submit(_flaky_task, retries=2)
        summary = q.run()
        assert summary.ok
        assert summary.values == ("recovered",)


# -- Fail policies ------------------------------------------------------------


class TestFailPolicies:
    def test_fail_first_stops_queue(self):
        """fail_first cancels remaining tasks."""
        q = ProcessQueue(workers=1, fail_policy="fail_first")
        q.submit(fail_always)
        q.submit(double, 1)
        q.submit(double, 2)
        summary = q.run()
        assert summary.failed >= 1

    def test_continue_policy(self):
        """continue collects all results."""
        q = ProcessQueue(workers=1, fail_policy="continue")
        q.submit(fail_always)
        q.submit(double, 1)
        summary = q.run()
        assert summary.failed == 1
        assert summary.succeeded == 1


# -- Context manager extended --------------------------------------------------


class TestContextManagerExtended:
    def test_exception_cancels(self):
        """Exception in context manager triggers force shutdown."""
        with pytest.raises(RuntimeError):
            with ProcessQueue(workers=1) as q:
                q.submit(slow, 10)
                raise RuntimeError("abort")
        assert q.closed


# -- cancel() method ----------------------------------------------------------


class TestCancelMethod:
    def test_cancel_stops_queue(self):
        """cancel() stops the queue."""
        import threading

        q = ProcessQueue(workers=1)
        q.submit(slow, 10)
        q.start()
        time.sleep(0.3)
        t = threading.Thread(target=q.cancel)
        t.start()
        t.join(timeout=10)
        assert q.closed


# -- Handle extended -----------------------------------------------------------


class TestHandleExtended:
    def test_handle_value(self):
        """handle.value() on succeeded task returns the result."""
        with ProcessQueue(workers=1) as q:
            h = q.submit(double, 5)
            h.wait(timeout=5)
        assert h.value() == 10

    def test_handle_done(self):
        """handle.done() returns False before and True after completion."""
        q = ProcessQueue(workers=1)
        h = q.submit(double, 1)
        # Before running, done() should be False
        assert not h.done()
        q.run()
        assert h.done()

    def test_handle_cancel(self):
        """handle.cancel() on pending task succeeds."""
        with ProcessQueue(workers=1) as q:
            h = q.submit(slow, 10)
            time.sleep(0.3)
            cancelled = h.cancel()
            assert cancelled or h.done()


# -- Decorator ----------------------------------------------------------------


def _work_increment(x):
    return x + 1


def _square(x):
    return x ** 2


class TestDecorator:
    def test_task_decorator(self):
        """@q.task() decorator works."""
        q = ProcessQueue(workers=1)
        bound = q.task(retries=1)(_work_increment)
        h = bound(10)
        from osiiso import SyncTaskHandle
        assert isinstance(h, SyncTaskHandle)
        summary = q.run()
        assert summary.ok
        assert h.value() == 11

    def test_decorator_map(self):
        """bound_task.map() works."""
        q = ProcessQueue(workers=2)
        bound = q.task()(_square)
        bound.map([1, 2, 3])
        summary = q.run()
        assert set(summary.values) == {1, 4, 9}


# -- Hooks ---------------------------------------------------------------------


class TestHooks:
    def test_on_start(self):
        """on_start callback fires."""
        started = []
        q = ProcessQueue(workers=1, on_start=lambda h: started.append(h.name))
        q.submit(double, 1)
        q.run()
        assert started == ["double"]

    def test_on_complete(self):
        """on_complete callback fires."""
        completed = []
        q = ProcessQueue(workers=1, on_complete=lambda r: completed.append(r.status))
        q.submit(double, 1)
        q.submit(double, 2)
        q.run()
        assert completed == ["succeeded", "succeeded"]

    def test_on_retry(self):
        """on_retry callback fires on retries."""
        retry_calls = []

        def on_retry(handle, exc):
            retry_calls.append(type(exc).__name__)

        q = ProcessQueue(workers=1, on_retry=on_retry)
        q.submit(_flaky_task, retries=2)
        q.run()
        assert len(retry_calls) >= 1
        assert retry_calls[0] == "ValueError"


# -- map with tuples ----------------------------------------------------------


class TestMapExtended:
    def test_map_tuples(self):
        """map with tuple unpacking."""
        q = ProcessQueue(workers=2)
        q.map(add, [(1, 2), (3, 4), (5, 6)])
        summary = q.run()
        assert summary.succeeded == 3
        assert set(summary.values) == {3, 7, 11}


# -- group extended -----------------------------------------------------------


class TestGroupExtended:
    def test_group_homogeneous(self):
        """group(fn, iterable) form works."""
        q = ProcessQueue(workers=2)
        g = q.group(double, [1, 2, 3])
        summary = q.run()
        assert summary.ok
        group_summary = g.wait()
        assert set(group_summary.values) == {2, 4, 6}

    def test_group_values(self):
        """group.values() returns values."""
        q = ProcessQueue(workers=2)
        g = q.group([(double, 5), (double, 10)])
        q.run()
        values = g.values()
        assert set(values) == {10, 20}


# -- clear_results -------------------------------------------------------------


class TestClearResults:
    def test_clear_results(self):
        """clear_results() empties results."""
        q = ProcessQueue(workers=1)
        q.submit(double, 5)
        q.submit(double, 10)
        q.run()
        assert len(q.results) == 2
        q.clear_results()
        assert len(q.results) == 0


# -- Constructor validation ----------------------------------------------------


class TestConstructorValidation:
    def test_negative_size_raises(self):
        """ProcessQueue(size=-1) raises ValueError."""
        with pytest.raises(ValueError, match="size must be >= 0"):
            ProcessQueue(size=-1)

    def test_zero_workers_raises(self):
        """ProcessQueue(workers=0) raises ValueError."""
        with pytest.raises(ValueError, match="workers must be > 0"):
            ProcessQueue(workers=0)


# -- stats property ------------------------------------------------------------


class TestStatsProperty:
    def test_stats_returns_dict(self):
        """stats property returns expected structure."""
        q = ProcessQueue(workers=1)
        q.submit(double, 1)
        q.run()
        s = q.stats
        assert isinstance(s, dict)
        assert "pending" in s
        assert "active" in s
        assert "completed" in s
        assert "workers" in s
        assert "closed" in s
        assert s["completed"] == 1


# -- join() --------------------------------------------------------------------


class TestJoin:
    def test_join_completes_work(self):
        """join() waits for pending tasks to finish."""
        q = ProcessQueue(workers=2)
        h1 = q.submit(double, 3)
        h2 = q.submit(double, 7)
        q.join()
        assert h1.done()
        assert h2.done()
        assert h1.value() == 6
        assert h2.value() == 14
        q.shutdown()


# ---------------------------------------------------------------------------
# Module-level helpers for new tests (must be pickleable on Windows)
# ---------------------------------------------------------------------------


async def _async_double(x):
    return x * 2


def _important_work():
    time.sleep(0.2)
    return "completed"


# -- Submit awaitable ---------------------------------------------------------


class TestSubmitAwaitable:
    def test_submit_awaitable_raises(self):
        """Submitting a bare awaitable (coroutine object) raises TypeError."""
        q = ProcessQueue(workers=1)
        coro = _async_double(5)  # calling the async function produces a coroutine
        try:
            with pytest.raises(TypeError, match="process tasks must be callable, not awaitable"):
                q.submit(coro)
        finally:
            coro.close()


# -- Coroutine function in subprocess ----------------------------------------


class TestCoroutineInSubprocess:
    def test_coroutine_function_runs(self):
        """Coroutine function is executed with asyncio.run in the subprocess."""
        q = ProcessQueue(workers=1)
        h = q.submit(_async_double, 7)
        summary = q.run()
        assert summary.ok
        assert h.value() == 14


# -- Scheduling (delay / run_at) ---------------------------------------------


class TestSchedulingProcess:
    def test_delay(self):
        """submit with delay= defers execution."""
        q = ProcessQueue(workers=1)
        start = time.perf_counter()
        h = q.submit(double, 3, delay=0.3)
        summary = q.run()
        elapsed = time.perf_counter() - start
        assert summary.ok
        assert h.value() == 6
        assert elapsed >= 0.25

    def test_run_at(self):
        """submit with run_at= defers execution until wall-clock time."""
        q = ProcessQueue(workers=1)
        start = time.perf_counter()
        h = q.submit(double, 4, run_at=time.time() + 0.3)
        summary = q.run()
        elapsed = time.perf_counter() - start
        assert summary.ok
        assert h.value() == 8
        assert elapsed >= 0.25


# -- must_complete tasks survive timeout --------------------------------------


class TestMustCompleteProcess:
    def test_must_complete_survives_timeout(self):
        """A must_complete task finishes even when the queue times out."""
        q = ProcessQueue(workers=2, on_timeout="complete")
        q.submit(slow, 10)                              # will be cancelled
        h = q.submit(_important_work, must_complete=True)  # should survive
        summary = q.run(timeout=0.5)
        assert summary.timed_out
        assert h.done()
        assert h.status == "succeeded"
        assert h.value() == "completed"


# -- Negative timeout in constructor ------------------------------------------


class TestNegativeTimeout:
    def test_negative_timeout_raises(self):
        """ProcessQueue(timeout=-1) raises ValueError."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            ProcessQueue(timeout=-1)


# -- handle.exception() on failure --------------------------------------------


class TestHandleException:
    def test_exception_returns_error(self):
        """handle.exception() returns the ValueError on a failed task."""
        q = ProcessQueue(workers=1)
        h = q.submit(fail_always)
        q.run()
        exc = h.exception()
        assert isinstance(exc, ValueError)
        assert str(exc) == "boom"


# -- Persistent pool behaviour --------------------------------------------------


class TestPersistentPool:
    def test_worker_process_is_reused(self):
        """Sequential tasks on one worker run in the same subprocess."""
        q = ProcessQueue(workers=1)
        q.map(_pid, [(), (), (), ()])
        summary = q.run()
        assert summary.ok
        assert len(set(summary.values)) == 1  # one PID for all four tasks

    def test_worker_crash_is_reported_and_pool_recovers(self):
        """A hard-crashing task fails cleanly; the next task still runs."""
        q = ProcessQueue(workers=1)
        crash = q.submit(_crash_hard)
        ok = q.submit(double, 4)
        summary = q.run()
        assert summary.failed == 1
        assert summary.succeeded == 1
        assert isinstance(crash.exception(), RuntimeError)
        assert "worker process died" in str(crash.exception())
        assert ok.value() == 8

    def test_initializer_runs_in_subprocess(self):
        """initializer/initargs run inside the pool subprocess before tasks."""
        q = ProcessQueue(workers=1, initializer=_proc_init, initargs=("configured",))
        h = q.submit(_read_init_tag)
        summary = q.run()
        assert summary.ok
        assert h.value() == "configured"

    def test_unpicklable_result_reports_failure(self):
        """A result that cannot be pickled is reported as a task failure."""
        q = ProcessQueue(workers=1)
        h = q.submit(_return_lambda)
        summary = q.run()
        assert summary.failed == 1
        assert "pickl" in str(h.exception()).lower()


# -- pmap shortcut ----------------------------------------------------------------


class TestPmap:
    def test_pmap_ordered_values(self):
        from osiiso import pmap

        assert pmap(double, [3, 1, 2], workers=2) == (6, 2, 4)


# -- Detached tasks -----------------------------------------------------------------


class TestDetachedProcess:
    def test_detached_excluded_from_summary(self):
        q = ProcessQueue(workers=2)
        h = q.submit(double, 5, detached=True)
        q.submit(double, 10)
        summary = q.run()
        assert summary.total_submitted == 1
        assert summary.values == (20,)
        assert h.value() == 10
