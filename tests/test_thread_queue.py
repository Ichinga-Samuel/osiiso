"""Tests for the ThreadQueue."""

import threading
import time
from functools import partial

import pytest

from osiiso import ClosedError, ExecutionError, SyncTaskHandle, TaskOptions, ThreadQueue, iter_completed, tmap


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


# -- Constructor validation ----------------------------------------------------


class TestConstructorValidation:
    def test_negative_size_raises(self):
        """ThreadQueue(size=-1) raises ValueError."""
        with pytest.raises(ValueError, match="size must be >= 0"):
            ThreadQueue(size=-1)

    def test_zero_workers_raises(self):
        """ThreadQueue(workers=0) raises ValueError."""
        with pytest.raises(ValueError, match="workers must be > 0"):
            ThreadQueue(workers=0)

    def test_negative_timeout_raises(self):
        """ThreadQueue(timeout=-1) raises ValueError."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            ThreadQueue(timeout=-1)


# -- Type rejection ------------------------------------------------------------


class TestTypeRejection:
    def test_submit_coroutine_function_raises(self):
        """Submitting an async def function raises TypeError."""

        async def coro_fn():
            return 42

        q = ThreadQueue(workers=1)
        with pytest.raises(TypeError, match="sync callable"):
            q.submit(coro_fn)

    def test_submit_awaitable_raises(self):
        """Submitting an awaitable object raises TypeError."""

        async def coro_fn():
            return 42

        awaitable = coro_fn()
        q = ThreadQueue(workers=1)
        try:
            with pytest.raises(TypeError, match="awaitable"):
                q.submit(awaitable)
        finally:
            awaitable.close()


# -- on_retry hook -------------------------------------------------------------


class TestOnRetryHook:
    def test_on_retry_callback(self):
        """on_retry is called with handle and exception on each retry."""
        retry_calls = []

        def on_retry(handle, exc):
            retry_calls.append((handle.name, type(exc).__name__))

        call_count = 0

        def flaky_for_retry():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        q = ThreadQueue(workers=1, on_retry=on_retry)
        q.submit(flaky_for_retry, retries=3)
        summary = q.run()
        assert summary.ok
        assert len(retry_calls) == 2
        assert all(name == "flaky_for_retry" for name, _ in retry_calls)
        assert all(exc == "ValueError" for _, exc in retry_calls)


# -- cancel() method ----------------------------------------------------------


class TestCancelMethod:
    def test_cancel_stops_queue(self):
        """cancel() from external thread stops the queue."""
        import threading

        q = ThreadQueue(workers=1)
        q.submit(slow, 10)
        q.start()
        time.sleep(0.1)
        t = threading.Thread(target=q.cancel)
        t.start()
        t.join(timeout=5)
        assert q.closed


# -- clear_results() -----------------------------------------------------------


class TestClearResults:
    def test_clear_results(self):
        """clear_results() empties the results list."""
        q = ThreadQueue(workers=1)
        q.submit(double, 5)
        q.submit(double, 10)
        q.run()
        assert len(q.results) == 2
        q.clear_results()
        assert len(q.results) == 0


# -- join() --------------------------------------------------------------------


class TestJoin:
    def test_join_completes_work(self):
        """join() waits for pending tasks to finish."""
        q = ThreadQueue(workers=2)
        h1 = q.submit(double, 3)
        h2 = q.submit(double, 7)
        q.join()
        assert h1.done()
        assert h2.done()
        assert h1.value() == 6
        assert h2.value() == 14
        q.shutdown()


# -- Infinite mode -------------------------------------------------------------


class TestInfiniteMode:
    def test_infinite_mode(self):
        """start(), submit, verify execution, shutdown() in infinite mode."""
        import threading

        q = ThreadQueue(workers=1, mode="infinite")
        q.start()
        h = q.submit(double, 21)
        h.wait(timeout=2)
        assert h.done()
        assert h.value() == 42
        # Shutdown from a separate thread to avoid blocking
        t = threading.Thread(target=q.shutdown)
        t.start()
        t.join(timeout=5)
        assert q.closed


# -- map with dict entries -----------------------------------------------------


class TestMapExtended:
    def test_map_with_dict_entries(self):
        """map with Mapping/dict elements passes kwargs."""

        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        q = ThreadQueue(workers=2)
        q.map(greet, [{"name": "Alice", "greeting": "Hi"}, {"name": "Bob", "greeting": "Hey"}])
        summary = q.run()
        assert summary.succeeded == 2
        assert set(summary.values) == {"Hi, Alice!", "Hey, Bob!"}


# -- stats property ------------------------------------------------------------


class TestStatsProperty:
    def test_stats_returns_dict(self):
        """stats property returns expected structure."""
        q = ThreadQueue(workers=1)
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


# -- handle.wait timeout -------------------------------------------------------


class TestHandleTimeout:
    def test_handle_wait_timeout_raises(self):
        """handle.wait(timeout=0.01) raises TimeoutError for unfinished task."""
        q = ThreadQueue(workers=1)
        q.start()
        h = q.submit(slow, 10)
        with pytest.raises(TimeoutError):
            h.wait(timeout=0.01)
        q.shutdown(force=True)


# -- delay scheduling ---------------------------------------------------------


class TestScheduling:
    def test_delay_scheduling(self):
        """submit with delay, verify delayed execution."""
        q = ThreadQueue(workers=1)
        start = time.perf_counter()
        q.submit(double, 5, delay=0.3)
        summary = q.run()
        elapsed = time.perf_counter() - start
        assert summary.ok
        assert summary.values == (10,)
        assert elapsed >= 0.25  # small margin for timing


# -- retry with delay and backoff ----------------------------------------------


class TestRetryMechanics:
    def test_retry_with_delay_and_backoff(self):
        """retry_delay + backoff mechanics produce increasing delays."""
        call_times = []

        def record_and_fail():
            call_times.append(time.perf_counter())
            raise ValueError("fail")

        q = ThreadQueue(workers=1)
        q.submit(record_and_fail, retries=2, retry_delay=0.1, backoff=2.0)
        summary = q.run()
        assert summary.failed == 1
        # Should have 3 total attempts (1 initial + 2 retries)
        assert len(call_times) == 3
        # First retry delay should be ~0.1s
        gap1 = call_times[1] - call_times[0]
        assert gap1 >= 0.08
        # Second retry delay should be ~0.2s (0.1 * 2.0)
        gap2 = call_times[2] - call_times[1]
        assert gap2 >= 0.15


# -- must_complete survives forced shutdown ------------------------------------


class TestMustComplete:
    def test_must_complete_survives_cancel(self):
        """must_complete tasks survive forced shutdown via timeout."""
        result_holder = []

        def important():
            time.sleep(0.2)
            result_holder.append("done")
            return "completed"

        q = ThreadQueue(workers=2, on_timeout="complete")
        q.submit(slow, 10)  # long-running, will be cancelled
        q.submit(important, must_complete=True)
        summary = q.run(timeout=0.3)
        # The must_complete task should have finished
        assert any(r.status == "succeeded" and r.value == "completed" for r in summary.results)
        assert "done" in result_holder


# -- detached tasks ------------------------------------------------------------


class TestDetachedThread:
    def test_detached_excluded_from_summary(self):
        """Detached tasks run but are excluded from the RunSummary."""
        q = ThreadQueue(workers=2)
        h = q.submit(double, 5, detached=True)
        q.submit(double, 10)
        summary = q.run()
        assert summary.total_submitted == 1
        assert summary.values == (20,)
        assert h.done()
        assert h.value() == 10
        assert any(r.detached for r in q.results)


# -- run_at scheduling ---------------------------------------------------------


class TestRunAtScheduling:
    def test_run_at_delays_execution(self):
        """submit with run_at=time.time()+0.3 delays execution."""
        q = ThreadQueue(workers=1)
        start = time.perf_counter()
        q.submit(double, 5, run_at=time.time() + 0.3)
        summary = q.run()
        elapsed = time.perf_counter() - start
        assert summary.ok
        assert summary.values == (10,)
        assert elapsed >= 0.25  # small margin for timing


# -- Graceful drain on exit ------------------------------------------------------


class TestDrainOnExit:
    def test_context_exit_drains_pending_tasks(self):
        """Leaving the context without run() executes pending tasks."""
        seen = []

        def collect(x):
            seen.append(x)
            return x

        with ThreadQueue(workers=2) as q:
            handles = q.map(collect, [1, 2, 3])
        assert sorted(seen) == [1, 2, 3]
        assert all(h.status == "succeeded" for h in handles)


# -- Bounded queue: submit blocks ---------------------------------------------------


class TestBoundedQueue:
    def test_submit_blocks_until_space(self):
        q = ThreadQueue(workers=1, size=1)
        q.start()
        q.submit(slow, 0.3)
        t0 = time.perf_counter()
        h = q.submit(double, 4)  # must wait for the slow task to finish
        blocked_for = time.perf_counter() - t0
        q.shutdown()
        assert blocked_for >= 0.2
        assert h.value() == 8


# -- Rate limiting -------------------------------------------------------------------


class TestRateLimit:
    def test_rate_spaces_attempts(self):
        stamps = []
        lock = threading.Lock()

        def mark():
            with lock:
                stamps.append(time.perf_counter())

        q = ThreadQueue(workers=4, rate=20)
        q.map(mark, [()] * 6)
        summary = q.run()
        assert summary.ok
        assert max(stamps) - min(stamps) >= 0.2


# -- Worker initializer -----------------------------------------------------------------


class TestInitializer:
    def test_initializer_runs_in_each_worker(self):
        ready = []
        lock = threading.Lock()

        def init(tag):
            with lock:
                ready.append((threading.current_thread().name, tag))

        q = ThreadQueue(workers=2, initializer=init, initargs=("db",))
        q.map(double, [1, 2, 3, 4])
        summary = q.run()
        assert summary.ok
        assert len(ready) == 2
        assert all(tag == "db" for _, tag in ready)


# -- iter_completed ------------------------------------------------------------------------


class TestIterCompleted:
    def test_yields_in_completion_order(self):
        with ThreadQueue(workers=2) as q:
            slow_h = q.submit(slow, 0.3)
            fast_h = q.submit(double, 1)
            ordered = list(iter_completed([slow_h, fast_h], timeout=5))
        assert ordered[0] is fast_h
        assert ordered[1] is slow_h

    def test_group_as_completed(self):
        with ThreadQueue(workers=2) as q:
            g = q.group(double, [1, 2, 3])
            values = sorted(h.value() for h in g.as_completed(timeout=5))
        assert values == [2, 4, 6]

    def test_timeout_raises(self):
        q = ThreadQueue(workers=1)
        q.start()
        h = q.submit(slow, 10)
        with pytest.raises(TimeoutError):
            list(iter_completed([h], timeout=0.1))
        q.shutdown(force=True)


# -- Scheduled tasks do not block workers ----------------------------------------------------


class TestSchedulerNonBlocking:
    def test_delayed_task_does_not_starve_ready_tasks(self):
        order = []
        lock = threading.Lock()

        def mark(tag):
            with lock:
                order.append(tag)

        q = ThreadQueue(workers=1)
        q.submit(mark, "delayed", delay=0.3)
        q.submit(mark, "immediate")
        q.run()
        assert order == ["immediate", "delayed"]


# -- tmap shortcut -----------------------------------------------------------------------------


class TestTmap:
    def test_tmap_ordered_values(self):
        assert tmap(double, [3, 1, 2], workers=4) == (6, 2, 4)

    def test_tmap_raises_on_failure(self):
        with pytest.raises(ExecutionError):
            tmap(fail_always, ["a"], workers=1)


# -- fail_first spares must_complete ------------------------------------------------------------


class TestFailFirstMustComplete:
    def test_must_complete_survives_fail_first(self):
        q = ThreadQueue(workers=1, fail_policy="fail_first")
        q.submit(fail_always)
        protected = q.submit(slow, 0.05, must_complete=True)
        expendable = q.submit(double, 1)
        summary = q.run()
        assert summary.failed == 1
        assert protected.status == "succeeded"
        assert expendable.status == "cancelled"
