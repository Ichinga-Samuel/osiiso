"""Tests for SharedQueue — thread-safe submission onto a shared event loop."""

import asyncio
import threading

import pytest

from osiiso import ClosedError, QueueFullError, SharedQueue


async def double(x):
    return x * 2


async def slow(seconds):
    await asyncio.sleep(seconds)
    return "done"


# -- Same-thread parity with AsyncQueue ----------------------------------------


class TestSameThread:
    async def test_prestart_submit_and_run(self):
        q = SharedQueue(workers=2)
        q.submit(double, 5)
        q.submit(double, 10)
        summary = await q.run()
        assert summary.ok
        assert set(summary.values) == {10, 20}

    async def test_submit_on_loop_thread_while_running(self):
        async with SharedQueue(workers=2) as q:
            h = q.submit(double, 7)
            summary = await q.run()
        assert summary.ok
        assert h.value() == 14

    async def test_map_and_group_on_loop_thread(self):
        q = SharedQueue(workers=2)
        q.map(double, [1, 2, 3])
        grp = q.group(double, [4, 5])
        summary = await q.run()
        assert summary.succeeded == 5
        assert {h.value() for h in grp} == {8, 10}


# -- Cross-thread submission ----------------------------------------------------


class TestCrossThread:
    async def test_submit_from_foreign_thread(self):
        async with SharedQueue(workers=2) as q:
            h = await asyncio.to_thread(q.submit, double, 21)
            result = await h
        assert result.status == "succeeded"
        assert h.value() == 42

    async def test_many_producer_threads(self):
        async with SharedQueue(workers=4) as q:

            def produce(base):
                for i in range(25):
                    q.submit(double, base * 100 + i)

            threads = [threading.Thread(target=produce, args=(b,)) for b in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                await asyncio.to_thread(t.join)
            await q.join()
            results = q.results
        assert len(results) == 200
        assert all(r.status == "succeeded" for r in results)
        expected = {(b * 100 + i) * 2 for b in range(8) for i in range(25)}
        assert {r.value for r in results} == expected

    async def test_map_from_foreign_thread(self):
        async with SharedQueue(workers=4) as q:
            handles = await asyncio.to_thread(q.map, double, list(range(30)))
            await q.join()
        assert len(handles) == 30
        assert all(h.done() for h in handles)
        assert sorted(h.value() for h in handles) == [i * 2 for i in range(30)]

    async def test_group_from_foreign_thread(self):
        async with SharedQueue(workers=2) as q:
            grp = await asyncio.to_thread(q.group, double, [1, 2, 3])
            summary = await grp.wait()
            await q.run()
        assert summary.values == (2, 4, 6)

    async def test_bound_task_from_foreign_thread(self):
        async with SharedQueue(workers=1) as q:
            bound = q.task(retries=1)(double)
            h = await asyncio.to_thread(bound, 6)
            await q.run()
        assert h.value() == 12

    async def test_scheduled_submit_from_foreign_thread(self):
        async with SharedQueue(workers=1) as q:
            h = await asyncio.to_thread(q.submit, double, 4, delay=0.05)
            summary = await q.run()
        assert summary.succeeded == 1
        assert h.value() == 8

    async def test_submit_during_active_run(self):
        async with SharedQueue(workers=2) as q:
            q.submit(slow, 0.3)
            run_task = asyncio.create_task(q.run())
            await asyncio.sleep(0.05)
            h = await asyncio.to_thread(q.submit, double, 21)
            summary = await run_task
        assert summary.succeeded == 2
        assert h.value() == 42


# -- Error propagation to the submitting thread ---------------------------------


class TestErrors:
    async def test_closed_error_raises_on_foreign_thread(self):
        async with SharedQueue(workers=1) as q:
            pass
        with pytest.raises(ClosedError):
            await asyncio.to_thread(q.submit, double, 1)

    async def test_queue_full_raises_on_foreign_thread(self):
        async with SharedQueue(workers=1, size=2) as q:
            q.submit(slow, 0.3)
            q.submit(slow, 0.3)
            with pytest.raises(QueueFullError):
                await asyncio.to_thread(q.submit, double, 1)
            summary = await q.run()
        assert summary.succeeded == 2

    async def test_unknown_option_raises_on_foreign_thread(self):
        async with SharedQueue(workers=1) as q:
            with pytest.raises(TypeError, match="Unknown task option"):
                await asyncio.to_thread(q.submit, double, 1, bogus=True)
            await q.run()


# -- Background loop, sync producers ---------------------------------------------


class TestBackgroundLoop:
    def test_producers_submit_to_background_loop(self):
        q = SharedQueue(workers=4, mode="infinite")
        started = threading.Event()

        async def serve():
            async with q:
                started.set()
                await q.run()

        runner = threading.Thread(target=asyncio.run, args=(serve(),))
        runner.start()
        try:
            assert started.wait(5)
            handles = [q.submit(double, i) for i in range(50)]
            latch = threading.Semaphore(0)
            for h in handles:
                h.add_done_callback(lambda _h: latch.release())
            for _ in handles:
                assert latch.acquire(timeout=5)
            assert sorted(h.value() for h in handles) == [i * 2 for i in range(50)]
        finally:
            q.cancel()
            runner.join(5)
        assert not runner.is_alive()
        assert q.closed
        with pytest.raises(ClosedError):
            q.submit(double, 99)

    def test_handle_cancel_from_producer_thread(self):
        q = SharedQueue(workers=1, mode="infinite")
        started = threading.Event()

        async def serve():
            async with q:
                started.set()
                await q.run()

        runner = threading.Thread(target=asyncio.run, args=(serve(),))
        runner.start()
        try:
            assert started.wait(5)
            blocker = q.submit(slow, 5)
            victim = q.submit(double, 1, delay=10)
            done = threading.Event()
            victim.add_done_callback(lambda _h: done.set())
            assert victim.cancel()
            assert done.wait(5)
            assert victim.cancelled()
            assert blocker.cancel()
        finally:
            q.cancel()
            runner.join(5)
        assert not runner.is_alive()
