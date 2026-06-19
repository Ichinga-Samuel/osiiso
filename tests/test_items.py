"""Tests for internal task item classes and helper functions."""

from __future__ import annotations

import asyncio
import inspect
import time
from unittest.mock import MagicMock, patch

import pytest

from osiiso.items import (
    AsyncItem,
    ProcessItem,
    ThreadItem,
    _resolve_name,
    _resolve_schedule,
)
from osiiso.options import TaskOptions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _plain_function():
    """A plain sync function for testing."""
    return "sync result"


async def _async_function(x: int = 1) -> int:
    """A coroutine function for testing."""
    return x * 2


def _default_opts(**kw) -> TaskOptions:
    """Shortcut for creating a TaskOptions with defaults."""
    return TaskOptions(**kw)


# ---------------------------------------------------------------------------
# _resolve_name
# ---------------------------------------------------------------------------

class TestResolveName:
    """Tests for the _resolve_name helper."""

    def test_regular_callable_uses_name(self):
        """A regular function's __name__ attribute is returned."""
        name = _resolve_name(_plain_function)
        assert name == "_plain_function"

    def test_coroutine_function_uses_name(self):
        """A coroutine function's __name__ attribute is returned."""
        name = _resolve_name(_async_function)
        assert name == "_async_function"

    def test_awaitable_with_cr_code(self):
        """An awaitable coroutine object uses its cr_code.co_name."""
        coro = _async_function(5)
        try:
            name = _resolve_name(coro)
            assert name == "_async_function"
        finally:
            coro.close()

    def test_awaitable_without_cr_code_uses_type_name(self):
        """An awaitable without cr_code falls back to type(fn).__name__."""

        class CustomAwaitable:
            def __await__(self):
                yield
                return 42

        obj = CustomAwaitable()
        assert inspect.isawaitable(obj)
        name = _resolve_name(obj)
        assert name == "CustomAwaitable"

    def test_lambda_gets_name(self):
        """A lambda's __name__ attribute ('<lambda>') is returned."""
        fn = lambda: None  # noqa: E731
        name = _resolve_name(fn)
        assert name == "<lambda>"

    def test_callable_object_without_name(self):
        """A callable object without __name__ uses type name."""

        class Worker:
            def __call__(self):
                pass

        w = Worker()
        # callable objects have no __name__ by default
        name = _resolve_name(w)
        assert name == "Worker"


# ---------------------------------------------------------------------------
# _resolve_schedule
# ---------------------------------------------------------------------------

class TestResolveSchedule:
    """Tests for the _resolve_schedule helper."""

    def test_delay_produces_absolute_target(self):
        """When delay is set, result ≈ perf_counter() + delay."""
        opts = _default_opts(delay=5.0)
        before = time.perf_counter()
        result = _resolve_schedule(opts)
        after = time.perf_counter()
        assert result is not None
        # The result should be roughly perf_counter() + 5.0
        assert before + 5.0 <= result <= after + 5.0

    def test_run_at_uses_run_at_value(self):
        """When run_at is set, result is perf_counter + max(0, run_at - time.time())."""
        future_time = time.time() + 10.0
        opts = _default_opts(run_at=future_time)
        before = time.perf_counter()
        result = _resolve_schedule(opts)
        after = time.perf_counter()
        assert result is not None
        # Should be roughly perf_counter() + 10 (±small margin)
        assert before + 9.0 <= result <= after + 11.0

    def test_run_at_in_past_clamps_to_zero(self):
        """When run_at is in the past, the delta is clamped to 0."""
        past_time = time.time() - 100.0
        opts = _default_opts(run_at=past_time)
        before = time.perf_counter()
        result = _resolve_schedule(opts)
        after = time.perf_counter()
        assert result is not None
        # max(0, past - now) == 0, so result ≈ perf_counter()
        assert before <= result <= after + 0.1

    def test_neither_set_returns_none(self):
        """When neither delay nor run_at is set, None is returned."""
        opts = _default_opts()
        result = _resolve_schedule(opts)
        assert result is None


# ---------------------------------------------------------------------------
# AsyncItem
# ---------------------------------------------------------------------------

class TestAsyncItem:
    """Tests for AsyncItem."""

    def test_construction_with_callable(self):
        """AsyncItem stores fn, args, and opts correctly."""
        opts = _default_opts()
        item = AsyncItem(_plain_function, (1, 2), opts)
        assert item.fn is _plain_function
        assert item.args == (1, 2)
        assert item.opts is opts

    def test_task_id_is_uuid_hex(self):
        """task_id is a 32-character hex string."""
        item = AsyncItem(_plain_function, (), _default_opts())
        assert isinstance(item.task_id, str)
        assert len(item.task_id) == 32
        int(item.task_id, 16)  # must be valid hex

    def test_name_resolved_from_function(self):
        """name is derived from the callable when opts.name is None."""
        item = AsyncItem(_plain_function, (), _default_opts())
        assert item.name == "_plain_function"

    def test_name_from_opts_overrides(self):
        """opts.name overrides the auto-resolved name."""
        item = AsyncItem(_plain_function, (), _default_opts(name="custom"))
        assert item.name == "custom"

    @pytest.mark.asyncio
    async def test_call_with_awaitable_object(self):
        """An awaitable passed as fn is awaited directly, args are ignored."""
        coro = _async_function(7)
        item = AsyncItem(coro, (), _default_opts())
        result = await item()
        assert result == 14

    @pytest.mark.asyncio
    async def test_call_with_coroutine_function(self):
        """A coroutine function is called with args then awaited."""
        item = AsyncItem(_async_function, (5,), _default_opts())
        result = await item()
        assert result == 10

    @pytest.mark.asyncio
    async def test_call_with_sync_function(self):
        """A sync function is offloaded via asyncio.to_thread."""
        def add(a, b):
            return a + b

        item = AsyncItem(add, (3, 4), _default_opts())
        result = await item()
        assert result == 7

    @pytest.mark.asyncio
    async def test_call_sync_returning_awaitable(self):
        """If a sync function returns an awaitable, that awaitable is awaited."""
        async def inner():
            return 99

        def factory():
            return inner()

        item = AsyncItem(factory, (), _default_opts())
        result = await item()
        assert result == 99


# ---------------------------------------------------------------------------
# ThreadItem
# ---------------------------------------------------------------------------

class TestThreadItem:
    """Tests for ThreadItem."""

    def test_construction_with_sync_callable(self):
        """ThreadItem accepts a plain sync callable."""
        item = ThreadItem(_plain_function, (), _default_opts())
        assert item.fn is _plain_function

    def test_rejects_awaitable(self):
        """ThreadItem rejects an awaitable with TypeError."""
        coro = _async_function(1)
        try:
            with pytest.raises(TypeError, match="thread tasks must be callable, not awaitable"):
                ThreadItem(coro, (), _default_opts())
        finally:
            coro.close()

    def test_rejects_coroutine_function(self):
        """ThreadItem rejects a coroutine function with TypeError."""
        with pytest.raises(TypeError, match="thread tasks must be sync callable"):
            ThreadItem(_async_function, (), _default_opts())

    def test_call_invokes_fn_with_args(self):
        """__call__ invokes the wrapped function with positional args."""
        def multiply(a, b):
            return a * b

        item = ThreadItem(multiply, (6, 7), _default_opts())
        assert item() == 42


# ---------------------------------------------------------------------------
# ProcessItem
# ---------------------------------------------------------------------------

class TestProcessItem:
    """Tests for ProcessItem."""

    def test_rejects_bare_awaitable(self):
        """ProcessItem rejects a bare awaitable with TypeError."""
        coro = _async_function(1)
        try:
            with pytest.raises(TypeError, match="process tasks must be callable, not awaitable"):
                ProcessItem(coro, (), _default_opts())
        finally:
            coro.close()

    def test_accepts_coroutine_function(self):
        """ProcessItem accepts coroutine functions (they'll be run via asyncio.run)."""
        item = ProcessItem(_async_function, (3,), _default_opts())
        assert item.fn is _async_function

    def test_call_coroutine_function_uses_asyncio_run(self):
        """A coroutine function is executed with asyncio.run."""
        async def double(x):
            return x * 2

        item = ProcessItem(double, (5,), _default_opts())
        result = item()
        assert result == 10

    def test_call_sync_function_returning_awaitable(self):
        """If a sync function returns an awaitable, it's run with asyncio.run."""
        async def inner():
            return 77

        def factory():
            return inner()

        item = ProcessItem(factory, (), _default_opts())
        result = item()
        assert result == 77

    def test_call_plain_sync_function(self):
        """A plain sync function is called directly."""
        def greet(name):
            return f"hello {name}"

        item = ProcessItem(greet, ("world",), _default_opts())
        result = item()
        assert result == "hello world"
