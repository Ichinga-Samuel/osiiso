"""Tests for the event loop helpers in osiiso.loop."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

import osiiso.loop as loop_module
from osiiso.loop import _check_uvloop, run


class TestRun:
    """Tests for the run() convenience wrapper."""

    def test_basic_coroutine_returns_result(self):
        """run() executes a coroutine and returns its result."""
        async def hello():
            return "world"

        result = run(hello(), use_uvloop=False)
        assert result == "world"

    def test_debug_enables_debug_mode(self):
        """When debug=True, asyncio runs in debug mode."""
        async def check_debug():
            return asyncio.get_event_loop().get_debug()

        result = run(check_debug(), use_uvloop=False, debug=True)
        assert result is True

    def test_debug_disabled_by_default(self):
        """When debug is not set, asyncio debug mode is off."""
        async def check_debug():
            return asyncio.get_event_loop().get_debug()

        result = run(check_debug(), use_uvloop=False, debug=False)
        assert result is False

    def test_use_uvloop_false_uses_stdlib(self):
        """use_uvloop=False runs with the standard asyncio loop."""
        async def get_loop_type():
            return type(asyncio.get_event_loop()).__name__

        result = run(get_loop_type(), use_uvloop=False)
        # On Windows the default is ProactorEventLoop or SelectorEventLoop
        assert "EventLoop" in result

    @patch.object(loop_module, "_check_uvloop", return_value=False)
    def test_use_uvloop_true_raises_when_unavailable(self, mock_check):
        """use_uvloop=True raises ImportError if uvloop is not installed."""
        async def noop():
            pass

        coro = noop()
        try:
            with pytest.raises(ImportError, match="uvloop is not installed"):
                run(coro, use_uvloop=True)
        finally:
            coro.close()

    @patch.object(loop_module, "_check_uvloop", return_value=False)
    def test_use_uvloop_none_falls_back_to_stdlib(self, mock_check):
        """use_uvloop=None (default) falls back to stdlib when uvloop is absent."""
        async def hello():
            return 42

        result = run(hello(), use_uvloop=None)
        assert result == 42

    def test_policy_restored_after_execution(self):
        """The event loop policy is not leaked after run() completes."""
        policy_before = asyncio.get_event_loop_policy()

        async def noop():
            return 1

        run(noop(), use_uvloop=False)
        policy_after = asyncio.get_event_loop_policy()
        # With use_uvloop=False the policy should not be changed at all
        assert type(policy_after) is type(policy_before)


class TestCheckUvloop:
    """Tests for the _check_uvloop caching helper."""

    def test_returns_bool(self):
        """_check_uvloop returns a boolean."""
        result = _check_uvloop()
        assert isinstance(result, bool)

    def test_caching_behaviour(self):
        """After first call, _check_uvloop returns the cached value without re-importing."""
        # Save and reset the module-level cache
        original = loop_module._uvloop_available
        try:
            loop_module._uvloop_available = None  # reset cache

            with patch("builtins.__import__", side_effect=ImportError("no uvloop")):
                result1 = _check_uvloop()
                assert result1 is False

            # Second call should use cached value even though import is no longer mocked
            result2 = _check_uvloop()
            assert result2 is False
            assert loop_module._uvloop_available is False
        finally:
            # Restore original cache state
            loop_module._uvloop_available = original

    def test_caching_true_when_importable(self):
        """When uvloop is importable, the cached value is True."""
        original = loop_module._uvloop_available
        try:
            loop_module._uvloop_available = None  # reset cache

            fake_uvloop = MagicMock()
            with patch.dict("sys.modules", {"uvloop": fake_uvloop}):
                result = _check_uvloop()
                assert result is True
                assert loop_module._uvloop_available is True
        finally:
            loop_module._uvloop_available = original
