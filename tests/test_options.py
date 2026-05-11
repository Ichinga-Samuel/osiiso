"""Tests for TaskOptions."""

import pytest

from osiiso import TaskOptions
from osiiso.options import resolve_opts


class TestTaskOptions:
    def test_defaults(self):
        opts = TaskOptions()
        assert opts.priority == 3
        assert opts.retries == 0
        assert opts.timeout is None
        assert opts.must_complete is False
        assert opts.detached is False

    def test_custom_values(self):
        opts = TaskOptions(priority=1, retries=5, timeout=30, backoff=2.0)
        assert opts.priority == 1
        assert opts.retries == 5
        assert opts.timeout == 30
        assert opts.backoff == 2.0

    def test_immutable(self):
        opts = TaskOptions()
        with pytest.raises(AttributeError):
            opts.priority = 10  # type: ignore[misc]

    def test_replace(self):
        opts = TaskOptions(priority=1, retries=3)
        new = opts.replace(priority=5)
        assert new.priority == 5
        assert new.retries == 3
        assert opts.priority == 1  # original unchanged

    def test_validation_timeout(self):
        with pytest.raises(ValueError, match="timeout must be > 0"):
            TaskOptions(timeout=-1)
        with pytest.raises(ValueError, match="timeout must be > 0"):
            TaskOptions(timeout=0)

    def test_validation_retries(self):
        with pytest.raises(ValueError, match="retries must be >= 0"):
            TaskOptions(retries=-1)

    def test_validation_retry_delay(self):
        with pytest.raises(ValueError, match="retry_delay must be >= 0"):
            TaskOptions(retry_delay=-1)

    def test_validation_backoff(self):
        with pytest.raises(ValueError, match="backoff must be > 0"):
            TaskOptions(backoff=0)

    def test_validation_delay(self):
        with pytest.raises(ValueError, match="delay must be >= 0"):
            TaskOptions(delay=-1)

    def test_validation_delay_run_at_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            TaskOptions(delay=1, run_at=1000)


class TestResolveOpts:
    def test_no_overrides(self):
        opts = TaskOptions(retries=3)
        result = resolve_opts(opts, {})
        assert result is opts

    def test_no_base(self):
        result = resolve_opts(None, {"retries": 5})
        assert result.retries == 5
        assert result.priority == 3  # default

    def test_override_base(self):
        base = TaskOptions(retries=3, priority=1)
        result = resolve_opts(base, {"priority": 10})
        assert result.retries == 3
        assert result.priority == 10

    def test_unknown_key_raises(self):
        with pytest.raises(TypeError, match="Unknown task option"):
            resolve_opts(None, {"unknown_key": 42})

    def test_empty_both(self):
        result = resolve_opts(None, {})
        assert result == TaskOptions()
