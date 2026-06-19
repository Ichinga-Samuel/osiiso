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


class TestTaskOptionsExtended:
    """Additional validation and edge-case tests for TaskOptions."""

    def test_replace_with_invalid_triggers_validation(self):
        """replace() delegates to __post_init__, so invalid values still raise."""
        opts = TaskOptions()
        with pytest.raises(ValueError, match="timeout must be > 0"):
            opts.replace(timeout=-1)

    def test_option_fields_contents(self):
        """OPTION_FIELDS frozenset must exactly match the dataclass field names."""
        import dataclasses

        from osiiso.options import OPTION_FIELDS

        expected = frozenset(f.name for f in dataclasses.fields(TaskOptions))
        assert OPTION_FIELDS == expected

    def test_valid_edge_values(self):
        """Boundary values just inside the valid range must not raise."""
        opts_timeout = TaskOptions(timeout=0.001)
        assert opts_timeout.timeout == 0.001

        opts_delay = TaskOptions(delay=0)
        assert opts_delay.delay == 0

        opts_retries = TaskOptions(retries=0)
        assert opts_retries.retries == 0

    def test_timeout_zero_raises(self):
        """timeout=0 is not > 0, so it must raise ValueError."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            TaskOptions(timeout=0)

    def test_delay_none_and_run_at_none(self):
        """Both delay=None and run_at=None is the default (no scheduling)."""
        opts = TaskOptions()
        assert opts.delay is None
        assert opts.run_at is None

    def test_replace_preserves_unset_fields(self):
        """replace() only changes the specified fields; others stay the same."""
        original = TaskOptions(priority=1, retries=5, timeout=10, name="orig")
        replaced = original.replace(priority=9)
        assert replaced.priority == 9
        assert replaced.retries == 5
        assert replaced.timeout == 10
        assert replaced.name == "orig"
        # Verify defaults are also preserved
        assert replaced.must_complete is False
        assert replaced.detached is False
