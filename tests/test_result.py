"""Tests for osiiso.result — TaskResult, RunSummary, and make_result."""

import time
from unittest.mock import MagicMock, patch

import pytest

from osiiso.exceptions import ExecutionError
from osiiso.result import RunSummary, TaskResult, make_result


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_task_result(
    *,
    task_id: str = "aaa",
    name: str = "task",
    status: str = "succeeded",
    value=None,
    exception=None,
    attempts: int = 1,
    priority: int = 0,
    must_complete: bool = False,
    group_id: str | None = None,
    detached: bool = False,
    scheduled_for: float | None = None,
    created_at: float = 0.0,
    started_at: float | None = 1.0,
    finished_at: float = 2.0,
    duration: float = 1.0,
    message: str = "",
) -> TaskResult:
    """Helper to build a TaskResult with convenient defaults."""
    return TaskResult(
        task_id=task_id,
        name=name,
        status=status,
        value=value,
        exception=exception,
        attempts=attempts,
        priority=priority,
        must_complete=must_complete,
        group_id=group_id,
        detached=detached,
        scheduled_for=scheduled_for,
        created_at=created_at,
        started_at=started_at,
        finished_at=finished_at,
        duration=duration,
        message=message,
    )


def _make_summary(
    results: list[TaskResult],
    *,
    timed_out: bool = False,
) -> RunSummary:
    """Build a RunSummary from a list of TaskResult objects.

    Uses from_results to exercise the real aggregation logic, but patches
    perf_counter so the duration is deterministic.
    """
    with patch("osiiso.result.time") as mock_time:
        mock_time.perf_counter.return_value = 10.0
        return RunSummary.from_results(results, run_start=5.0, timed_out=timed_out)


# ===================================================================
# TestTaskResult
# ===================================================================

class TestTaskResult:
    """Tests for the TaskResult frozen dataclass."""

    def test_construction_all_fields(self):
        """All fields are stored correctly when explicitly provided."""
        exc = ValueError("boom")
        r = TaskResult(
            task_id="abc123",
            name="my_task",
            status="failed",
            value=None,
            exception=exc,
            attempts=3,
            priority=2,
            must_complete=True,
            group_id="grp1",
            detached=True,
            scheduled_for=100.0,
            created_at=90.0,
            started_at=95.0,
            finished_at=99.0,
            duration=4.0,
            message="it broke",
        )
        assert r.task_id == "abc123"
        assert r.name == "my_task"
        assert r.status == "failed"
        assert r.value is None
        assert r.exception is exc
        assert r.attempts == 3
        assert r.priority == 2
        assert r.must_complete is True
        assert r.group_id == "grp1"
        assert r.detached is True
        assert r.scheduled_for == 100.0
        assert r.created_at == 90.0
        assert r.started_at == 95.0
        assert r.finished_at == 99.0
        assert r.duration == 4.0
        assert r.message == "it broke"

    def test_frozen_immutable(self):
        """TaskResult is frozen — attribute assignment raises AttributeError."""
        r = _make_task_result()
        with pytest.raises(AttributeError):
            r.name = "other"  # type: ignore[misc]

    def test_default_values(self):
        """Only task_id, name, and status are required; others have defaults."""
        r = TaskResult(task_id="x", name="t", status="succeeded")
        assert r.value is None
        assert r.exception is None
        assert r.attempts == 0
        assert r.priority == 0
        assert r.must_complete is False
        assert r.group_id is None
        assert r.detached is False
        assert r.scheduled_for is None
        assert r.created_at == 0.0
        assert r.started_at is None
        assert r.finished_at == 0.0
        assert r.duration == 0.0
        assert r.message == ""


# ===================================================================
# TestRunSummary
# ===================================================================

class TestRunSummary:
    """Tests for the RunSummary frozen dataclass."""

    # ---- ok property ----

    def test_ok_true_when_all_succeeded(self):
        """ok is True when there are no failures, cancellations, or timeouts."""
        results = [
            _make_task_result(task_id="1", status="succeeded"),
            _make_task_result(task_id="2", status="succeeded"),
        ]
        summary = _make_summary(results)
        assert summary.ok is True

    def test_ok_false_when_failed(self):
        """ok is False when failed > 0."""
        results = [
            _make_task_result(task_id="1", status="succeeded"),
            _make_task_result(task_id="2", status="failed", exception=RuntimeError("x")),
        ]
        summary = _make_summary(results)
        assert summary.ok is False

    def test_ok_false_when_cancelled(self):
        """ok is False when cancelled > 0."""
        results = [
            _make_task_result(task_id="1", status="cancelled"),
        ]
        summary = _make_summary(results)
        assert summary.ok is False

    def test_ok_false_when_timed_out(self):
        """ok is False when timed_out is True, even with no failures."""
        results = [
            _make_task_result(task_id="1", status="succeeded"),
        ]
        summary = _make_summary(results, timed_out=True)
        assert summary.ok is False

    # ---- errors property ----

    def test_errors_filters_failed(self):
        """errors returns only TaskResult objects with status == 'failed'."""
        r_ok = _make_task_result(task_id="1", status="succeeded")
        r_fail = _make_task_result(task_id="2", status="failed", exception=RuntimeError("x"))
        r_cancel = _make_task_result(task_id="3", status="cancelled")
        summary = _make_summary([r_ok, r_fail, r_cancel])
        assert summary.errors == (r_fail,)

    # ---- values property ----

    def test_values_maps_succeeded(self):
        """values returns (value,) tuples from succeeded results only."""
        r1 = _make_task_result(task_id="1", status="succeeded", value=42)
        r2 = _make_task_result(task_id="2", status="failed", exception=RuntimeError("x"))
        r3 = _make_task_result(task_id="3", status="succeeded", value="hello")
        summary = _make_summary([r1, r2, r3])
        assert summary.values == (42, "hello")

    # ---- by_task_id ----

    def test_by_task_id(self):
        """by_task_id returns a dict keyed by task_id."""
        r1 = _make_task_result(task_id="aaa")
        r2 = _make_task_result(task_id="bbb")
        summary = _make_summary([r1, r2])
        indexed = summary.by_task_id()
        assert indexed["aaa"] is r1
        assert indexed["bbb"] is r2
        assert len(indexed) == 2

    # ---- by_name ----

    def test_by_name(self):
        """by_name groups results by name in a dict of tuples."""
        r1 = _make_task_result(task_id="1", name="alpha")
        r2 = _make_task_result(task_id="2", name="beta")
        r3 = _make_task_result(task_id="3", name="alpha")
        summary = _make_summary([r1, r2, r3])
        grouped = summary.by_name()
        assert grouped["alpha"] == (r1, r3)
        assert grouped["beta"] == (r2,)

    # ---- by_group ----

    def test_by_group(self):
        """by_group groups results by group_id (including None)."""
        r1 = _make_task_result(task_id="1", group_id="g1")
        r2 = _make_task_result(task_id="2", group_id="g1")
        r3 = _make_task_result(task_id="3", group_id=None)
        summary = _make_summary([r1, r2, r3])
        grouped = summary.by_group()
        assert grouped["g1"] == (r1, r2)
        assert grouped[None] == (r3,)

    # ---- successes ----

    def test_successes(self):
        """successes filters to only succeeded TaskResults."""
        r_ok = _make_task_result(task_id="1", status="succeeded")
        r_fail = _make_task_result(task_id="2", status="failed", exception=RuntimeError("x"))
        summary = _make_summary([r_ok, r_fail])
        assert summary.successes() == (r_ok,)

    # ---- cancellations ----

    def test_cancellations(self):
        """cancellations filters to only cancelled TaskResults."""
        r_ok = _make_task_result(task_id="1", status="succeeded")
        r_cancel = _make_task_result(task_id="2", status="cancelled")
        summary = _make_summary([r_ok, r_cancel])
        assert summary.cancellations() == (r_cancel,)

    # ---- raise_for_errors ----

    def test_raise_for_errors_raises_on_failures(self):
        """raise_for_errors raises ExecutionError containing failed results."""
        r_fail = _make_task_result(task_id="1", status="failed", exception=RuntimeError("x"))
        summary = _make_summary([r_fail])
        with pytest.raises(ExecutionError) as exc_info:
            summary.raise_for_errors()
        assert exc_info.value.results == (r_fail,)

    def test_raise_for_errors_noop_when_ok(self):
        """raise_for_errors does nothing when all tasks succeeded."""
        r_ok = _make_task_result(task_id="1", status="succeeded")
        summary = _make_summary([r_ok])
        summary.raise_for_errors()  # should not raise

    # ---- display ----

    def test_display_passed(self, capsys):
        """display prints 'PASSED' when the run is ok."""
        r_ok = _make_task_result(task_id="1", status="succeeded")
        summary = _make_summary([r_ok])
        summary.display()
        captured = capsys.readouterr()
        assert "PASSED" in captured.out

    def test_display_timed_out(self, capsys):
        """display prints 'TIMED OUT' when timed_out is True."""
        r_ok = _make_task_result(task_id="1", status="succeeded")
        summary = _make_summary([r_ok], timed_out=True)
        summary.display()
        captured = capsys.readouterr()
        assert "TIMED OUT" in captured.out

    def test_display_error_with_attempts(self, capsys):
        """display shows attempt count for failed tasks with attempts > 1."""
        r_fail = _make_task_result(
            task_id="1",
            name="flaky_task",
            status="failed",
            exception=RuntimeError("x"),
            attempts=3,
        )
        summary = _make_summary([r_fail])
        summary.display()
        captured = capsys.readouterr()
        assert "3 attempts" in captured.out
        assert "flaky_task" in captured.out

    def test_display_error_with_message(self, capsys):
        """display shows the message when present on a failed result."""
        r_fail = _make_task_result(
            task_id="1",
            name="bad_task",
            status="failed",
            exception=RuntimeError("x"),
            message="RuntimeError: x",
        )
        summary = _make_summary([r_fail])
        summary.display()
        captured = capsys.readouterr()
        assert "RuntimeError: x" in captured.out

    # ---- from_results ----

    def test_from_results_aggregates_counts(self):
        """from_results correctly counts succeeded, failed, cancelled."""
        results = [
            _make_task_result(task_id="1", status="succeeded"),
            _make_task_result(task_id="2", status="succeeded"),
            _make_task_result(task_id="3", status="failed", exception=RuntimeError("x")),
            _make_task_result(task_id="4", status="cancelled"),
            _make_task_result(task_id="5", status="cancelled"),
            _make_task_result(task_id="6", status="cancelled"),
        ]
        summary = _make_summary(results)
        assert summary.total_submitted == 6
        assert summary.succeeded == 2
        assert summary.failed == 1
        assert summary.cancelled == 3
        assert summary.timed_out is False
        assert summary.duration == pytest.approx(5.0)  # 10.0 - 5.0
        assert len(summary.results) == 6


# ===================================================================
# TestMakeResult
# ===================================================================

class TestMakeResult:
    """Tests for the make_result() factory function."""

    @staticmethod
    def _mock_handle(
        *,
        task_id="abc",
        name="my_fn",
        attempts=1,
        priority=3,
        must_complete=False,
        group_id=None,
        detached=False,
        scheduled_for=None,
        created_at=100.0,
        started_at=105.0,
    ):
        """Build a mock object with the attributes make_result reads."""
        handle = MagicMock()
        handle.task_id = task_id
        handle.name = name
        handle.attempts = attempts
        handle.priority = priority
        handle.must_complete = must_complete
        handle.group_id = group_id
        handle.detached = detached
        handle.scheduled_for = scheduled_for
        handle.created_at = created_at
        handle._started_at = started_at
        return handle

    def test_duration_when_started(self):
        """Duration equals finished_at - started_at when started_at is set."""
        handle = self._mock_handle(started_at=100.0)
        with patch("osiiso.result.time") as mock_time:
            mock_time.perf_counter.return_value = 110.0
            result = make_result(handle, status="succeeded", value=42)
        assert result.duration == pytest.approx(10.0)
        assert result.finished_at == pytest.approx(110.0)
        assert result.started_at == 100.0

    def test_duration_zero_when_not_started(self):
        """Duration is 0.0 when started_at is None (cancelled before starting)."""
        handle = self._mock_handle(started_at=None)
        result = make_result(handle, status="cancelled")
        assert result.duration == 0.0
        assert result.started_at is None

    def test_all_handle_metadata_copied(self):
        """All metadata fields from the handle are faithfully copied."""
        handle = self._mock_handle(
            task_id="xyz789",
            name="do_work",
            attempts=5,
            priority=1,
            must_complete=True,
            group_id="batch-1",
            detached=True,
            scheduled_for=200.0,
            created_at=150.0,
            started_at=160.0,
        )
        exc = TypeError("oops")
        result = make_result(
            handle,
            status="failed",
            value=None,
            exception=exc,
            message="TypeError: oops",
        )
        assert result.task_id == "xyz789"
        assert result.name == "do_work"
        assert result.status == "failed"
        assert result.exception is exc
        assert result.attempts == 5
        assert result.priority == 1
        assert result.must_complete is True
        assert result.group_id == "batch-1"
        assert result.detached is True
        assert result.scheduled_for == 200.0
        assert result.created_at == 150.0
        assert result.message == "TypeError: oops"
