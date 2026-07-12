"""Tests for the osiiso exception hierarchy."""

import pytest

from osiiso.exceptions import ClosedError, ExecutionError, OsiisoError, QueueFullError
from osiiso.result import TaskResult


def _failed(n: str = "t") -> TaskResult:
    return TaskResult(task_id=n, name=n, status="failed", exception=RuntimeError("boom"))


def _cancelled(n: str = "t") -> TaskResult:
    return TaskResult(task_id=n, name=n, status="cancelled")


class TestOsiisoError:
    """Tests for the base OsiisoError exception."""

    def test_instantiation_no_args(self):
        err = OsiisoError()
        assert isinstance(err, Exception)
        assert str(err) == ""

    def test_instantiation_with_message(self):
        err = OsiisoError("something went wrong")
        assert str(err) == "something went wrong"

    def test_is_exception_subclass(self):
        assert issubclass(OsiisoError, Exception)


class TestClosedError:
    """Tests for ClosedError."""

    def test_inherits_from_osiiso_error(self):
        assert issubclass(ClosedError, OsiisoError)

    def test_message_propagation(self):
        err = ClosedError("queue is closed")
        assert str(err) == "queue is closed"

    def test_catchable_as_osiiso_error(self):
        with pytest.raises(OsiisoError):
            raise ClosedError("closed")


class TestQueueFullError:
    """Tests for QueueFullError."""

    def test_inherits_from_osiiso_error(self):
        assert issubclass(QueueFullError, OsiisoError)

    def test_message_propagation(self):
        err = QueueFullError("queue is full")
        assert str(err) == "queue is full"


class TestExecutionError:
    """Tests for ExecutionError."""

    def test_inherits_from_osiiso_error(self):
        assert issubclass(ExecutionError, OsiisoError)

    def test_results_converted_to_tuple_from_list(self):
        results = [_failed("a"), _failed("b")]
        err = ExecutionError(results)
        assert isinstance(err.results, tuple)
        assert len(err.results) == 2
        assert err.results[0] is results[0]
        assert err.results[1] is results[1]

    def test_results_converted_to_tuple_from_tuple(self):
        results = (_failed("a"),)
        err = ExecutionError(results)
        assert isinstance(err.results, tuple)

    def test_message_formatting_multiple(self):
        err = ExecutionError([_failed("a"), _failed("b"), _failed("c")])
        assert str(err) == "3 task(s) failed"

    def test_message_formatting_single(self):
        err = ExecutionError([_failed()])
        assert str(err) == "1 task(s) failed"

    def test_message_counts_cancelled(self):
        err = ExecutionError([_failed(), _cancelled(), _cancelled()])
        assert str(err) == "1 task(s) failed; 2 task(s) cancelled"

    def test_message_all_cancelled(self):
        err = ExecutionError([_cancelled()])
        assert str(err) == "1 task(s) cancelled"

    def test_empty_results_list(self):
        err = ExecutionError([])
        assert err.results == ()
        assert str(err) == "0 task(s) failed"

    def test_catchable_as_osiiso_error(self):
        with pytest.raises(OsiisoError):
            raise ExecutionError([_failed()])
