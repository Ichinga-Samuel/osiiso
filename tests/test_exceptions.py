"""Tests for the osiiso exception hierarchy."""

from unittest.mock import MagicMock

import pytest

from osiiso.exceptions import ClosedError, ExecutionError, OsiisoError


class TestOsiisoError:
    """Tests for the base OsiisoError exception."""

    def test_instantiation_no_args(self):
        """OsiisoError can be instantiated with no arguments."""
        err = OsiisoError()
        assert isinstance(err, Exception)
        assert str(err) == ""

    def test_instantiation_with_message(self):
        """OsiisoError stores the provided message string."""
        err = OsiisoError("something went wrong")
        assert str(err) == "something went wrong"

    def test_is_exception_subclass(self):
        """OsiisoError inherits from the built-in Exception."""
        assert issubclass(OsiisoError, Exception)


class TestClosedError:
    """Tests for ClosedError."""

    def test_inherits_from_osiiso_error(self):
        """ClosedError is a subclass of OsiisoError."""
        assert issubclass(ClosedError, OsiisoError)

    def test_message_propagation(self):
        """ClosedError forwards its message to the base Exception."""
        err = ClosedError("queue is closed")
        assert str(err) == "queue is closed"

    def test_catchable_as_osiiso_error(self):
        """ClosedError can be caught with an 'except OsiisoError' clause."""
        with pytest.raises(OsiisoError):
            raise ClosedError("closed")


class TestExecutionError:
    """Tests for ExecutionError."""

    def test_inherits_from_osiiso_error(self):
        """ExecutionError is a subclass of OsiisoError."""
        assert issubclass(ExecutionError, OsiisoError)

    def test_results_converted_to_tuple_from_list(self):
        """A list of results passed to __init__ is stored as a tuple."""
        results = [MagicMock(), MagicMock()]
        err = ExecutionError(results)
        assert isinstance(err.results, tuple)
        assert len(err.results) == 2
        assert err.results[0] is results[0]
        assert err.results[1] is results[1]

    def test_results_converted_to_tuple_from_tuple(self):
        """A tuple of results passed to __init__ is stored as a tuple."""
        results = (MagicMock(),)
        err = ExecutionError(results)
        assert isinstance(err.results, tuple)
        assert err.results is not results or isinstance(err.results, tuple)

    def test_message_formatting_multiple(self):
        """Message reads '{n} task(s) failed' for multiple results."""
        err = ExecutionError([MagicMock(), MagicMock(), MagicMock()])
        assert str(err) == "3 task(s) failed"

    def test_message_formatting_single(self):
        """Message reads '1 task(s) failed' for a single result."""
        err = ExecutionError([MagicMock()])
        assert str(err) == "1 task(s) failed"

    def test_empty_results_list(self):
        """An empty results list produces '0 task(s) failed' and an empty tuple."""
        err = ExecutionError([])
        assert err.results == ()
        assert str(err) == "0 task(s) failed"

    def test_catchable_as_osiiso_error(self):
        """ExecutionError can be caught with an 'except OsiisoError' clause."""
        with pytest.raises(OsiisoError):
            raise ExecutionError([MagicMock()])
