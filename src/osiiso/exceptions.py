"""Public exception hierarchy for the osiiso package.

All exceptions raised by osiiso descend from :class:`OsiisoError`, so callers
can use a single ``except OsiisoError`` clause to catch library-specific errors
without suppressing unrelated exceptions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .result import TaskResult


class OsiisoError(Exception):
    """Base exception for the osiiso package."""


class ClosedError(OsiisoError):
    """Raised when a task is submitted to a queue that is closed or shutting down.

    Thrown by ``submit()``, ``map()``, and ``group()`` when called after
    ``shutdown()`` has been initiated or after the context-manager block exits.
    """


class ExecutionError(OsiisoError):
    """Raised when one or more tasks fail during queue execution.

    Attributes:
        results: Tuple of :class:`~osiiso.TaskResult` objects whose
            ``status`` is ``"failed"``.
    """

    def __init__(self, results: list[TaskResult] | tuple[TaskResult, ...]):
        """Initialise the error.

        Args:
            results: The failed :class:`~osiiso.TaskResult` objects.
        """
        self.results = tuple(results)
        super().__init__(f"{len(self.results)} task(s) failed")
