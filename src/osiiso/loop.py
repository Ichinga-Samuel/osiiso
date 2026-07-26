"""Event-loop helper — :func:`run` executes a coroutine, on uvloop when available.

Uses :func:`asyncio.run`'s ``loop_factory`` so no global event-loop policy is
ever mutated.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

_uvloop_available: bool | None = None


def _check_uvloop() -> bool:
    """``True`` if uvloop is importable (cached after the first call)."""
    global _uvloop_available
    if _uvloop_available is None:
        try:
            import uvloop  # noqa: F401

            _uvloop_available = True
        except ImportError:
            _uvloop_available = False
    return _uvloop_available


def run[T](coro: Coroutine[Any, Any, T], *, use_uvloop: bool | None = None, debug: bool = False) -> T:
    """Run *coro* to completion, using uvloop when available.

    Args:
        coro: The coroutine to execute.
        use_uvloop: ``None`` (default) — uvloop if installed, else stdlib;
            ``True`` — require uvloop (``ImportError`` if missing);
            ``False`` — force the stdlib loop.
        debug: Enable asyncio debug mode.

    Returns:
        Whatever *coro* returns.

    Raises:
        ImportError: If ``use_uvloop=True`` but uvloop is not installed.

    Example::

        summary = osiiso.run(main())
    """
    factory = None
    if use_uvloop is True and not _check_uvloop():
        raise ImportError("uvloop is not installed. Install it with: pip install osiiso[uvloop]")
    if use_uvloop is not False and _check_uvloop():
        import uvloop

        factory = uvloop.new_event_loop
    return asyncio.run(coro, debug=debug, loop_factory=factory)
