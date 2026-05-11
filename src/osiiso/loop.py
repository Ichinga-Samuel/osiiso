"""Event loop helpers — uvloop integration and :func:`run` convenience wrapper.

:func:`run` is the recommended entry-point for executing osiiso coroutines
from synchronous code.  It automatically uses `uvloop`_ if it is installed,
unless the caller opts out.

.. _uvloop: https://github.com/MagicStack/uvloop
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

_uvloop_available: bool | None = None


def _check_uvloop() -> bool:
    """Return ``True`` if uvloop is importable on the current system.

    The result is cached after the first call so subsequent checks are free.

    Returns:
        ``True`` if uvloop can be imported, ``False`` otherwise.
    """
    global _uvloop_available
    if _uvloop_available is None:
        try:
            import uvloop as _uvloop  # noqa: F401

            _uvloop_available = True
        except ImportError:
            _uvloop_available = False
    return _uvloop_available


def _install_uvloop() -> None:
    """Set uvloop as the default asyncio event loop policy for this process."""
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def run[T](coro: Coroutine[Any, Any, T], *, use_uvloop: bool | None = None, debug: bool = False) -> T:
    """Run *coro* with optional uvloop acceleration.

    Args:
        coro: The coroutine to execute.
        use_uvloop:
            ``None`` (default) — use uvloop if installed, else stdlib.
            ``True`` — use uvloop; raise ``ImportError`` if missing.
            ``False`` — force stdlib asyncio loop.
        debug: Enable asyncio debug mode.

    Example::

        import osiiso

        async def main():
            async with osiiso.AsyncQueue(workers=4) as q:
                q.submit(work, 1)
                return await q.run()

        summary = osiiso.run(main())
    """
    if use_uvloop is True:
        if not _check_uvloop():
            raise ImportError("uvloop is not installed. Install it with: pip install osiiso[uvloop]")
        _install_uvloop()
    elif use_uvloop is None and _check_uvloop():
        _install_uvloop()

    try:
        return asyncio.run(coro, debug=debug)
    finally:
        # Restore default policy so we don't leak uvloop globally.
        if use_uvloop is not False and _check_uvloop():
            asyncio.set_event_loop_policy(None)
