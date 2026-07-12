"""One-shot mapping helpers — build a queue, run it, return values in input order.

Each helper raises :class:`~osiiso.ExecutionError` if any task failed or was
cancelled, so a returned tuple always lines up 1:1 with the input.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .asyncqueue import AsyncQueue
from .processqueue import ProcessQueue
from .threadqueue import ThreadQueue


async def amap(
    fn: Any,
    iterable: Iterable[Any],
    *,
    workers: int | None = None,
    rate: float | None = None,
    timeout: float | None = None,
    **options: Any,
) -> tuple[Any, ...]:
    """Run *fn* over *iterable* on an :class:`~osiiso.AsyncQueue` and return ordered values.

    Args:
        workers / rate: Forwarded to the queue.
        timeout: Overall run limit in seconds.
        **options: :class:`~osiiso.TaskOptions` fields (``retries=3``, ...).

    Raises:
        ~osiiso.ExecutionError: If any task failed or was cancelled.

    Example::

        pages = await osiiso.amap(fetch, urls, workers=8, retries=2)
    """
    async with AsyncQueue(workers=workers, rate=rate) as q:
        grp = q.group(fn, iterable, **options)
        await q.run(timeout=timeout)
        return await grp.values()


def tmap(
    fn: Any,
    iterable: Iterable[Any],
    *,
    workers: int | None = None,
    rate: float | None = None,
    timeout: float | None = None,
    **options: Any,
) -> tuple[Any, ...]:
    """Run *fn* over *iterable* on a :class:`~osiiso.ThreadQueue` and return ordered values.

    See :func:`amap` for arguments and failure behaviour.
    """
    with ThreadQueue(workers=workers, rate=rate) as q:
        grp = q.group(fn, iterable, **options)
        q.run(timeout=timeout)
        return grp.values()


def pmap(
    fn: Any,
    iterable: Iterable[Any],
    *,
    workers: int | None = None,
    rate: float | None = None,
    timeout: float | None = None,
    context: Any = None,
    initializer: Any = None,
    initargs: tuple[Any, ...] = (),
    **options: Any,
) -> tuple[Any, ...]:
    """Run *fn* over *iterable* on a :class:`~osiiso.ProcessQueue` and return ordered values.

    See :func:`amap` for arguments and failure behaviour; *context*,
    *initializer*, and *initargs* are forwarded to the queue.
    """
    with ProcessQueue(workers=workers, rate=rate, context=context, initializer=initializer, initargs=initargs) as q:
        grp = q.group(fn, iterable, **options)
        q.run(timeout=timeout)
        return grp.values()
