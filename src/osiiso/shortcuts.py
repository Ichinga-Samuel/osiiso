"""One-shot mapping helpers — build a queue, run it, return values in input order.

Each helper raises :class:`~osiiso.ExecutionError` if any task failed or was
cancelled, so a returned tuple always lines up 1:1 with the input.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from .asyncqueue import AsyncQueue
from .processqueue import ProcessQueue
from .threadqueue import ThreadQueue

if TYPE_CHECKING:
    from .checkpoint import Checkpoint


async def amap(
    fn: Any,
    iterable: Iterable[Any],
    *,
    workers: int | None = None,
    rate: float | None = None,
    timeout: float | None = None,
    checkpoint: Checkpoint | None = None,
    key: Callable[[Any], Any] | None = None,
    namespace: str | None = None,
    **options: Any,
) -> tuple[Any, ...]:
    """Run *fn* over *iterable* on an :class:`~osiiso.AsyncQueue` and return ordered values.

    Args:
        fn: The callable to run for every element.
        iterable: Elements to fan *fn* out over, interpreted as in ``queue.map()``.
        workers / rate: Forwarded to the queue.
        timeout: Overall run limit in seconds.
        checkpoint: A :class:`~osiiso.Checkpoint` to resume against — elements it
            has already recorded are not re-run, and their stored values are
            returned in place.
        key: Derives the checkpoint key from an element (default: canonical JSON).
        namespace: Isolates this fan-out inside the checkpoint (default: the name of *fn*).
        **options: :class:`~osiiso.TaskOptions` fields (``retries=3``, ...).

    Returns:
        The return values, in input order.

    Raises:
        ~osiiso.ExecutionError: If any task failed or was cancelled.

    Example::

        pages = await osiiso.amap(fetch, urls, workers=8, retries=2)

        # Resumable: a second run after a crash only fetches what is missing.
        pages = await osiiso.amap(fetch, urls, checkpoint=cp, retries=2)
    """
    async with AsyncQueue(workers=workers, rate=rate) as q:
        grp = q.group(fn, iterable, checkpoint=checkpoint, key=key, namespace=namespace, **options)
        await q.run(timeout=timeout)
        return await grp.values()


def tmap(
    fn: Any,
    iterable: Iterable[Any],
    *,
    workers: int | None = None,
    rate: float | None = None,
    timeout: float | None = None,
    checkpoint: Checkpoint | None = None,
    key: Callable[[Any], Any] | None = None,
    namespace: str | None = None,
    **options: Any,
) -> tuple[Any, ...]:
    """Run *fn* over *iterable* on a :class:`~osiiso.ThreadQueue` and return ordered values.

    See :func:`amap` for arguments and failure behaviour.
    """
    with ThreadQueue(workers=workers, rate=rate) as q:
        grp = q.group(fn, iterable, checkpoint=checkpoint, key=key, namespace=namespace, **options)
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
    checkpoint: Checkpoint | None = None,
    key: Callable[[Any], Any] | None = None,
    namespace: str | None = None,
    **options: Any,
) -> tuple[Any, ...]:
    """Run *fn* over *iterable* on a :class:`~osiiso.ProcessQueue` and return ordered values.

    See :func:`amap` for arguments and failure behaviour; *context*,
    *initializer*, and *initargs* are forwarded to the queue.
    """
    with ProcessQueue(workers=workers, rate=rate, context=context, initializer=initializer, initargs=initargs) as q:
        grp = q.group(fn, iterable, checkpoint=checkpoint, key=key, namespace=namespace, **options)
        q.run(timeout=timeout)
        return grp.values()
