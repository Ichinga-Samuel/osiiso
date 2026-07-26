"""Checkpoint — completion tracking keyed by input, for resumable fan-out.

A :class:`Checkpoint` remembers which *inputs* a fan-out has already completed
successfully, so re-running the same ``map()`` / ``group()`` after a crash
skips the work that already finished:

.. code-block:: python

    from osiiso import Checkpoint, ThreadQueue

    with Checkpoint("run.sqlite") as cp:
        with ThreadQueue(workers=8) as q:
            grp = q.group(fetch, urls, checkpoint=cp, retries=2)
            q.run()
        pages = grp.values()      # still lines up 1:1 with *urls*

This is deliberately **not** a durable task queue.  Nothing about the callable
is persisted — only a key derived from each input and, by default, the value
the task returned.  Tasks that fail or are cancelled are never recorded, so
they run again on the next pass.

Storage is a single SQLite file opened in WAL mode with
``synchronous=NORMAL``: completions survive a process crash (the failure this
is built for) without paying an fsync per task.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterable
from logging import getLogger
from typing import Any, NamedTuple

logger = getLogger("osiiso")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS completions (
    namespace   TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       BLOB,
    encoding    TEXT NOT NULL,
    name        TEXT,
    recorded_at REAL NOT NULL,
    PRIMARY KEY (namespace, key)
) WITHOUT ROWID;
"""

# SQLite caps host parameters per statement; batch lookups well under the limit.
_CHUNK = 500


class Hit(NamedTuple):
    """A recorded completion found in the store.

    Attributes:
        value: The value the task returned, or ``None`` when values are not retained.
        has_value: ``True`` if *value* was actually recorded, ``False`` when the
            checkpoint was created with ``store_values=False``.
    """

    value: Any
    has_value: bool


def canonical_key(entry: Any) -> str:
    """Derive a stable, human-readable key from one fan-out input.

    The input is encoded as canonical JSON (sorted keys, no whitespace), which
    keeps the key identical across runs and processes — unlike :func:`repr`,
    which embeds memory addresses for most objects and would silently never
    match on resume.

    Args:
        entry: One element of the iterable passed to ``map()`` / ``group()``.

    Returns:
        The canonical JSON encoding of *entry*.

    Raises:
        TypeError: If *entry* is not JSON-encodable; pass ``key=`` to derive a
            key yourself in that case.
    """
    try:
        return json.dumps(entry, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"cannot derive a checkpoint key from {type(entry).__name__} — "
            f"pass key=<callable> to map()/group() to derive one yourself ({exc})"
        ) from None


def _default_encode(value: Any) -> str:
    """Encode a task's return value as compact JSON."""
    return json.dumps(value, separators=(",", ":"))


class Checkpoint:
    """A SQLite-backed record of which inputs have already completed.

    Pass one to ``map()`` / ``group()`` (or to :func:`~osiiso.amap` /
    :func:`~osiiso.tmap` / :func:`~osiiso.pmap`) via ``checkpoint=``.  Inputs
    already recorded are not submitted at all; they come back as handles that
    are already ``succeeded``, carrying the stored value, so ordering and
    ``values()`` still line up 1:1 with the input.

    Args:
        path: SQLite file to store completions in.  ``":memory:"`` gives a
            non-persistent store, which is mostly useful in tests.
        store_values: ``True`` (default) records each task's return value so it
            can be handed back on resume; the value must be encodable (JSON by
            default).  ``False`` records only the key, which is the right
            choice for tasks that exist for their side effects — restored
            handles then carry a value of ``None``.
        encoder: Callable turning a return value into ``str``/``bytes``.
            Defaults to compact JSON.
        decoder: Inverse of *encoder*.  Defaults to :func:`json.loads`.

    Note:
        Values are recorded from whichever thread or event loop resolved the
        task.  Access is serialised internally, so one :class:`Checkpoint` is
        safe to share across queues and threads in a single process.

    Example::

        cp = Checkpoint("uploads.sqlite", store_values=False)   # side-effect tasks

        with ThreadQueue(workers=4) as q:
            q.map(upload, paths, checkpoint=cp, retries=3)
            q.run()

        print(cp.count("upload"), "of", len(paths), "done")
        cp.close()
    """

    __slots__ = ("path", "store_values", "_encode", "_decode", "_db", "_lock", "_closed")

    def __init__(self, path: str = ":memory:", *, store_values: bool = True, encoder: Any = None, decoder: Any = None) -> None:
        """Open (creating if needed) the store at *path*; see the class docstring."""
        import sqlite3  # Imported lazily so the package still imports without it.

        self.path = str(path)
        self.store_values = store_values
        self._encode = encoder or _default_encode
        self._decode = decoder or json.loads
        self._lock = threading.Lock()
        self._closed = False
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)

    def __repr__(self) -> str:
        """Return ``Checkpoint('path', entries=N)``, or a closed marker."""
        if self._closed:
            return f"Checkpoint({self.path!r}, closed)"
        return f"Checkpoint({self.path!r}, entries={self.count()})"

    def __enter__(self) -> Checkpoint:
        """Return the store, so ``with Checkpoint(...) as cp:`` reads naturally."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close the store.

        Note:
            Close the *queue* first, so every completion callback has fired —
            ``with Checkpoint(...) as cp, ThreadQueue() as q:`` gets this right,
            since context managers unwind in reverse.
        """
        self.close()

    def is_done(self, namespace: str, key: str) -> bool:
        """``True`` if *key* has already completed under *namespace*."""
        with self._lock:
            self._ensure_open()
            row = self._db.execute(
                "SELECT 1 FROM completions WHERE namespace=? AND key=?", (namespace, key)
            ).fetchone()
        return row is not None

    def lookup(self, namespace: str, keys: Iterable[str]) -> dict[str, Hit]:
        """Fetch every recorded completion among *keys*, in one batched query.

        Args:
            namespace: The namespace to search.
            keys: Candidate keys.

        Returns:
            A mapping of key to :class:`Hit` for the keys that were found.
            Rows whose value cannot be decoded are omitted (and logged), so the
            task runs again rather than yielding a corrupted value.
        """
        keys = list(keys)
        rows: list[tuple[str, Any, str]] = []
        with self._lock:
            self._ensure_open()
            for start in range(0, len(keys), _CHUNK):
                chunk = keys[start : start + _CHUNK]
                holes = ",".join("?" * len(chunk))
                rows += self._db.execute(
                    f"SELECT key, value, encoding FROM completions WHERE namespace=? AND key IN ({holes})",
                    (namespace, *chunk),
                ).fetchall()

        found: dict[str, Hit] = {}
        for key, blob, encoding in rows:  # Decode outside the lock — it can be slow.
            if encoding == "none":
                found[key] = Hit(None, False)
                continue
            try:
                found[key] = Hit(self._decode(blob), True)
            except Exception:
                logger.warning("checkpoint: could not decode stored value for %s/%s — it will run again", namespace, key)
        return found

    def record(self, namespace: str, key: str, value: Any = None, *, name: str | None = None) -> bool:
        """Record *key* as completed under *namespace*.

        Args:
            namespace: The namespace to record under.
            key: The input key that completed.
            value: The task's return value, recorded when ``store_values`` is set.
            name: Task name, stored purely to make the table readable.

        Returns:
            ``True`` if the completion was written, ``False`` if *value* could
            not be encoded — in which case nothing is recorded and the input
            runs again next time.

        Raises:
            RuntimeError: If the store has been closed.
        """
        if self.store_values:
            try:
                blob: Any = self._encode(value)
            except Exception as exc:
                logger.warning(
                    "checkpoint: value for %s/%s is not encodable (%s) — not recorded, so it will run again; "
                    "pass store_values=False or a custom encoder=",
                    namespace,
                    key,
                    exc,
                )
                return False
            encoding = "json"
        else:
            blob, encoding = None, "none"

        with self._lock:
            self._ensure_open()
            self._db.execute(
                "INSERT OR REPLACE INTO completions (namespace, key, value, encoding, name, recorded_at) VALUES (?,?,?,?,?,?)",
                # Wall clock, not perf_counter: this record has to outlive the process that wrote it.
                (namespace, key, blob, encoding, name, time.time()),
            )
        return True

    def count(self, namespace: str | None = None) -> int:
        """Number of recorded completions, in *namespace* or across all of them."""
        with self._lock:
            self._ensure_open()
            if namespace is None:
                row = self._db.execute("SELECT COUNT(*) FROM completions").fetchone()
            else:
                row = self._db.execute("SELECT COUNT(*) FROM completions WHERE namespace=?", (namespace,)).fetchone()
        return int(row[0])

    def namespaces(self) -> tuple[str, ...]:
        """Every namespace present in the store, sorted."""
        with self._lock:
            self._ensure_open()
            rows = self._db.execute("SELECT DISTINCT namespace FROM completions ORDER BY namespace").fetchall()
        return tuple(r[0] for r in rows)

    def clear(self, namespace: str | None = None) -> int:
        """Forget completions so they run again.

        Args:
            namespace: Clear only this namespace, or every one when ``None``.

        Returns:
            How many records were removed.
        """
        with self._lock:
            self._ensure_open()
            if namespace is None:
                cur = self._db.execute("DELETE FROM completions")
            else:
                cur = self._db.execute("DELETE FROM completions WHERE namespace=?", (namespace,))
        return cur.rowcount

    def close(self) -> None:
        """Close the underlying database (idempotent)."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._db.close()

    def _ensure_open(self) -> None:
        """Guard against use after :meth:`close` (call with the lock held).

        Raises:
            RuntimeError: If the store is closed.
        """
        if self._closed:
            raise RuntimeError("checkpoint is closed — close it after the queue has finished, not before")

    def _watch(self, handle: Any, namespace: str, key: str) -> None:
        """Record *key* once *handle* succeeds; failures and cancellations are left unrecorded."""

        def _on_done(h: Any) -> None:
            """Completion callback: persist the key only on success."""
            if h.status != "succeeded":
                return
            self.record(namespace, key, h.result().value, name=h.name)

        handle.add_done_callback(_on_done)
