# Checkpoint

`osiiso.Checkpoint` — completion tracking keyed by input, so a fan-out that
died halfway can be re-run without redoing the work that already finished.

```python
from osiiso import Checkpoint
```

Pass one to [`map()`](asyncqueue.md#task-submission), [`group()`](groups.md),
or any [shortcut](shortcuts.md) via `checkpoint=`. Inputs the store has
already recorded are **not submitted at all** — they come back as handles that
are already `succeeded`, carrying the stored value, so ordering and
`values()` still line up 1:1 with the input.

!!! warning "This is not a durable task queue"
    Nothing about the *callable* is persisted — only a key derived from each
    input and, by default, the value the task returned. `Checkpoint` answers
    "which inputs am I done with?", not "what work was outstanding?". For
    durable, broker-backed queues, reach for Celery, dramatiq, or RQ.

---

## Constructor

```python
Checkpoint(
    path: str = ":memory:",
    *,
    store_values: bool = True,
    encoder: Callable[[Any], str | bytes] | None = None,
    decoder: Callable[[Any], Any] | None = None,
)
```

| Parameter | Description |
|-----------|-------------|
| `path` | SQLite file to store completions in. `":memory:"` gives a non-persistent store, mostly useful in tests |
| `store_values` | `True` (default) records each return value so it can be handed back on resume. `False` records only the key — the right choice for side-effecting tasks; restored handles then carry `None` |
| `encoder` | Turns a return value into `str`/`bytes`. Defaults to compact JSON |
| `decoder` | Inverse of `encoder`. Defaults to `json.loads` |

Storage is a single SQLite file in WAL mode with `synchronous=NORMAL`:
completions survive a process crash — the failure this is built for — without
paying an fsync per task.

---

## Submission parameters

`map()`, `group()`, `amap()`, `tmap()`, and `pmap()` all accept the same three:

| Parameter | Description |
|-----------|-------------|
| `checkpoint` | The `Checkpoint` to resume against |
| `key` | Derives the checkpoint key from an element. Defaults to the element's canonical JSON — pass one when elements are not JSON-encodable, or are large |
| `namespace` | Isolates this fan-out inside the store. Defaults to the name of the callable |

```python
q.map(fetch, urls, checkpoint=cp)                          # namespace "fetch"
q.map(fetch, rows, checkpoint=cp, key=lambda r: r["id"])   # key on a field
q.map(fetch, urls, checkpoint=cp, namespace="fetch-v2")    # force a re-run
```

### Keys

The default key is the element's **canonical JSON** (sorted keys, no
whitespace), which stays identical across runs and processes. `repr()` is not
used, because it embeds memory addresses for most objects and would silently
never match on resume.

If an element is not JSON-encodable, submission raises `TypeError` telling you
to pass `key=`. That is deliberate: failing loudly beats a checkpoint that
quietly never resumes.

### Namespaces

For `group([(fn, *args), ...])` — the heterogeneous form — the default key is
the callable's *name* plus its arguments, and `namespace=` is **required**:

```python
q.group([(extract, "db"), (load, dest)], checkpoint=cp, namespace="etl")
```

A generated `group_id` changes every run and would never match on resume, so
omitting `namespace=` (without an explicit `group_id=`) raises `ValueError`.

---

## What gets recorded

Only **successes**. Tasks that fail, are cancelled, or never run leave no
record, so the next pass retries them — which is the entire point.

A restored result is identifiable:

```python
result = handle.result()
result.status      # "succeeded"
result.attempts    # 0 — it never executed this run
result.message     # "restored from checkpoint"
```

Restored tasks are also absent from the [`RunSummary`](runsummary.md), which
therefore reflects the work actually performed this run:

```python
q.map(process, items, checkpoint=cp)
summary = q.run()
print(f"{summary.total_submitted} of {len(items)} still needed doing")
```

---

## Methods

| Method | Description |
|--------|-------------|
| `is_done(namespace, key)` | `True` if `key` has completed under `namespace` |
| `lookup(namespace, keys)` | Batched fetch; returns `{key: Hit}` for the keys found |
| `record(namespace, key, value=None, *, name=None)` | Record a completion manually. Returns `False` (and logs) if `value` could not be encoded |
| `count(namespace=None)` | Number of recorded completions, in one namespace or all |
| `namespaces()` | Every namespace present, sorted |
| `clear(namespace=None)` | Forget completions so they run again; returns how many were removed |
| `close()` | Close the database (idempotent) |

`lookup()` returns `Hit` records — a `NamedTuple` of `(value, has_value)`, where
`has_value` is `False` when the store was created with `store_values=False`.
You never construct one, so it is not exported at the top level; import it from
`osiiso.checkpoint` if you need the type for an annotation.

`Checkpoint` is a context manager. Close the **queue first**, so every
completion callback has fired:

```python
with Checkpoint("run.sqlite") as cp, ThreadQueue(workers=8) as q:   # unwinds q, then cp
    q.map(fetch, urls, checkpoint=cp)
    q.run()
```

Using a closed store raises `RuntimeError`.

---

## Thread safety

Values are recorded from whichever thread or event loop resolved the task.
Access is serialised internally, so one `Checkpoint` is safe to share across
queues and threads within a single process. It is **not** designed for
concurrent use from multiple processes.

---

## Examples

### Resume a large fan-out

```python
from osiiso import Checkpoint, ThreadQueue

with Checkpoint("scrape.sqlite") as cp, ThreadQueue(workers=8, rate=5) as q:
    grp = q.group(fetch, urls, checkpoint=cp, retries=3)
    q.run()

pages = grp.values()   # every url, restored or freshly fetched
```

Kill it at 30k of 50k, run it again, and only the remaining 20k are fetched.

### Side-effecting tasks

```python
cp = Checkpoint("uploads.sqlite", store_values=False)

with ThreadQueue(workers=4) as q:
    q.map(upload, paths, checkpoint=cp, retries=3)
    q.run()

print(cp.count("upload"), "of", len(paths), "uploaded")
cp.close()
```

### Force a re-run

```python
cp.clear("fetch")            # one namespace
cp.clear()                   # everything
```

---

## See also

- [Resumable Runs](../guides/resumable-runs.md) — the guide
- [Shortcuts](shortcuts.md) — `amap` / `tmap` / `pmap` take `checkpoint=` too
- [RunSummary](runsummary.md) — what a partially-restored run reports
