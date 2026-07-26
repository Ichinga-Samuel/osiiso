# Resumable Runs

Long fan-outs fail partway. You are 30,000 URLs into a 50,000-URL scrape when
the process dies, and the expensive question is not "what was queued?" but
"which inputs am I already done with?"

[`Checkpoint`](../reference/checkpoint.md) answers exactly that, and nothing
more.

## The problem it solves

```python
from osiiso import Checkpoint, ThreadQueue

with Checkpoint("scrape.sqlite") as cp, ThreadQueue(workers=8, rate=5) as q:
    grp = q.group(fetch, urls, checkpoint=cp, retries=3)
    q.run()

pages = grp.values()
```

Run this once and it fetches everything. Kill it halfway and run it again: the
URLs that already succeeded are **not submitted at all**, and `pages` still
contains all 50,000 values in input order — the finished ones read back from
the store, the rest freshly fetched.

## What is actually stored

For each input that succeeded:

- a **key** derived from the input (its canonical JSON, by default)
- the **value** the task returned (unless you set `store_values=False`)
- a namespace, the task name, and a wall-clock timestamp

That is the whole schema. The callable is never persisted, so closures,
lambdas, and bound methods all work — the store simply doesn't care what ran.

!!! note "Not a durable queue"
    This is completion tracking, not durability. Nothing recovers work that was
    *queued but never started* — those inputs simply run again on the next
    pass, which is exactly what you want for a retryable fan-out. If you need
    tasks to survive independently of the calling process, use a real broker
    (Celery, dramatiq, RQ).

## Only successes are recorded

Failures and cancellations leave no record, so they retry next run:

```python
with Checkpoint("run.sqlite") as cp, ThreadQueue(workers=4) as q:
    q.map(flaky, items, checkpoint=cp, retries=2)
    summary = q.run()

print(summary.failed, "failed — they will run again next time")
```

This is why `retries=` and `checkpoint=` compose cleanly: retries handle
transient failure *within* a run, the checkpoint handles it *across* runs.

## Idempotency is still your job

A checkpoint records a completion **after** the task returns. If the process
dies between the side effect and the write, that input runs again — this is
at-least-once, not exactly-once. For tasks that charge cards or send email,
make the operation idempotent (an idempotency key, an upsert) or accept the
duplicate.

## Choosing keys

The default key is the input's canonical JSON, which is right when inputs are
URLs, ids, or small dicts. Pass `key=` when they are not:

```python
q.map(process, records, checkpoint=cp, key=lambda r: r["id"])
```

If an input is not JSON-encodable and you did not pass `key=`, submission
raises `TypeError` rather than silently producing a key that never matches.

## Namespaces

Each fan-out gets a namespace inside the store, defaulting to the callable's
name. One file can track many different operations:

```python
with Checkpoint("pipeline.sqlite") as cp, ThreadQueue(workers=8) as q:
    q.map(download, urls, checkpoint=cp)      # namespace "download"
    q.run()
    q.reset()
    q.map(parse, urls, checkpoint=cp)         # namespace "parse" — same urls, separate tracking
    q.run()
```

Set `namespace=` explicitly when the callable is renamed, when you want to
force a re-run under a new name (`namespace="fetch-v2"`), or for
heterogeneous groups, where it is required:

```python
q.group([(extract, "db"), (transform, raw), (load, dest)],
        checkpoint=cp, namespace="etl-nightly")
```

## Side-effecting tasks

When the return value is irrelevant, skip storing it:

```python
cp = Checkpoint("uploads.sqlite", store_values=False)

with ThreadQueue(workers=4) as q:
    q.map(upload, paths, checkpoint=cp, retries=3)
    q.run()
```

Restored handles then carry `None`, and their result message says
`"restored from checkpoint (value not retained)"`. Don't call `values()` on a
resumed run in this mode and expect data back.

## Reading the summary

Restored tasks never enter the queue, so the
[`RunSummary`](../reference/runsummary.md) describes only the work this run
actually performed:

```python
q.map(process, items, checkpoint=cp)
summary = q.run()
print(f"{summary.total_submitted} of {len(items)} still needed doing")
```

Individual restored results are identifiable by `attempts == 0`.

## Shortcuts

The one-shot helpers take the same arguments, which makes a resumable fan-out
a one-liner:

```python
pages = tmap(fetch, urls, workers=8, retries=2, checkpoint=cp)
```

## Inspecting and resetting

The store is a plain SQLite file — readable with any SQLite tool:

```python
cp.count()             # completions across all namespaces
cp.count("fetch")      # just this one
cp.namespaces()        # every namespace present
cp.clear("fetch")      # forget one namespace so it runs again
cp.clear()             # forget everything
```

## Ordering the context managers

Close the queue **before** the checkpoint, so every completion callback has
fired. Listing them in this order gets it right, since context managers unwind
in reverse:

```python
with Checkpoint("run.sqlite") as cp, ThreadQueue(workers=8) as q:
    ...
```

Using a closed store raises `RuntimeError`.
