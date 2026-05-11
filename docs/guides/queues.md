# Queue Guide

The queues are intentionally similar. After you know one, you can move work
between async, thread, and process execution with minimal API changes.

## Shared constructor options

```python
queue = osiiso.AsyncQueue(
    workers=4,
    size=0,
    timeout=None,
    mode="finite",
    fail_policy="continue",
    on_exit="complete_priority",
    on_start=None,
    on_complete=None,
    on_retry=None,
)
```

`ThreadQueue` and `ProcessQueue` also accept `poll`, which controls how often
the queue checks for cancellation and timeout while sync work is running.
`ProcessQueue` also accepts a multiprocessing `context`.

## AsyncQueue

`AsyncQueue` runs coroutine functions directly. If you submit a normal sync
function, it is executed with `asyncio.to_thread()`.

```python
async with osiiso.AsyncQueue(workers=8) as q:
    q.submit(fetch_user, "ada", retries=3, timeout=5)
    q.submit(fetch_user, "grace", priority=0)
    summary = await q.run()
```

Use `osiiso.run()` to run your top-level coroutine. It uses `uvloop` when
available unless you force stdlib asyncio.

```python
result = osiiso.run(main(), use_uvloop=False)
```

## ThreadQueue

`ThreadQueue` is for regular blocking functions:

```python
with osiiso.ThreadQueue(workers=4) as q:
    q.submit(write_row, row, must_complete=True)
    q.map(read_file, ["a.txt", "b.txt"])
    summary = q.run()
```

It is a good fit for SDKs, filesystem work, SQLite writes, and code that blocks
but does not need process-level parallelism.

## ProcessQueue

`ProcessQueue` runs work in subprocesses:

```python
def parse_document(path: str) -> dict[str, int]:
    ...

if __name__ == "__main__":
    with osiiso.ProcessQueue(workers=4) as q:
        q.map(parse_document, paths, timeout=30)
        summary = q.run()
```

Keep process functions importable and pickleable. Prefer top-level functions and
plain data arguments.

## `submit()`, `map()`, and `group()`

Use `submit()` for one task:

```python
handle = q.submit(fn, arg1, arg2, priority=1)
```

Use `map()` for one callable over many inputs:

```python
q.map(download, urls)
q.map(add, [(1, 2), (3, 4)])
q.map(request, [{"method": "GET", "url": "https://example.com"}])
```

Use `group()` for a named batch, especially when entries have different
callables:

```python
group = q.group(
    [
        (extract, "db"),
        (transform, raw_data),
        (load, record),
    ],
    group_id="etl-batch-1",
)
```

The current tests and docs use this heterogeneous `group([(fn, *args), ...])`
style.
