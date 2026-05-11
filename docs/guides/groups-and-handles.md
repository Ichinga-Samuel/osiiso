# Groups and Handles

Every `submit()` call returns a handle. Groups collect multiple handles under a
shared group id.

## Async handles

`AsyncQueue.submit()` returns `TaskHandle`, which is awaitable:

```python
async with osiiso.AsyncQueue(workers=2) as q:
    handle = q.submit(fetch, "https://example.com")
    result = await handle

print(result.status)
print(handle.value())
```

Useful methods and properties:

```python
await handle.wait()
handle.result()
handle.value()
handle.cancel()
handle.done()
handle.status
handle.attempts
handle.task_id
handle.name
```

## Sync handles

`ThreadQueue` and `ProcessQueue` return `SyncTaskHandle`:

```python
with osiiso.ThreadQueue(workers=2) as q:
    handle = q.submit(write_file, path)
    result = handle.wait(timeout=5)
```

## Groups

Use `group()` for a batch that should be named and waited on together:

```python
group = q.group(
    [
        (fetch_user, "ada"),
        (fetch_user, "grace"),
        (fetch_user, "linus"),
    ],
    group_id="users",
)

summary = await q.run()
user_values = await group.values()
```

Sync queues use blocking group methods:

```python
summary = group.wait(timeout=30)
values = group.values()
cancelled = group.cancel()
```

## Summaries by group

`RunSummary.by_group()` makes grouped reporting straightforward:

```python
by_group = summary.by_group()
print(by_group["users"])
```

`RunSummary.by_name()` and `RunSummary.by_task_id()` are available when you need
different lookup shapes.

## Streaming async completions

For async queues, process results as soon as handles complete:

```python
q = osiiso.AsyncQueue(workers=4)
handles = q.map(fetch, urls)
await q.start()
try:
    async for handle in osiiso.AsyncQueue.as_completed(handles):
        print(handle.value())
finally:
    await q.shutdown()
```
