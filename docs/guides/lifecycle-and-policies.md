# Lifecycle and Policies

Most applications should use context managers:

```python
async with osiiso.AsyncQueue(workers=4) as q:
    q.submit(work, 1)
    summary = await q.run()
```

```python
with osiiso.ThreadQueue(workers=4) as q:
    q.submit(work, 1)
    summary = q.run()
```

## Manual lifecycle

```python
q = osiiso.AsyncQueue(workers=4)
await q.start()
q.submit(work, 1)
summary = await q.run()
await q.shutdown()
```

Use `shutdown(force=True)` to cancel pending and active work immediately.

## Reset and reuse

After a run, call `reset()` when you want to reuse the same queue object:

```python
q.submit(fetch, "first")
await q.run()

q.reset()
q.submit(fetch, "second")
await q.run()
```

Use `clear_results()` to free stored result history without reopening or
reinitializing the queue.

## Finite and infinite modes

`mode="finite"` drains the submitted work and stops. This is the default.

`mode="infinite"` runs until shutdown or timeout. It is useful for producers
that keep adding work while workers are active.

```python
q = osiiso.AsyncQueue(mode="infinite", timeout=60)
```

## Fail policies

`fail_policy="continue"` records failures and keeps running remaining tasks.

```python
q = osiiso.ThreadQueue(fail_policy="continue")
```

`fail_policy="fail_first"` cancels remaining work after the first failure.

```python
summary = q.run(fail_policy="fail_first")
```

## Shutdown behavior

`on_exit="complete_priority"` cancels ordinary pending work on timeout but lets
`must_complete=True` tasks finish.

`on_exit="cancel"` cancels everything it can.

```python
q.submit(save_checkpoint, data, must_complete=True, priority=0)
summary = q.run(timeout=5)
```

## Hooks

Hooks are synchronous callbacks. Exceptions raised inside hooks are logged and
do not crash the queue.

```python
q = osiiso.AsyncQueue(
    on_start=lambda handle: print("start", handle.name),
    on_complete=lambda result: print("done", result.name, result.status),
    on_retry=lambda handle, exc: print("retry", handle.name, exc),
)
```
