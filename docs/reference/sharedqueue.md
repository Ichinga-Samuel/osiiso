# SharedQueue

`osiiso.SharedQueue` — an [`AsyncQueue`](asyncqueue.md) whose **submission
plane is thread-safe**. Tasks still execute on the single event loop the
queue is bound to (the *shared loop*), but `submit()`, `map()`, and `group()`
may be called from any thread.

```python
from osiiso import SharedQueue
```

---

## How it works

`SharedQueue` inherits everything from [`AsyncQueue`](asyncqueue.md) — the
constructor, lifecycle, policies, callbacks, and introspection are identical.
The only difference: when a submission call arrives from a thread other than
the loop's, it is marshaled onto the loop with `call_soon_threadsafe` and the
caller blocks for one loop round-trip until the task is admitted.

Because admission happens on the loop thread, error semantics are unchanged —
[`ClosedError`](exceptions.md#closederror) and
[`QueueFullError`](exceptions.md#queuefullerror) raise at the call site on
the submitting thread, and the returned [`TaskHandle`](handles.md#taskhandle)
is fully registered before the call returns.

`map()` and `group()` marshal the whole batch in a single round-trip.

---

## Example

```python
import threading
import osiiso

q = osiiso.SharedQueue(workers=4, mode="infinite")

def producer():                      # runs on any thread
    for item in source:
        q.submit(work, item, retries=2)

async def main():
    async with q:                    # binds the shared loop
        threading.Thread(target=producer).start()
        await q.run()                # serve until shutdown()

osiiso.run(main())
```

Producer threads can observe completion with `handle.add_done_callback(fn)`
or by polling `handle.done()`; `await handle` works from any coroutine,
including ones running on other event loops.

---

## Thread-safety contract

| Operation | Safe from any thread? |
|-----------|-----------------------|
| `submit()` / `map()` / `group()` / bound tasks | ✅ once the queue has started and while its loop runs |
| `queue.cancel()` / `handle.cancel()` | ✅ (inherited from `AsyncQueue`) |
| `handle.add_done_callback()` / `handle.done()` / `handle.result()` | ✅ |
| `run()` / `join()` / `shutdown()` / `reset()` | ❌ owning loop only |

!!! note "Hand the queue to threads only after it has started"
    Before the queue is bound to its loop (first `start()` / `__aenter__`),
    submission is single-threaded, exactly like `AsyncQueue`. Start the queue
    on its loop first, then share it with producer threads.

!!! warning "Submitting from another event loop's thread"
    A cross-thread submission blocks the caller for one loop round-trip.
    That is fine for plain threads, but briefly blocks an event loop if you
    call it from one — submit from the owning loop or from regular threads.
