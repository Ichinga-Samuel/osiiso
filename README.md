# osiiso

Structured task queues for Python across `asyncio`, threads, and processes.

<p>
  <a href="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/action.yml"><img alt="CI" src="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/action.yml/badge.svg"></a>
  <a href="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/docs.yml"><img alt="Docs" src="https://github.com/Ichinga-Samuel/osiiso/actions/workflows/docs.yml/badge.svg"></a>
  <img alt="Python 3.13+" src="https://img.shields.io/badge/python-3.13%2B-3776AB?logo=python&logoColor=white">
  <img alt="Typed package" src="https://img.shields.io/badge/typed-py.typed-blue">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green.svg"></a>
</p>

`osiiso` gives you one compact queue API for three execution backends:

- `AsyncQueue` for coroutine-heavy I/O and async integrations (`SharedQueue` adds thread-safe submission onto one shared event loop).
- `ThreadQueue` for blocking I/O, synchronous SDKs, filesystem work, and SQLite writes.
- `ProcessQueue` for CPU-heavy work that benefits from separate subprocesses.

It is dependency-free at runtime, typed with `py.typed`, and built around a predictable workflow: submit tasks, apply options, run the queue, then inspect handles and a `RunSummary`.

## Contents

- [Why osiiso](#why-osiiso)
- [Installation](#installation)
- [Choose a queue](#choose-a-queue)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [Task options](#task-options)
- [Results and errors](#results-and-errors)
- [Examples](#examples)
- [Documentation](#documentation)
- [Development](#development)
- [Community](#community)

## Why osiiso

- Shared API across async, thread, and process execution.
- Priority scheduling where lower priority numbers run first.
- Retries with optional delay and exponential backoff.
- Per-task timeouts and queue-level run timeouts.
- Built-in rate limiting (`rate=` / `burst=`) across all backends.
- Delayed and absolute-time scheduling that never blocks a worker.
- A persistent process pool — subprocesses are reused across tasks, with crash detection and automatic respawn.
- Graceful shutdown that drains outstanding work, with `must_complete` protection.
- Batch workflows with `submit()`, `map()`, and `group()`.
- Resumable fan-out via `Checkpoint` — a killed run picks up where it left off.
- Awaitable async handles and blocking sync handles, with `add_done_callback()`.
- Completion-order iteration via `as_completed()` (async) and `iter_completed()` (sync).
- One-shot helpers: `amap()`, `tmap()`, and `pmap()` return ordered values.
- Structured `RunSummary` and immutable `TaskResult` records with `metadata` passthrough.
- Lifecycle hooks for `on_start`, `on_complete`, and `on_retry`.
- Worker `initializer` support for threads and subprocesses.
- Optional `uvloop` integration through `osiiso.run()`.

## Installation

```bash
pip install osiiso
```

With optional `uvloop` support:

```bash
pip install "osiiso[uvloop]"
```

The project targets Python 3.13 and newer.

## Choose a queue

| Workload | Queue | Good for |
| --- | --- | --- |
| Coroutine-based I/O | `AsyncQueue` | HTTP clients, async databases, websockets, API fan-out |
| Coroutine I/O fed from threads | `SharedQueue` | One event loop serving producers on other threads |
| Blocking synchronous work | `ThreadQueue` | File operations, blocking SDKs, SQLite writes, sync integrations |
| CPU-heavy functions | `ProcessQueue` | Ranking, parsing, scoring, transformations, analytics |

The queues intentionally look similar, so work can move between execution models with minimal changes.

## Quick start

```python
import asyncio
import osiiso


async def fetch(name: str) -> str:
    await asyncio.sleep(0.1)
    return f"fetched {name}"


async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(fetch, "users", priority=0)
        q.submit(fetch, "posts", retries=2, retry_delay=0.25, timeout=5)

        summary = await q.run(strict=True)
        return summary.values


print(osiiso.run(main()))
```

## Core concepts

### `submit()`

Use `submit()` for one task. It returns a handle immediately.

```python
handle = q.submit(fetch_user, "ada", retries=3, timeout=10, name="fetch-user")
```

Async handles are awaitable:

```python
result = await handle
value = handle.value()
```

Thread and process handles are blocking:

```python
result = handle.wait(timeout=5)
value = handle.value()
```

### `map()`

Use `map()` for one callable over many inputs.

```python
q.map(download, urls, retries=2, group_id="downloads")
q.map(add, [(1, 2), (3, 4), (5, 6)], name="add")
q.map(request, [{"method": "GET", "url": "https://example.com"}])
```

Tuple entries are unpacked as positional arguments. Mapping entries are passed as keyword arguments.

### `group()`

Use `group()` for a named batch, especially when tasks have different callables.

```python
group = q.group(
    [
        (extract, "db"),
        (transform, raw_records),
        (load, destination),
    ],
    group_id="etl-batch-1",
)

summary = q.run()
values = group.values()
```

For `AsyncQueue`, use `await group.wait()` and `await group.values()`.
`values()` raises `ExecutionError` if any task failed or was cancelled, so a
returned tuple always lines up 1:1 with the inputs.  Iterate a group in
completion order with `group.as_completed()`.

### One-shot helpers

When all you need is "run this function over these inputs", skip the queue
ceremony.  Each helper builds a queue, runs it, and returns values in input
order, raising `ExecutionError` on any failure:

```python
pages = await osiiso.amap(fetch, urls, workers=8, retries=2)   # AsyncQueue
sizes = osiiso.tmap(stat_file, paths, workers=8)               # ThreadQueue
scores = osiiso.pmap(rank, datasets, workers=4)                # ProcessQueue
```

### Resumable runs

Long fan-outs fail partway.  Pass a `Checkpoint` and the inputs that already
succeeded are not submitted again — their stored values are handed straight
back, so results still line up 1:1 with the input:

```python
from osiiso import Checkpoint, ThreadQueue

with Checkpoint("scrape.sqlite") as cp, ThreadQueue(workers=8, rate=5) as q:
    grp = q.group(fetch, urls, checkpoint=cp, retries=3)
    q.run()

pages = grp.values()
```

Kill it at 30k of 50k URLs and run it again: only the missing 20k are fetched.
Only successes are recorded, so failed and cancelled tasks retry next run.

This is completion tracking keyed by input, **not** a durable task queue — the
callable is never persisted, and nothing recovers work that was queued but
never started.  See [Resumable Runs](docs/guides/resumable-runs.md).

### Rate limiting

Every queue accepts `rate` (maximum task attempts per second) and `burst`
(how many attempts may start back-to-back after an idle period):

```python
async with osiiso.AsyncQueue(workers=8, rate=10, burst=3) as q:
    q.map(call_api, payloads, retries=2)
    await q.run()
```

### Bound tasks

Bind a callable to a queue with `@q.task()`.

```python
async with osiiso.AsyncQueue(workers=4) as q:
    @q.task(retries=2, retry_delay=0.25, name="fetch")
    async def fetch(url: str) -> str:
        return await client.get(url)

    fetch("https://example.com")
    fetch.map(["https://example.org", "https://example.net"])

    summary = await q.run(strict=True)
```

## Queue examples

### AsyncQueue

```python
import asyncio
import osiiso


async def fetch(name: str) -> str:
    await asyncio.sleep(0.1)
    return f"fetched {name}"


async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.map(fetch, ["users", "posts", "comments"], retries=2, timeout=5)
        summary = await q.run(strict=True)
        print(summary.values)


osiiso.run(main())
```

### ThreadQueue

```python
import time
import osiiso


def resize(path: str) -> str:
    time.sleep(0.1)
    return f"resized {path}"


with osiiso.ThreadQueue(workers=4) as q:
    q.map(resize, ["a.png", "b.png", "c.png"], name="resize")
    summary = q.run(strict=True)

print(summary.values)
```

### ProcessQueue

Each worker owns a **persistent subprocess** that is reused across tasks, so
spawn cost is paid once per worker instead of once per task.  Timeouts and
cancellation terminate the subprocess (it is respawned for the next task),
and a crashed worker is reported as that task's failure while the pool
recovers automatically.

Tasks and arguments must be pickleable, and standard spawn rules apply:
guard your script's entry point with `if __name__ == "__main__":`.

```python
import osiiso


def score(n: int) -> int:
    return sum(i * i for i in range(n))


if __name__ == "__main__":
    with osiiso.ProcessQueue(workers=4) as q:
        q.map(score, [10_000, 20_000, 30_000], name="score")
        summary = q.run(strict=True)

    print(summary.values)
```

## Task options

Task behavior can be configured inline or through an immutable `TaskOptions` object.

```python
from osiiso import TaskOptions


retrying = TaskOptions(retries=3, retry_delay=0.5, backoff=2, timeout=10)
urgent = retrying.replace(priority=0, name="urgent-api-call")

q.submit(fetch, url, opts=urgent)
q.submit(fetch, other_url, retries=3, retry_delay=0.5, backoff=2)
```

| Option | Default | Meaning |
| --- | --- | --- |
| `priority` | `3` | Lower numbers run first. |
| `must_complete` | `False` | Protects a task during graceful shutdown. |
| `timeout` | `None` | Per-task timeout in seconds. |
| `retries` | `0` | Retry attempts after the first failure. |
| `retry_delay` | `0.0` | Delay before the first retry. |
| `backoff` | `1.0` | Multiplier applied after each retry. |
| `delay` | `None` | Run after this many seconds. |
| `run_at` | `None` | Run at an absolute epoch timestamp. |
| `name` | `None` | Custom result and hook name. |
| `group_id` | `None` | Group label for summaries. |
| `detached` | `False` | Task still runs, but its result is excluded from the `RunSummary`; observe it via its handle. |
| `metadata` | `None` | Arbitrary user data carried onto the handle and the `TaskResult`. |

`TaskOptions` validates invalid combinations immediately. For example, `delay` and `run_at` are mutually exclusive, negative retries are rejected, and unknown submit options raise `TypeError`.

## Results and errors

Every `run()` returns a `RunSummary`.

```python
summary.ok
summary.succeeded
summary.failed
summary.cancelled
summary.timed_out
summary.values
summary.errors
summary.by_task_id()
summary.by_name()
summary.by_group()
summary.raise_for_errors()
summary.display()
```

Use `strict=True` when failures should raise `ExecutionError` after the run finishes:

```python
summary = await q.run(strict=True)
```

Each task result is stored as a `TaskResult` with task id, name, status, value, exception, attempts, priority, timing, group id, and cancellation metadata.

## Lifecycle and policies

Queues support finite and long-running modes:

```python
q = osiiso.AsyncQueue(mode="finite", fail_policy="continue", on_timeout="complete")
```

- `mode="finite"` runs pending work and exits.
- `mode="infinite"` keeps workers alive until shutdown or timeout.
- `fail_policy="continue"` records failures and keeps processing.
- `fail_policy="fail_first"` cancels remaining work after the first failure (`must_complete` tasks are spared).
- `on_timeout="complete"` lets `must_complete` tasks finish when a run times out.
- `on_timeout="cancel"` cancels everything when a run times out.

Leaving the context manager (or calling `shutdown()`) drains all outstanding
work — including scheduled tasks — before stopping workers.  Use
`shutdown(force=True)` (or raise inside the `with` block) to cancel
everything immediately instead.

Bounded queues (`size=N`) cap outstanding tasks: sync queues block `submit()`
until space frees (natural backpressure), while `AsyncQueue.submit()` raises
`QueueFullError`.

Hooks give you a simple integration point for logging, metrics, and tracing:

```python
def completed(result: osiiso.TaskResult) -> None:
    print(result.name, result.status, result.duration)


q = osiiso.ThreadQueue(on_complete=completed)
```

## Examples

Run the compact feature gallery:

```bash
uv run python examples/feature_gallery.py
```

Run the complete Hacker News style showcase:

```bash
uv run python -m examples.hackernews_showcase --limit 6
```

Use the live Hacker News API:

```bash
uv run python -m examples.hackernews_showcase --limit 20 --online
```

The showcase uses all three backends:

- `AsyncQueue` fetches feeds, items, and users.
- `ThreadQueue` persists records into SQLite.
- `ProcessQueue` ranks stories and computes keywords.

## Documentation

Full documentation is available at **[ichinga-samuel.github.io/osiiso](https://ichinga-samuel.github.io/osiiso/)**.

Run the docs locally:

```bash
python -m pip install -e ".[docs]"
mkdocs serve
```

Build the docs strictly:

```bash
mkdocs build --strict
```

## Development

Install the development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Run tests:

```bash
uv run pytest
```

Run Ruff:

```bash
uv run ruff check .
```

Build the package:

```bash
python -m build
```

Build the docs with the docs extra:

```bash
uv run --extra docs mkdocs build --strict
```

## Community

- Read the [contribution guide](CONTRIBUTING.md) before opening larger pull requests.
- Check the [changelog](CHANGELOG.md) for release history and upcoming changes.
- Use [support guidance](SUPPORT.md) for questions, bug reports, and feature requests.
- Report vulnerabilities through the [security policy](SECURITY.md), not public issues.
- Follow the [code of conduct](CODE_OF_CONDUCT.md) when participating in project spaces.

## Project layout

```text
.
|-- src/osiiso/                  # Library source
|-- tests/                       # Unit tests for async, thread, process, and options behavior
|-- docs/                        # MkDocs documentation
|-- examples/feature_gallery.py  # Compact API showcase
|-- examples/hackernews_showcase # Complete multi-backend example project
|-- pyproject.toml               # Package metadata and tool configuration
`-- mkdocs.yml                   # Documentation site configuration
```

## Public API

```python
from osiiso import (
    AsyncQueue,
    ThreadQueue,
    ProcessQueue,
    TaskOptions,
    TaskHandle,
    SyncTaskHandle,
    TaskGroup,
    SyncTaskGroup,
    TaskResult,
    RunSummary,
    ExecutionError,
    ClosedError,
    QueueFullError,
    OsiisoError,
    run,
    amap,
    tmap,
    pmap,
    as_completed,
    iter_completed,
)
```

## License

`osiiso` is released under the [MIT License](LICENSE).
