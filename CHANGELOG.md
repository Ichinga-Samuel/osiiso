# Changelog

All notable changes to this project are documented in this file.

This project follows semantic versioning where practical:

- **Major** versions may include breaking API changes.
- **Minor** versions add backward-compatible features.
- **Patch** versions fix bugs, tighten documentation, or improve internals.

---

## [Unreleased]

## [1.1.0] - 2026-07-26

### Added

- **`Checkpoint`** — completion tracking keyed by input, for resumable fan-out. `map()`, `group()`, `amap()`, `tmap()`, and `pmap()` accept `checkpoint=`, `key=`, and `namespace=`; inputs already recorded are not submitted, returning finished handles that carry the stored value, so results still line up 1:1 with the input. Backed by a single SQLite file in WAL mode, so completions survive a hard process kill. Only successes are recorded, so failed and cancelled tasks run again on the next pass. This is completion tracking, not a durable task queue: the callable is never persisted.
- `Checkpoint` is exported from the package root; the `Hit` record it returns stays in `osiiso.checkpoint` to keep the top-level namespace clean.
- Resumable fetches in the Hacker News showcase, a checkpoint section in the feature gallery, and `--checkpoint` / `--no-checkpoint` / `--reset-checkpoint` flags on the showcase CLI.
- Documentation: a "Resumable Runs" guide and a `Checkpoint` API reference page.
- Public README refresh with banner artwork, badges, queue examples, development workflow, and API overview.
- Community documentation for contributing, security reporting, support, and project conduct.
- GitHub issue templates and pull request template.

---

### Changed

- Completed the Google-style docstrings across the package — `Args`, `Returns`, and `Raises` sections are now filled in on public methods, module-level helpers, and dunder methods. No behaviour changes.

### Compatibility

- Backward compatible with 1.0.x. `checkpoint`, `key`, and `namespace` are keyword-only and default to `None`; the code paths taken without them are unchanged. In 1.0.x those names raised `TypeError: Unknown task option(s): ...`, so no working call can break. `sqlite3` is imported lazily inside `Checkpoint`, so `import osiiso` still succeeds without it.

## [1.0.1] - 2026-07-13

Released to PyPI but not previously recorded here.

### Fixed

- Repaired mojibake in four error messages, where a UTF-8 em dash had been written as `â€”` (`ThreadQueue._validate_fn`, `AsyncQueue.reset`, `AsyncQueue._enqueue`, and the sync queue's `reset`).

## [1.0.0] — 2026-07-11

Core rewrite around a shared engine. Breaking release.

### Added

- **Rate limiting** on every queue: `rate=` (attempts/second) and `burst=`, implemented as a thread-safe GCRA token bucket.
- **Persistent process pool**: `ProcessQueue` workers now keep a subprocess alive and ship tasks over a pipe instead of spawning one process per task. Worker crashes are reported as task failures and the pool respawns automatically.
- `initializer` / `initargs` on `ThreadQueue` (runs in each worker thread) and `ProcessQueue` (runs inside each subprocess).
- `handle.add_done_callback(fn)` on both handle flavours.
- Completion-order iteration: `osiiso.as_completed(handles)` (async) and `osiiso.iter_completed(handles, timeout=None)` (blocking), plus `group.as_completed()` on both group types.
- One-shot helpers `osiiso.amap()`, `osiiso.tmap()`, and `osiiso.pmap()` — build a queue, run it, and return values in input order (raising `ExecutionError` on failure or cancellation).
- `TaskOptions.metadata` — arbitrary user data carried onto the handle and the `TaskResult`.
- `QueueFullError`, raised by `AsyncQueue.submit()` when a bounded queue is full; sync queues now block on `submit()` for natural backpressure (previously an internal `queue.Full` escaped).
- `stats` now reports a `scheduled` count.
- `RunSummary.raise_for_errors(include_cancelled=True)` option; `group.values()` uses it so returned tuples always align with inputs.

### Changed (breaking)

- `on_exit=` is now `on_timeout=` with values `"complete"` (was `"complete_priority"`) and `"cancel"`; it applies only to run timeouts.
- The `poll=` parameter is gone: thread and process execution are now fully event-driven (completion events and `multiprocessing.connection.wait`), so cancellation and timeouts take effect immediately instead of on a 50 ms poll.
- Detached tasks (`detached=True`) still run and are awaited by `run()`, but their results are now genuinely excluded from the `RunSummary` (previously the flag was informational only). Observe them via their handles or `queue.results`.
- `fail_policy="fail_first"` no longer cancels `must_complete` tasks.
- `size=` now bounds *outstanding* tasks (queued + scheduled + running) rather than the internal queue length.
- `AsyncQueue.as_completed` static method moved to module-level `osiiso.as_completed`.
- `ExecutionError` messages now distinguish failed from cancelled counts.
- `group.values()` raises `ExecutionError` when tasks were cancelled, not only when they failed.
- `osiiso.run()` uses `asyncio.run(..., loop_factory=uvloop.new_event_loop)` instead of mutating the global event-loop policy.
- `ProcessQueue` follows standard multiprocessing spawn semantics: script entry points must be guarded with `if __name__ == "__main__":`, and in exchange callables defined in `__main__` now work (the previous `__spec__` patching hack is gone).
- Pool subprocesses are daemonic; task code must not spawn multiprocessing children of its own.
- `reset()` now cancels anything still pending and refuses to run while workers are alive.
- `osiiso.items` module removed; callable validation happens at submit time.

### Fixed

- **Graceful `AsyncQueue` shutdown skipped pending tasks**: `shutdown(force=False)` (and `__aexit__`) set the stop flag before draining, so queued tasks were recorded as "skipped during shutdown" instead of executed. All queues now drain outstanding work — including scheduled tasks — before stopping workers.
- Delayed tasks (`delay=` / `run_at=`) no longer occupy a worker while waiting: they are armed on loop timers (`AsyncQueue`) or a scheduler thread (sync queues), so ready tasks are never starved behind a scheduled one, and priority ordering applies from the moment a task becomes due.
- Run-completion tracking is based on an outstanding-task counter instead of `queue.join()`, closing gaps where scheduled-but-unqueued tasks could be missed.
- Large subprocess results can no longer deadlock the parent (results stream over a dedicated pipe that is read promptly).
- A worker cancelled from outside now cancels its in-flight task instead of leaking it.
- Retrying a bare awaitable is rejected at submit time (a spent coroutine cannot be re-awaited).

---

## [0.0.1] — 2026-05-11

Initial release.

### Added

- **`AsyncQueue`** — asyncio-based task execution with priorities, retries, scheduling, timeouts, groups, handles, hooks, and structured summaries.
- **`ThreadQueue`** — blocking synchronous work with the same queue shape as the async backend.
- **`ProcessQueue`** — CPU-heavy work in subprocesses with full feature parity.
- **`TaskOptions`** — immutable, reusable configuration for task submission.
- **`TaskHandle`** and **`SyncTaskHandle`** — for waiting, cancellation, status inspection, and result access.
- **`TaskGroup`** and **`SyncTaskGroup`** — named batches of submitted work.
- **`TaskResult`** and **`RunSummary`** — structured result reporting with grouping, filtering, and display.
- **`osiiso.run()`** — convenience runner with optional `uvloop` integration.
- `py.typed` marker for static type checkers.
- MkDocs documentation and runnable examples, including the feature gallery.
- Hacker News showcase demonstrating all three queue backends.

### Notes

- The package targets Python 3.13 and newer.
- Runtime dependencies are intentionally empty; optional extras are available for docs, development, and `uvloop`.
