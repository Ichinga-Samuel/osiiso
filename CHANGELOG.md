# Changelog

All notable changes to this project are documented in this file.

This project follows semantic versioning where practical:

- Major versions may include breaking API changes.
- Minor versions add backward-compatible features.
- Patch versions fix bugs, tighten documentation, or improve internals without changing the public contract.

## [Unreleased]

### Added

- Public README refresh with banner artwork, badges, queue examples, development workflow, and API overview.
- Community documentation for contributing, security reporting, support, and project conduct.
- GitHub issue templates and pull request template.

## [1.0.0] - 2026-05-11

### Added

- `AsyncQueue` for asyncio-based task execution with priorities, retries, scheduling, timeouts, groups, handles, hooks, and structured summaries.
- `ThreadQueue` for blocking synchronous work with the same queue shape as the async backend.
- `ProcessQueue` for CPU-heavy work in subprocesses.
- Immutable `TaskOptions` for reusable task configuration.
- `TaskHandle` and `SyncTaskHandle` for waiting, cancellation, status inspection, and result access.
- `TaskGroup` and `SyncTaskGroup` for named batches of submitted work.
- `TaskResult` and `RunSummary` records for structured result reporting.
- `osiiso.run()` convenience runner with optional `uvloop` integration.
- Typed package marker through `py.typed`.
- MkDocs documentation and runnable examples.
- Hacker News showcase demonstrating all three queue backends.

### Notes

- The package targets Python 3.13 and newer.
- Runtime dependencies are intentionally empty; optional extras are available for docs, development, and `uvloop`.
