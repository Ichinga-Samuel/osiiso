# Changelog

All notable changes to this project are documented in this file.

This project follows semantic versioning where practical:

- **Major** versions may include breaking API changes.
- **Minor** versions add backward-compatible features.
- **Patch** versions fix bugs, tighten documentation, or improve internals.

---

## [Unreleased]

### Added

- Production-ready MkDocs documentation with Material theme for GitHub Pages.
- Public README refresh with banner artwork, badges, queue examples, development workflow, and API overview.
- Community documentation: contributing guide, security policy, support guidance, and code of conduct.
- GitHub issue templates and pull request template.

---

## [1.0.0] — 2026-05-11

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
- Runnable examples: feature gallery and Hacker News showcase.

### Notes

- The package targets Python 3.13 and newer.
- Runtime dependencies are intentionally empty; optional extras are available for docs, development, and `uvloop`.
