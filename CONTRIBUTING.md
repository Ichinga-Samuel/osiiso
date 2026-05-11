# Contributing

Thanks for taking the time to improve `osiiso`.

This project is a typed Python package for structured queues across `asyncio`, threads, and processes. 
Contributions should keep the public API small, predictable, and consistent across the three queue backends.

## Development setup

Requirements:

- Python 3.13 or newer
- `uv` for the commands used in this repository, or standard `pip` as a fallback

Install the package with development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Run the test suite:

```bash
uv run pytest
```

Run Ruff:

```bash
uv run ruff check .
```

Build the documentation:

```bash
uv run --extra docs mkdocs build --strict
```

Build the package:

```bash
python -m build
```

## Repository layout

```text
.
|-- src/osiiso/                  # Library source
|-- tests/                       # Unit tests
|-- docs/                        # MkDocs documentation
|-- examples/feature_gallery.py  # Compact API showcase
|-- examples/hackernews_showcase # Complete multi-backend example
|-- pyproject.toml               # Package metadata and tool configuration
`-- mkdocs.yml                   # Documentation configuration
```

## Before opening a pull request

1. Open or reference an issue when the change is non-trivial.
2. Keep the change focused on one behavior, bug, or documentation improvement.
3. Add or update tests for behavior changes.
4. Update documentation when the public API, supported behavior, examples, or development workflow changes.
5. Run `uv run pytest`.
6. Run `uv run ruff check .`.
7. Run `uv run --extra docs mkdocs build --strict` when docs changed.
8. Update `CHANGELOG.md` under `[Unreleased]`.

## Coding guidelines

- Prefer the existing API shape over new abstractions.
- Keep `AsyncQueue`, `ThreadQueue`, and `ProcessQueue` behavior aligned unless a backend-specific difference is necessary.
- Preserve typed public exports from `osiiso.__init__`.
- Keep process queue callables pickle-friendly in examples and tests.
- Avoid adding runtime dependencies unless the benefit is clear and broadly useful.
- Prefer explicit failures and structured results over silent behavior.

## Testing guidelines

- Add narrow tests for bug fixes.
- Add cross-backend tests when a behavior should be shared by async, thread, and process queues.
- Include timeout, cancellation, retry, and failure-policy coverage when touching lifecycle code.
- Keep process tests guarded by top-level functions so multiprocessing works on Windows.

## Documentation guidelines

- Keep examples short and runnable.
- Use `osiiso` in imports and package references.
- Mention Python 3.13+ when installation or environment setup is discussed.
- Keep root-level docs concise; deeper usage belongs in `docs/`.

## Release checklist

1. Confirm `pyproject.toml` version.
2. Move relevant `[Unreleased]` changelog entries into a dated release section.
3. Run tests, Ruff, docs build, and package build.
4. Confirm README examples still match the public API.
5. Tag the release after the final commit is merged.

## Reporting issues

Use the GitHub issue templates when possible. Include:

- `osiiso` version or commit.
- Python version and operating system.
- Which queue backend is affected.
- A minimal reproduction.
- Expected behavior and actual behavior.

For security reports, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.
