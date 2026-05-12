# Installation

## Requirements

- **Python 3.13** or newer
- No runtime dependencies

## Install from PyPI

```bash
pip install osiiso
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add osiiso
```

## Optional: uvloop

For improved async performance on Linux and macOS, install with uvloop support:

```bash
pip install "osiiso[uvloop]"
```

!!! note "uvloop availability"
    `uvloop` is not available on Windows. When `osiiso.run()` detects that uvloop
    is installed, it uses it automatically. Pass `use_uvloop=False` to force the
    stdlib event loop.

## Development Install

Clone the repository and install with development extras:

```bash
git clone https://github.com/Ichinga-Samuel/osiiso.git
cd osiiso
pip install -e ".[dev]"
```

This includes:

| Extra | Packages |
|-------|----------|
| `dev` | pytest, pytest-asyncio, coverage, ruff, mkdocs, build |
| `docs` | mkdocs, mkdocs-material, pymdownx extensions |
| `uvloop` | uvloop ≥ 0.19 |

## Verify Installation

```python
import osiiso
print(osiiso.__all__)
```

Expected output:

```
['AsyncQueue', 'ThreadQueue', 'ProcessQueue', 'TaskHandle', 'SyncTaskHandle',
 'TaskGroup', 'SyncTaskGroup', 'TaskOptions', 'TaskResult', 'RunSummary',
 'OsiisoError', 'ClosedError', 'ExecutionError', 'run']
```
