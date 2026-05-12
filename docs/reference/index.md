# API Reference

Complete reference for all public classes, methods, and attributes exported by
`osiiso`.

---

## Public API

```python
from osiiso import (
    # Core queues
    AsyncQueue,
    ThreadQueue,
    ProcessQueue,

    # Handles
    TaskHandle,
    SyncTaskHandle,

    # Groups
    TaskGroup,
    SyncTaskGroup,

    # Options & results
    TaskOptions,
    TaskResult,
    RunSummary,

    # Exceptions
    OsiisoError,
    ClosedError,
    ExecutionError,

    # Runner
    run,
)
```

---

## Reference Pages

### Queues

| Class | Description |
|-------|-------------|
| [AsyncQueue](asyncqueue.md) | Asyncio-native task queue |
| [ThreadQueue](threadqueue.md) | Thread-based queue for blocking work |
| [ProcessQueue](processqueue.md) | Process-based queue for CPU-heavy work |

### Configuration

| Class | Description |
|-------|-------------|
| [TaskOptions](taskoptions.md) | Immutable task configuration |

### Handles & Groups

| Class | Description |
|-------|-------------|
| [TaskHandle](handles.md#taskhandle) | Async handle (awaitable) |
| [SyncTaskHandle](handles.md#synctaskhandle) | Blocking handle |
| [TaskGroup](groups.md#taskgroup) | Async group |
| [SyncTaskGroup](groups.md#synctaskgroup) | Blocking group |

### Results

| Class | Description |
|-------|-------------|
| [TaskResult](taskresult.md) | Immutable record of a single task |
| [RunSummary](runsummary.md) | Aggregate summary of a queue run |

### Exceptions

| Class | Description |
|-------|-------------|
| [OsiisoError](exceptions.md#osiisoerror) | Base exception |
| [ClosedError](exceptions.md#closederror) | Queue is closed |
| [ExecutionError](exceptions.md#executionerror) | Tasks failed |

### Utilities

| Function | Description |
|----------|-------------|
| [run()](runner.md) | Convenience runner with uvloop support |
