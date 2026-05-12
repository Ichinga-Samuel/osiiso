# Results & Summaries

Every queue `run()` returns a `RunSummary` — a structured aggregate of all task
outcomes. Individual tasks produce immutable `TaskResult` records.

---

## TaskResult

Each completed task generates a frozen `TaskResult` dataclass with full
execution metadata:

```python
result = handle.result()

result.task_id       # UUID hex identifier
result.name          # Human-readable name
result.status        # "succeeded", "failed", or "cancelled"
result.value         # Return value (succeeded only)
result.exception     # Exception object (failed only)
result.attempts      # Total attempts (1 = no retries)
result.priority      # Priority level
result.must_complete # Whether protected from cancellation
result.group_id      # Group identifier, or None
result.detached      # Whether excluded from run() aggregation
result.created_at    # perf_counter submission timestamp
result.started_at    # perf_counter first execution timestamp
result.finished_at   # perf_counter completion timestamp
result.duration      # Wall-clock seconds from start to finish
result.message       # Short description of the outcome
```

---

## RunSummary

`RunSummary` aggregates all task results from a single `run()` call:

### Counters

```python
summary = await q.run()

summary.total_submitted  # Total tasks in this run
summary.succeeded        # Count of succeeded tasks
summary.failed           # Count of failed tasks
summary.cancelled        # Count of cancelled tasks
summary.timed_out        # True if the run hit a timeout
summary.duration         # Wall-clock seconds for the entire run
```

### Status Check

```python
summary.ok  # True if no failures, cancellations, or timeouts
```

### Extracting Values

```python
# Return values from succeeded tasks (in result order)
summary.values    # tuple[Any, ...]

# Failed TaskResult objects
summary.errors    # tuple[TaskResult, ...]

# All results
summary.results   # tuple[TaskResult, ...]
```

### Filtering

```python
summary.successes()      # tuple of succeeded TaskResult objects
summary.cancellations()  # tuple of cancelled TaskResult objects
```

---

## Lookup Methods

### By Task ID

```python
by_id = summary.by_task_id()  # dict[str, TaskResult]
result = by_id["abc123..."]
```

### By Name

```python
by_name = summary.by_name()  # dict[str, tuple[TaskResult, ...]]
fetch_results = by_name["fetch"]
```

### By Group

```python
by_group = summary.by_group()  # dict[str | None, tuple[TaskResult, ...]]
api_results = by_group["api-calls"]
ungrouped = by_group[None]
```

---

## Error Handling

### Strict Mode

Pass `strict=True` to `run()` to raise `ExecutionError` automatically when any
task fails:

```python
try:
    summary = await q.run(strict=True)
except osiiso.ExecutionError as e:
    print(f"{len(e.results)} task(s) failed")
    for result in e.results:
        print(f"  {result.name}: {result.message}")
```

### Manual Check

Use `raise_for_errors()` for deferred error handling:

```python
summary = await q.run()

# Do some processing first...
summary.display()

# Then raise if there were failures
summary.raise_for_errors()
```

---

## Display

`display()` prints a clean, human-readable report to stdout:

```python
summary.display()
```

Output:

```
----------------------------------------
  Run Summary: PASSED
----------------------------------------
  Total tasks : 6
  Succeeded   : 6
  Failed      : 0
  Cancelled   : 0
  Duration    : 0.142s
----------------------------------------
```

When there are failures:

```
----------------------------------------
  Run Summary: COMPLETED WITH ERRORS
----------------------------------------
  Total tasks : 5
  Succeeded   : 3
  Failed      : 2
  Cancelled   : 0
  Duration    : 1.234s
----------------------------------------
  Failures:
    - fetch-api (3 attempts)
      Connection refused
    - parse-data
      Invalid JSON at line 42
----------------------------------------
```

---

## Next Steps

- [Lifecycle & Policies](lifecycle-and-policies.md) — Modes, shutdown, and hooks
- [API Reference: RunSummary](../reference/runsummary.md) — Complete method reference
