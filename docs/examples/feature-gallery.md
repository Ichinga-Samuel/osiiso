# Feature Gallery

`examples/feature_gallery.py` is a compact script that demonstrates the most
important features without the larger Hacker News pipeline.

Run it:

```bash
uv run python examples/feature_gallery.py
```

It covers:

- `AsyncQueue`, `ThreadQueue`, and `ProcessQueue`.
- `submit()`, `map()`, and current `group([(fn, *args), ...])` usage.
- `TaskOptions` and inline options.
- Priority, retries, retry delay, timeout, delay, and `must_complete`.
- Bound task decorators with `@q.task()`.
- Event hooks.
- `AsyncQueue.as_completed()`.
- `RunSummary.display()`.
- Group values and grouped summaries.

Use this script when you want a quick sanity check that the package works on a
machine before running the larger example project.
