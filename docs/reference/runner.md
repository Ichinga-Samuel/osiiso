# Runner

`osiiso.run()` — convenience entry point for executing async osiiso code.

```python
from osiiso import run
```

---

## Signature

```python
osiiso.run(coro, *, use_uvloop=None, debug=False) -> T
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `coro` | `Coroutine` | required | The coroutine to execute |
| `use_uvloop` | `bool \| None` | `None` | `None` = auto-detect; `True` = require; `False` = force stdlib |
| `debug` | `bool` | `False` | Enable asyncio debug mode |

---

## uvloop Behavior

| `use_uvloop` | uvloop installed | Behavior |
|-------------|-----------------|----------|
| `None` | Yes | Uses uvloop |
| `None` | No | Uses stdlib asyncio |
| `True` | Yes | Uses uvloop |
| `True` | No | Raises `ImportError` |
| `False` | Any | Uses stdlib asyncio |

The uvloop policy is restored to default after `run()` completes to avoid leaking globally.

---

## Example

```python
import osiiso

async def main():
    async with osiiso.AsyncQueue(workers=4) as q:
        q.submit(work, 1)
        return await q.run()

summary = osiiso.run(main())
summary = osiiso.run(main(), use_uvloop=False)  # Force stdlib
```
