# 🥫 Preserve — A simple Python key/value store with multiple backends

[![CI](https://github.com/evhart/preserve/actions/workflows/push.yml/badge.svg)](https://github.com/evhart/preserve/actions/workflows/push.yml)
[![PyPI](https://img.shields.io/pypi/v/preserve)](https://pypi.org/project/preserve/)
[![Python](https://img.shields.io/pypi/pyversions/preserve)](https://pypi.org/project/preserve/)
[![License](https://img.shields.io/pypi/l/preserve)](https://github.com/evhart/preserve/blob/master/LICENSE)

Preserve is a simple, dict-like key/value store for Python 3.9+ that supports multiple storage backends (SQLite, in-memory, shelf, MongoDB) with a unified API. It also provides a response-caching decorator and context manager for memoising expensive function calls.

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Backends](#backends)
- [Multi-Collection API](#multi-collection-api)
- [Type Coercion](#type-coercion)
- [Caching](#caching)
- [CLI](#cli)
- [Running Tests](#running-tests)

---

## Installation

```bash
pip install preserve
# or, with uv:
uv add preserve
```

Install from source:

```bash
pip install git+https://github.com/evhart/preserve#egg=preserve
```

**Requirements:** Python ≥ 3.9, [pydantic](https://docs.pydantic.dev/) v2, [python-dotenv](https://pypi.org/project/python-dotenv/).

Optional — MongoDB backend:

```bash
pip install preserve[mongo]
# or
uv add "preserve[mongo]"
```

---

## Quick Start

The API mirrors a standard Python `dict`. Open a connector, use it as a dictionary, and close it (or use it as a context manager).

```python
import preserve

# Open an SQLite-backed store (file persists across runs)
with preserve.open("sqlite", filename="my_store.db") as db:
    db["user:1"] = {"name": "Alice", "score": 42}
    print(db["user:1"])          # {'name': 'Alice', 'score': 42}
    print("user:1" in db)        # True
    del db["user:1"]

# Open an in-memory store (ephemeral)
with preserve.open("memory") as db:
    db["temp"] = [1, 2, 3]

# Open via URI
with preserve.from_uri("sqlite:///my_store.db") as db:
    db["key"] = "value"
```

---

## Backends

| Scheme | Class | Notes |
|---|---|---|
| `sqlite` | `SQLite` | Persisted JSON in SQLite; supports `:memory:` |
| `memory` | `Memory` | In-process dict; lost when closed |
| `shelf` | `Shelf` | Python `shelve` file |
| `mongodb` | `Mongo` | Requires `pymongo` |

List available backends at runtime:

```python
from preserve.preserve import connectors
for c in connectors():
    print(c.scheme())
```

Register a third-party connector:

```python
from preserve import Preserve
Preserve.register(MyCustomConnector)
```

---

## Multi-Collection API

`open_multi` / `from_uri_multi` return a `MultiConnector` that maps collection names to individual stores (e.g. one SQLite table per collection, one file per Shelf collection).

```python
import preserve

with preserve.open_multi("sqlite", filename="app.db") as db:
    db["users"]["alice"] = {"role": "admin"}
    db["logs"]["2024-01-01"] = {"event": "login"}

    # Same collection reference is stable
    users = db["users"]
    users["bob"] = {"role": "viewer"}

with preserve.from_uri_multi("sqlite:///app.db") as db:
    print(db["users"]["alice"])   # {'role': 'admin'}
```

---

## Type Coercion

Connectors use Pydantic v2 to coerce retrieved values to a specific type. Coercion is applied on **read**, not on write.

### Per-connector defaults

```python
from preserve.connectors import SQLite

with SQLite(filename=":memory:", default_value_type=float) as db:
    db["score"] = 9          # stored as int
    print(db.get("score"))   # 9.0  (coerced to float on read)
```

### Per-key mapping

```python
with SQLite(filename=":memory:", key_types={"score": float, "count": int}) as db:
    db["score"] = "7.5"
    print(db.get("score"))   # 7.5  (str → float)
```

### Per-call override

```python
with SQLite(filename=":memory:") as db:
    db["n"] = 5
    print(db.get("n", value_type=float))   # 5.0
```

### Per-collection override (multi-connector)

```python
with preserve.open_multi("sqlite", filename="app.db") as db:
    typed = db.open("metrics", default_value_type=float)
    db["metrics"]["latency"] = "12"
    print(typed.get("latency"))   # 12.0
```

> **Note:** Pydantic v2 uses strict validation for primitives by default. `int → float` and `str → int` (when the string is a valid integer) work; `int → str` does not.

---

## Caching

Preserve ships a `cache` decorator and `Cache` context manager for memoising function results. The cache key is derived from the function name and its arguments.

### Decorator

```python
from preserve import cache

@cache(backend="sqlite", connector_kwargs={"filename": "cache.db"})
def fetch_data(url: str) -> dict:
    ...  # expensive HTTP call
    return {}

fetch_data("https://example.com/api")   # computed and stored
fetch_data("https://example.com/api")   # returned from cache
fetch_data("https://example.com/api", use_cache=False)   # bypass cache
```

The `use_cache` keyword argument is injected by the decorator; it is never passed through to the wrapped function.

**Key customisation:**

```python
# Cache only on selected arguments
@cache(backend="sqlite", connector_kwargs={"filename": "cache.db"}, key=["user_id"])
def get_profile(user_id: int, noise: str = "") -> dict:
    ...

# Use a callable to compute the key
@cache(backend="sqlite", connector_kwargs={"filename": "cache.db"},
       key=lambda user_id, **_: f"profile:{user_id}")
def get_profile(user_id: int) -> dict:
    ...
```

**Multi-collection backend:**

```python
@cache(multi=True, collection="results", backend="sqlite",
       connector_kwargs={"filename": "cache.db"})
def compute(n: int) -> int:
    return n ** 2
```

### Context manager

```python
from preserve import Cache

c = Cache(key="my_key", backend="sqlite", connector_kwargs={"filename": "cache.db"})
with c as ctx:
    if ctx:
        result = ctx.get()        # cache hit
    else:
        result = expensive_call()
        ctx.set(result)           # write back on __exit__
```

### Environment variables

All `cache()` / `Cache()` defaults can be set via environment variables (loaded automatically from a `.env` file):

| Variable | Default | Description |
|---|---|---|
| `PRESERVE_CACHE_BACKEND` | `sqlite` | Backend scheme |
| `PRESERVE_CACHE_URI` | — | Full URI (overrides backend + file) |
| `PRESERVE_CACHE_MULTI` | `false` | Use multi-collection backend |
| `PRESERVE_CACHE_COLLECTION` | `preserve_cache` | Collection name (multi only) |
| `PRESERVE_CACHE_FILE` | `~/.local/share/preserve/preserve.db` | File path for file-backed backends |

---

## CLI

```
Usage: preserve [OPTIONS] COMMAND [ARGS]...

  🥫 Preserve — A simple Key/Value database with multiple backends.

Commands:
  connectors  List available connectors.
  export      Export a database to a different output.
  header      Show the first rows of a database.
```

Example:

```bash
preserve connectors
preserve export sqlite:///source.db sqlite:///dest.db
preserve header sqlite:///my_store.db
```

---

## Running Tests

```bash
uv sync --group dev
uv run pytest
```

Test coverage report:

```bash
uv run pytest --cov=preserve --cov-report=term-missing
```
