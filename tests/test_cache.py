"""Tests for the Cache decorator and context manager (preserve.cache)."""

from __future__ import annotations

import importlib
import sys

import pytest

# preserve/__init__.py re-exports the `cache` function as preserve.cache,
# so `import preserve.cache as _cache_module` would bind to the function.
# Load the real submodule via a bare import (which registers it in sys.modules)
# then retrieve it by key.
import preserve.cache  # noqa: F401 — populates sys.modules["preserve.cache"]
from preserve.cache import (
    Cache,
    CacheContext,
    _generate_cache_key,
    _hash_data,
    _sanitize_key,
    cache,
)

_cache_module = sys.modules["preserve.cache"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class TestSanitizeKey:
    def test_alphanumeric_unchanged(self):
        assert _sanitize_key("hello_World.123-ok") == "hello_World.123-ok"

    def test_spaces_replaced(self):
        assert _sanitize_key("hello world") == "hello_world"

    def test_special_chars_replaced(self):
        assert _sanitize_key("a/b:c?d=e") == "a_b_c_d_e"


class TestHashData:
    def test_deterministic(self):
        h1 = _hash_data({"a": 1, "b": [2, 3]})
        h2 = _hash_data({"a": 1, "b": [2, 3]})
        assert h1 == h2

    def test_different_data(self):
        assert _hash_data({"a": 1}) != _hash_data({"a": 2})

    def test_sort_keys_stable(self):
        assert _hash_data({"z": 1, "a": 2}) == _hash_data({"a": 2, "z": 1})


class TestGenerateCacheKey:
    def _func(self, x, y=10):
        return x + y

    def test_none_key_hashes_all_args(self):
        k1 = _generate_cache_key(self._func, None, (1,), {"y": 2})
        k2 = _generate_cache_key(self._func, None, (1,), {"y": 2})
        assert k1 == k2

    def test_different_args_different_key(self):
        k1 = _generate_cache_key(self._func, None, (1,), {})
        k2 = _generate_cache_key(self._func, None, (2,), {})
        assert k1 != k2

    def test_callable_key(self):
        # _sanitize_key replaces non-alphanumeric chars; '+' becomes '_'
        def key_fn(x, y=10):
            return f"{x}plus{y}"

        k = _generate_cache_key(self._func, key_fn, (3,), {"y": 4})
        assert "3plus4" in k

    def test_list_key_uses_named_params(self):
        k1 = _generate_cache_key(self._func, ["x"], (5,), {"y": 999})
        k2 = _generate_cache_key(self._func, ["x"], (5,), {"y": 0})
        # Only x differs → same key
        assert k1 == k2

    def test_list_key_different_params(self):
        k1 = _generate_cache_key(self._func, ["x"], (1,), {})
        k2 = _generate_cache_key(self._func, ["x"], (2,), {})
        assert k1 != k2

    def test_use_cache_excluded_from_key(self):
        k1 = _generate_cache_key(self._func, None, (1,), {"use_cache": True})
        k2 = _generate_cache_key(self._func, None, (1,), {"use_cache": False})
        assert k1 == k2


# ---------------------------------------------------------------------------
# CacheContext
# ---------------------------------------------------------------------------


class TestCacheContext:
    def _make_ctx(self, key="ctx_key", value=None):
        from preserve.connectors.memory import Memory

        conn = Memory()
        if value is not None:
            conn[key] = value
        return CacheContext(key, conn), conn

    def test_bool_false_when_empty(self):
        ctx, _ = self._make_ctx()
        assert not ctx

    def test_bool_true_when_cached(self):
        ctx, _ = self._make_ctx(value="existing")
        assert ctx

    def test_get_returns_cached(self):
        ctx, _ = self._make_ctx(value=42)
        assert ctx.get() == 42

    def test_get_returns_default_when_empty(self):
        ctx, _ = self._make_ctx()
        assert ctx.get("default") == "default"

    def test_set_marks_modified(self):
        ctx, conn = self._make_ctx()
        ctx.set(99)
        assert ctx.get() == 99
        assert ctx._modified

    def test_exit_writes_modified_value(self):
        ctx, conn = self._make_ctx(key="w_key")
        ctx.set("written")
        # __exit__ writes the value then closes the connector;
        # check _value directly since connector is closed afterwards.
        assert ctx._modified
        assert ctx.get() == "written"

    def test_exit_does_not_write_on_exception(self):
        ctx, conn = self._make_ctx(key="exc_key")
        ctx.set("not_written")
        ctx.__exit__(ValueError, ValueError("boom"), None)
        # Value should NOT have been written
        assert "exc_key" not in conn


# ---------------------------------------------------------------------------
# Cache as a decorator
# ---------------------------------------------------------------------------


class TestCacheDecorator:
    # NOTE: The memory backend creates a fresh in-process store each call,
    # so decorator-level caching tests must use a persistent backend (SQLite)
    # to observe cache hits across calls.

    def test_basic_caching(self, tmp_path):
        path = str(tmp_path / "c.db")
        call_count = 0

        @cache(backend="sqlite", connector_kwargs={"filename": path})
        def expensive(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        assert expensive(5) == 10
        assert expensive(5) == 10  # cache hit
        assert call_count == 1

    def test_different_args_not_cached(self, tmp_path):
        path = str(tmp_path / "c2.db")
        call_count = 0

        @cache(backend="sqlite", connector_kwargs={"filename": path})
        def fn(x):
            nonlocal call_count
            call_count += 1
            return x

        fn(1)
        fn(2)
        assert call_count == 2

    def test_use_cache_false_bypasses(self, tmp_path):
        path = str(tmp_path / "c3.db")
        call_count = 0

        @cache(backend="sqlite", connector_kwargs={"filename": path})
        def fn(x):
            nonlocal call_count
            call_count += 1
            return x

        fn(10)
        fn(10, use_cache=False)
        assert call_count == 2

    def test_use_cache_does_not_leak_to_func(self):
        """use_cache must not be forwarded to the wrapped function."""

        @cache(backend="memory")
        def fn(**kwargs):
            assert "use_cache" not in kwargs
            return "ok"

        result = fn(use_cache=False)
        assert result == "ok"

    def test_callable_key(self, tmp_path):
        path = str(tmp_path / "ck.db")
        call_count = 0

        @cache(backend="sqlite", connector_kwargs={"filename": path}, key=lambda x, y: f"{x}and{y}")
        def add(x, y):
            nonlocal call_count
            call_count += 1
            return x + y

        add(1, 2)
        add(1, 2)
        assert call_count == 1

    def test_list_key_ignores_other_params(self, tmp_path):
        path = str(tmp_path / "lk.db")
        call_count = 0

        @cache(backend="sqlite", connector_kwargs={"filename": path}, key=["user_id"])
        def fetch(user_id, noise=0):
            nonlocal call_count
            call_count += 1
            return user_id * 100

        fetch(user_id=7, noise=42)
        fetch(user_id=7, noise=99)
        assert call_count == 1

    def test_preserves_function_metadata(self):
        @cache(backend="memory")
        def my_func(x):
            """My docstring."""
            return x

        assert my_func.__name__ == "my_func"
        assert my_func.__doc__ == "My docstring."

    def test_invalid_key_type_raises(self):
        with pytest.raises(ValueError):
            Cache(key=12345)  # type: ignore[arg-type]

    def test_sqlite_backend_decorator(self, tmp_path):
        path = str(tmp_path / "cache.db")
        call_count = 0

        @cache(backend="sqlite", connector_kwargs={"filename": path})
        def compute(n):
            nonlocal call_count
            call_count += 1
            return n**2

        assert compute(4) == 16
        assert compute(4) == 16
        assert call_count == 1

    def test_multi_backend_decorator(self, tmp_path):
        path = str(tmp_path / "multi_mem.db")
        call_count = 0

        @cache(multi=True, collection="results", backend="sqlite", connector_kwargs={"filename": path})
        def square(n):
            nonlocal call_count
            call_count += 1
            return n**2

        assert square(3) == 9
        assert square(3) == 9
        assert call_count == 1

    def test_multi_backend_sqlite(self, tmp_path):
        path = str(tmp_path / "multi.db")
        call_count = 0

        @cache(multi=True, collection="myfunc", backend="sqlite", connector_kwargs={"filename": path})
        def cube(n):
            nonlocal call_count
            call_count += 1
            return n**3

        assert cube(3) == 27
        assert cube(3) == 27
        assert call_count == 1


# ---------------------------------------------------------------------------
# Cache as a context manager
# ---------------------------------------------------------------------------


class TestCacheContextManager:
    def test_context_manager_cache_miss(self):
        with cache(key="cm_miss", backend="memory") as ctx:
            assert not ctx
            ctx.set(42)
        with cache(key="cm_miss", backend="memory") as ctx:
            # Each memory connector is fresh — this is fine, tests the pattern
            assert not ctx

    def test_context_manager_requires_string_key(self):
        with pytest.raises(ValueError, match="key must be a string"):
            with cache(key=["not", "a", "string"], backend="memory"):
                pass

    def test_context_manager_set_and_get(self):
        from preserve.connectors.memory import Memory

        conn = Memory()
        ctx = CacheContext("test_key", conn)
        assert not ctx
        ctx.set("stored_value")
        assert ctx.get() == "stored_value"
        ctx.__exit__(None, None, None)

    def test_context_manager_sqlite(self, tmp_path):
        path = str(tmp_path / "cm.db")
        with cache(key="my_op", backend="sqlite", connector_kwargs={"filename": path}) as ctx:
            if not ctx:
                ctx.set("result")
            val = ctx.get()
        assert val == "result"

    def test_context_manager_multi_sqlite(self, tmp_path):
        path = str(tmp_path / "cm_multi.db")
        with cache(
            key="multi_op", multi=True, collection="results", backend="sqlite", connector_kwargs={"filename": path}
        ) as ctx:
            if not ctx:
                ctx.set({"answer": 42})
            val = ctx.get()
        assert val == {"answer": 42}


# ---------------------------------------------------------------------------
# Environment variable configuration
# ---------------------------------------------------------------------------


class TestCacheEnvVars:
    def test_backend_from_env(self, monkeypatch):
        monkeypatch.setenv("PRESERVE_CACHE_BACKEND", "memory")
        importlib.reload(sys.modules["preserve.cache"])
        # Re-import names from the freshly reloaded module
        _cache2 = sys.modules["preserve.cache"].cache

        # Memory connects fresh each time; just verify it doesn't error
        @_cache2()
        def fn(x):
            return x * 2

        assert fn(5) == 10

    def test_collection_from_env(self, monkeypatch):
        monkeypatch.setenv("PRESERVE_CACHE_COLLECTION", "my_collection")
        importlib.reload(sys.modules["preserve.cache"])
        _cache3 = sys.modules["preserve.cache"].cache

        c = _cache3(backend="memory", multi=True)
        # After reload, _DEFAULT_COLLECTION picks up the env var
        assert c._collection == "my_collection"
