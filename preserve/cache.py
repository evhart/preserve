"""Caching utilities for expensive function results.

This module provides a flexible caching system backed by any preserve
connector — single-collection or multi-collection.  Supports both a
decorator and a context-manager pattern.

Configuration via environment variables (loaded from a ``.env`` file when
``python-dotenv`` is installed):

- ``PRESERVE_CACHE_BACKEND`` — connector scheme (default: ``"sqlite"``).
- ``PRESERVE_CACHE_URI`` — full URI; overrides backend/kwargs when set.
- ``PRESERVE_CACHE_MULTI`` — set to ``"1"`` to use the multi-connector API.
- ``PRESERVE_CACHE_COLLECTION`` — collection name for multi mode (default: ``"cache"``).
- ``PRESERVE_CACHE_FILE`` — filename for SQLite/shelf backends (default: ``"./cache.sqlite3"``).

Examples::

    # Decorator — key derived from selected parameters:
    @cache(key=["user_id", "date"])
    def expensive_computation(user_id, date):
        return complex_calculation(user_id, date)

    # Decorator — key from a callable:
    @cache(key=lambda model, temp: f"{model}_{temp:.1f}")
    def query_llm(model, temp=0.7):
        return expensive_api_call(model, temp)

    # Decorator — all arguments form the key (default):
    @cache()
    def fetch_data(url, params=None):
        return requests.get(url, params=params).json()

    # Skip the cache at call time (works with any decorated function):
    result = expensive_computation(42, "2026-01-01", use_cache=False)

    # Context manager:
    with cache(key="my_result") as ctx:
        if not ctx:
            ctx.set(compute_expensive_result())
        result = ctx.get()

    # Different backend with extra connector kwargs:
    @cache(backend="shelf", connector_kwargs={"filename": "/tmp/mycache"})
    def compute(x):
        return x * x

    # Multi-connector — multiple named collections in one store:
    @cache(multi=True, collection="scores", backend="sqlite",
           connector_kwargs={"filename": "app.db"})
    def score(user_id):
        return lookup(user_id)
"""

from __future__ import annotations

import contextlib
import hashlib
import inspect
import json
import os
import re
from functools import wraps
from pathlib import Path
from typing import Any, Callable

# Import directly from the submodule to avoid a circular import through
# preserve/__init__.py (which itself imports from preserve.cache).
from preserve.preserve import from_uri as _from_uri
from preserve.preserve import from_uri_multi as _from_uri_multi
from preserve.preserve import open as _preserve_open
from preserve.preserve import open_multi as _open_multi

try:
    from dotenv import load_dotenv as _load_dotenv

    _load_dotenv()
except ImportError:
    pass  # python-dotenv is optional; env vars still work without it

# ---------------------------------------------------------------------------
# Environment-variable defaults (all namespaced under PRESERVE_CACHE_)
# ---------------------------------------------------------------------------
_DEFAULT_BACKEND: str = os.getenv("PRESERVE_CACHE_BACKEND", "sqlite")
_DEFAULT_URI: str | None = os.getenv("PRESERVE_CACHE_URI")  # overrides backend/file
_DEFAULT_MULTI: bool = os.getenv("PRESERVE_CACHE_MULTI", "0").strip() == "1"
_DEFAULT_COLLECTION: str = os.getenv("PRESERVE_CACHE_COLLECTION", "cache")
_DEFAULT_FILE: str = os.getenv("PRESERVE_CACHE_FILE", "./cache.sqlite3")

_SANITIZE_REGEX = re.compile(r"[^a-zA-Z0-9._-]")


def _sanitize_key(text: str) -> str:
    """Replace characters outside ``[a-zA-Z0-9._-]`` with underscores."""
    return _SANITIZE_REGEX.sub("_", text)


def _hash_data(data: Any) -> str:
    """Return the SHA-256 hex digest of *data* serialized as sorted JSON."""
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _generate_cache_key(func: Callable, key: Any, args: tuple, kwargs: dict) -> str:
    """Build a deterministic cache key for *func* called with *args*/*kwargs*.

    Args:
        func: The decorated function.
        key: Key strategy — callable, list/tuple of parameter names, or ``None``.
        args: Positional arguments the function was called with.
        kwargs: Keyword arguments the function was called with (may contain
            ``use_cache`` which is excluded from the key).

    Returns:
        str: A string of the form ``"module.qualname::key_hash"``.
    """
    func_id = _sanitize_key(f"{func.__module__}.{func.__qualname__}")

    if callable(key):
        key_base = _sanitize_key(str(key(*args, **kwargs)))
    elif isinstance(key, (list, tuple)):
        sig = inspect.signature(func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        key_data = {p: bound.arguments[p] for p in key if p in bound.arguments}
        key_base = _hash_data(key_data)
    else:
        call_data = {
            "args": args,
            "kwargs": {k: v for k, v in kwargs.items() if k != "use_cache"},
        }
        key_base = _hash_data(call_data)

    return f"{func_id}::{key_base}"


@contextlib.contextmanager
def _connector_context(
    backend: str,
    uri: str | None,
    multi: bool,
    collection: str,
    connector_kwargs: dict[str, Any],
):
    """Context manager that opens a connector and keeps its full lifecycle alive.

    For multi backends the parent :class:`~preserve.connector.MultiConnector` is
    held open for the duration, preventing the sub-connector's underlying
    resource (e.g. SQLite db connection) from being closed prematurely.

    Yields:
        Connector: A ready-to-use single-collection connector.
    """
    if multi:
        mc = _from_uri_multi(uri) if uri else _open_multi(backend, **connector_kwargs)
        with mc:
            yield mc[collection]
    else:
        conn = _from_uri(uri) if uri else _preserve_open(backend, **connector_kwargs)
        with conn:
            yield conn


class CacheContext:
    """Inner context object returned by :meth:`Cache.__enter__`.

    Holds an open preserve connector, tracks whether a cached value was found,
    and writes the value back on exit if it was modified.

    Usage::

        with cache(key="my_op") as ctx:
            if not ctx:
                ctx.set(expensive_operation())
            result = ctx.get()
    """

    def __init__(self, cache_key: str, connector: Any) -> None:
        self.cache_key = cache_key
        self._connector = connector
        self._value: Any = None
        self._has_value = False
        self._modified = False

        if cache_key in self._connector:
            self._value = self._connector[cache_key]
            self._has_value = True

    def __bool__(self) -> bool:
        """Return ``True`` if a cached value is present."""
        return self._has_value

    def get(self, default: Any = None) -> Any:
        """Return the cached value, or *default* if nothing is cached."""
        return self._value if self._has_value else default

    def set(self, value: Any) -> None:
        """Stage *value* to be written to the cache when the context exits."""
        self._value = value
        self._has_value = True
        self._modified = True

    def __enter__(self) -> "CacheContext":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if exc_type is None and self._modified:
            self._connector[self.cache_key] = self._value
        self._connector.close()


class Cache:
    """Decorator and context manager for caching function results.

    All constructor parameters default to the ``PRESERVE_CACHE_*`` environment
    variables (see module docstring).

    Args:
        key: Controls cache-key generation.

            - ``str``: Used verbatim as the key (context-manager mode only).
            - ``callable``: Called with the same arguments as the decorated
              function; its string return value is used as the key.
            - ``list`` / ``tuple``: Names of function parameters whose bound
              values are hashed together to form the key.
            - ``None`` (default): All call arguments form the key.

        backend: Connector scheme (``"sqlite"``, ``"memory"``, ``"shelf"``,
            ``"mongodb"``).  Ignored when *uri* is set.  Defaults to
            ``PRESERVE_CACHE_BACKEND`` (``"sqlite"`` if unset).
        uri: Full URI passed to :func:`preserve.from_uri` /
            :func:`preserve.from_uri_multi`.  When set, *backend* and the
            ``filename`` default are ignored.  Defaults to
            ``PRESERVE_CACHE_URI`` (``None`` if unset).
        multi: Use the multi-connector API so that different caches can share
            one underlying store via named collections.  Defaults to
            ``PRESERVE_CACHE_MULTI`` (``False`` if unset).
        collection: Collection name used when *multi* is ``True``.
            Defaults to ``PRESERVE_CACHE_COLLECTION`` (``"cache"`` if unset).
        connector_kwargs: Extra keyword arguments forwarded verbatim to the
            connector constructor (e.g. ``filename``, ``host``, ``port``,
            ``database``, or coercion fields such as ``default_value_type``).
            For ``sqlite`` and ``shelf`` backends, ``filename`` defaults to
            ``PRESERVE_CACHE_FILE`` when not already present.

    Examples::

        # Selected parameters form the key:
        @cache(key=["user_id", "date"])
        def compute(user_id, date): ...

        # Callable key:
        @cache(key=lambda model, temp: f"{model}_{temp:.1f}")
        def query_llm(model, temp=0.7): ...

        # Bypass the cache at call time:
        result = compute(42, "2026-03-16", use_cache=False)

        # Context manager:
        with cache(key="my_op") as ctx:
            if not ctx:
                ctx.set(expensive_operation())
            result = ctx.get()

        # Different backend:
        @cache(backend="shelf", connector_kwargs={"filename": "/tmp/mycache"})
        def expensive(x): ...

        # Multi-connector with a named collection:
        @cache(multi=True, collection="scores", connector_kwargs={"filename": "app.db"})
        def score(user_id): ...
    """

    def __init__(
        self,
        key: Any = None,
        *,
        backend: str | None = None,
        uri: str | None = None,
        multi: bool | None = None,
        collection: str | None = None,
        connector_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if key is not None and not isinstance(key, str) and not callable(key) and not isinstance(key, (list, tuple)):
            raise ValueError("key must be a string, callable, list/tuple of parameter names, or None")

        self.key = key
        self._backend: str = backend if backend is not None else _DEFAULT_BACKEND
        self._uri: str | None = uri if uri is not None else _DEFAULT_URI
        self._multi: bool = multi if multi is not None else _DEFAULT_MULTI
        self._collection: str = collection if collection is not None else _DEFAULT_COLLECTION

        # Build effective connector_kwargs; inject a default filename for
        # file-based backends when not already supplied.
        eff: dict[str, Any] = dict(connector_kwargs) if connector_kwargs else {}
        if self._uri is None and self._backend in ("sqlite", "shelf") and "filename" not in eff:
            path = Path(_DEFAULT_FILE)
            path.parent.mkdir(exist_ok=True, parents=True)
            eff["filename"] = str(path)
        self._connector_kwargs: dict[str, Any] = eff

        self._context: CacheContext | None = None

    # -- context-manager interface ----------------------------------------

    def __enter__(self) -> CacheContext:
        if not isinstance(self.key, str):
            raise ValueError("When using cache as a context manager, key must be a string.")
        if self._multi:
            self._multi_connector = (
                _from_uri_multi(self._uri) if self._uri else _open_multi(self._backend, **self._connector_kwargs)
            )
            self._multi_connector.__enter__()
            connector = self._multi_connector[self._collection]
        else:
            connector = _from_uri(self._uri) if self._uri else _preserve_open(self._backend, **self._connector_kwargs)
            connector.__enter__()
        self._context = CacheContext(_sanitize_key(self.key), connector)
        return self._context

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self._context is not None:
            self._context.__exit__(exc_type, exc_val, exc_tb)
            self._context = None
        if hasattr(self, "_multi_connector") and self._multi_connector is not None:
            self._multi_connector.__exit__(exc_type, exc_val, exc_tb)
            self._multi_connector = None

    # -- decorator interface ----------------------------------------------

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not kwargs.get("use_cache", True):
                actual = {k: v for k, v in kwargs.items() if k != "use_cache"}
                return func(*args, **actual)

            cache_key = _generate_cache_key(func, self.key, args, kwargs)
            with _connector_context(
                self._backend, self._uri, self._multi, self._collection, self._connector_kwargs
            ) as connector:
                if cache_key in connector:
                    return connector[cache_key]
                actual = {k: v for k, v in kwargs.items() if k != "use_cache"}
                result = func(*args, **actual)
                connector[cache_key] = result
                return result

        return wrapper


def cache(
    key: Any = None,
    *,
    backend: str | None = None,
    uri: str | None = None,
    multi: bool | None = None,
    collection: str | None = None,
    connector_kwargs: dict[str, Any] | None = None,
) -> Cache:
    """Return a :class:`Cache` instance usable as a decorator or context manager.

    Args:
        key: Controls cache-key generation.

            - ``str``: Used verbatim as the key (context-manager mode only).
            - ``callable``: Called with the same arguments as the decorated
              function; its string return value is used as the key.
            - ``list`` / ``tuple``: Names of function parameters whose bound
              values are hashed together to form the key.
            - ``None`` (default): All call arguments form the key.

        backend: Connector scheme (``"sqlite"``, ``"memory"``, ``"shelf"``,
            ``"mongodb"``).  Defaults to ``PRESERVE_CACHE_BACKEND``.
        uri: Full URI string; overrides *backend* / *connector_kwargs*.
            Defaults to ``PRESERVE_CACHE_URI``.
        multi: Use the multi-connector API with named collections.
            Defaults to ``PRESERVE_CACHE_MULTI``.
        collection: Collection name when *multi* is ``True``.
            Defaults to ``PRESERVE_CACHE_COLLECTION`` (``"cache"`` if unset).
        connector_kwargs: Extra keyword arguments forwarded to the connector
            constructor.  For ``sqlite`` and ``shelf``, ``filename`` defaults
            to ``PRESERVE_CACHE_FILE`` when not supplied.

    Returns:
        Cache: A :class:`Cache` instance.
    """
    return Cache(
        key=key,
        backend=backend,
        uri=uri,
        multi=multi,
        collection=collection,
        connector_kwargs=connector_kwargs,
    )
