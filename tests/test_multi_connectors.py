"""Tests for the multi-connector API (MultiMemory, MultiSQLite, MultiShelf)."""

from __future__ import annotations

import pytest

import preserve
from preserve.connectors.memory import MultiMemory
from preserve.connectors.shelf import MultiShelf
from preserve.connectors.sqlite import MultiSQLite

# ---------------------------------------------------------------------------
# Parametrised multi-backend fixtures
# ---------------------------------------------------------------------------

MULTI_BACKENDS = [
    pytest.param("memory", {}, id="memory"),
    pytest.param("sqlite", {"filename": ":memory:"}, id="sqlite"),
]


def open_multi(backend: str, **kwargs):
    return preserve.open_multi(backend, **kwargs)


# ---------------------------------------------------------------------------
# Generic multi-connector contract
# ---------------------------------------------------------------------------


class TestMultiConnectorCRUD:
    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_two_collections_isolated(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            db["col_a"]["x"] = 1
            db["col_b"]["x"] = 99
            assert db["col_a"]["x"] == 1
            assert db["col_b"]["x"] == 99

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_same_collection_stable(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            db["items"]["a"] = "alpha"
            db["items"]["b"] = "beta"
            assert db["items"]["a"] == "alpha"
            assert db["items"]["b"] == "beta"

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_collection_contains(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            db["c"]["key"] = "val"
            assert "key" in db["c"]
            assert "missing" not in db["c"]

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_collection_delete(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            db["d"]["item"] = 42
            del db["d"]["item"]
            assert "item" not in db["d"]

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_collection_len(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            assert len(db["e"]) == 0
            db["e"]["p"] = 1
            db["e"]["q"] = 2
            assert len(db["e"]) == 2

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_context_manager_closes_cleanly(self, backend, kwargs):
        mc = open_multi(backend, **kwargs)
        with mc:
            mc["f"]["data"] = {"ok": True}
        # No assertion — just checking no exception is raised on close


# ---------------------------------------------------------------------------
# Per-collection coercion overrides via open()
# ---------------------------------------------------------------------------


class TestMultiConnectorOpenOverrides:
    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_default_value_type_override(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            scores = db.open("scores", default_value_type=float)
            db["scores"]["s"] = 7
            result = scores.get("s")
            assert isinstance(result, float)
            assert result == 7.0

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_key_types_override(self, backend, kwargs):
        # Pydantic v2 TypeAdapter is strict for str; use numeric types only.
        with open_multi(backend, **kwargs) as db:
            typed = db.open("typed", key_types={"n": int, "score": float})
            db["typed"]["n"] = 42
            db["typed"]["score"] = 7
            assert typed.get("n") == 42
            assert isinstance(typed.get("n"), int)
            assert isinstance(typed.get("score"), float)

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_open_without_overrides_same_as_getitem(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            col = db.open("plain")
            db["plain"]["k"] = "v"
            assert col["k"] == "v"

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_open_mutually_exclusive_raises(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            with pytest.raises(ValueError):
                db.open("bad", key_types={"k": int}, default_value_type=float)

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_open_default_key_type_without_key_types_raises(self, backend, kwargs):
        with open_multi(backend, **kwargs) as db:
            with pytest.raises(ValueError):
                db.open("bad2", default_key_type=str)

    @pytest.mark.parametrize("backend,kwargs", MULTI_BACKENDS)
    def test_override_sticky_via_getitem(self, backend, kwargs):
        """After open(), subsequent db[col] lookups also use the override."""
        with open_multi(backend, **kwargs) as db:
            db.open("sticky", default_value_type=float)
            db["sticky"]["v"] = 3
            # __getitem__ on the multi-connector returns the same sub-connector
            result = db["sticky"].get("v")
            assert isinstance(result, float)


# ---------------------------------------------------------------------------
# MultiSQLite-specific
# ---------------------------------------------------------------------------


class TestMultiSQLite:
    def test_from_uri(self, tmp_path):
        path = str(tmp_path / "m.db")
        with MultiSQLite.from_uri(f"sqlite://{path}") as db:
            db["t1"]["k"] = "v"
            assert db["t1"]["k"] == "v"

    def test_collections_list(self):
        with MultiSQLite(filename=":memory:") as db:
            db["alpha"]["x"] = 1
            db["beta"]["y"] = 2
            cols = db.collections()
            assert set(cols) == {"alpha", "beta"}

    def test_persistence(self, tmp_path):
        path = str(tmp_path / "persist.db")
        with MultiSQLite(filename=path) as db:
            db["store"]["key"] = "stored"
        with MultiSQLite(filename=path) as db:
            assert db["store"]["key"] == "stored"

    def test_invalid_collection_raises(self):
        with MultiSQLite(filename=":memory:") as db:
            with pytest.raises(ValueError):
                _ = db["1bad-col"]

    def test_scheme(self):
        assert MultiSQLite.scheme() == "sqlite"


# ---------------------------------------------------------------------------
# MultiMemory-specific
# ---------------------------------------------------------------------------


class TestMultiMemory:
    def test_basic(self):
        with MultiMemory() as db:
            db["c1"]["a"] = 1
            db["c2"]["a"] = 2
            assert db["c1"]["a"] == 1
            assert db["c2"]["a"] == 2

    def test_collections_list(self):
        with MultiMemory() as db:
            db["x"]["k"] = 1
            db["y"]["k"] = 2
            assert set(db.collections()) == {"x", "y"}

    def test_from_uri(self):
        with MultiMemory.from_uri("memory://") as db:
            db["col"]["item"] = "hi"
            assert db["col"]["item"] == "hi"

    def test_scheme(self):
        assert MultiMemory.scheme() == "memory"


# ---------------------------------------------------------------------------
# MultiShelf-specific
# ---------------------------------------------------------------------------


class TestMultiShelf:
    def test_basic(self, tmp_path):
        # MultiShelf uses 'directory', not 'filename'
        with MultiShelf(directory=str(tmp_path / "shelves")) as db:
            db["shelf_col"]["entry"] = {"data": 123}
            assert db["shelf_col"]["entry"] == {"data": 123}

    def test_scheme(self):
        assert MultiShelf.scheme() == "shelf"
