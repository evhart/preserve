"""Tests for individual connector backends: Memory, SQLite, Shelf."""

from __future__ import annotations

import datetime
import decimal
import uuid
from typing import Any

import pytest

import preserve
from preserve.connectors.memory import Memory
from preserve.connectors.shelf import Shelf
from preserve.connectors.sqlite import SQLite

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BACKENDS = [
    pytest.param("memory", {}, id="memory"),
    pytest.param("sqlite", {"filename": ":memory:"}, id="sqlite"),
]


def make_connector(backend: str, **kwargs) -> Any:
    return preserve.open(backend, **kwargs)


# ---------------------------------------------------------------------------
# Generic CRUD contract — parametrised over Memory and SQLite
# ---------------------------------------------------------------------------


class TestCRUD:
    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_set_and_get(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["key"] = "value"
            assert db["key"] == "value"

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_overwrite(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["k"] = 1
            db["k"] = 2
            assert db["k"] == 2

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_contains(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["x"] = 99
            assert "x" in db
            assert "z" not in db

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_delitem(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["del_me"] = "bye"
            del db["del_me"]
            assert "del_me" not in db

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_keyerror_on_missing(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            with pytest.raises(KeyError):
                _ = db["nonexistent"]

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_len(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            assert len(db) == 0
            db["a"] = 1
            db["b"] = 2
            assert len(db) == 2

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_iter(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["p"] = 10
            db["q"] = 20
            keys = [k for k, _ in db]
            assert set(keys) == {"p", "q"}

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_get_default(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            assert db.get("missing") is None
            assert db.get("missing", "fallback") == "fallback"

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_get_existing(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["num"] = 42
            assert db.get("num") == 42

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_context_manager_closes(self, backend, kwargs):
        db = make_connector(backend, **kwargs)
        with db:
            db["tmp"] = "hi"
        # After __exit__ the connector is closed — no assertion needed beyond no crash


# ---------------------------------------------------------------------------
# Value types — dict, list, nested, None
# ---------------------------------------------------------------------------


class TestValueTypes:
    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_dict_value(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["rec"] = {"name": "Alice", "age": 30}
            assert db["rec"] == {"name": "Alice", "age": 30}

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_list_value(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["lst"] = [1, 2, 3]
            assert db["lst"] == [1, 2, 3]

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_nested_value(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["nested"] = {"a": {"b": [1, 2]}}
            assert db["nested"] == {"a": {"b": [1, 2]}}

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_none_value(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["nil"] = None
            assert db["nil"] is None

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_integer_value(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["n"] = 12345
            assert db["n"] == 12345

    @pytest.mark.parametrize("backend,kwargs", BACKENDS)
    def test_float_value(self, backend, kwargs):
        with make_connector(backend, **kwargs) as db:
            db["f"] = 3.14
            assert abs(db["f"] - 3.14) < 1e-9


# ---------------------------------------------------------------------------
# SQLite-specific tests
# ---------------------------------------------------------------------------


class TestSQLite:
    def test_from_uri(self, tmp_sqlite):
        with SQLite.from_uri(f"sqlite://{tmp_sqlite}") as db:
            db["hello"] = "world"
            assert db["hello"] == "world"

    def test_default_collection_name(self):
        db = SQLite(filename=":memory:")
        assert db.collection == "preserve"
        db.close()

    def test_custom_collection(self):
        with SQLite(filename=":memory:", collection="my_col") as db:
            db["k"] = "v"
            assert db["k"] == "v"

    def test_invalid_collection_raises(self):
        with pytest.raises(ValueError):
            SQLite(filename=":memory:", collection="1bad-name")

    def test_datetime_roundtrip(self):
        dt = datetime.datetime(2025, 3, 16, 12, 0, 0)
        with SQLite(filename=":memory:") as db:
            db["dt"] = dt
            # Stored as ISO string; use .get() with value_type for coercion
            assert db.get("dt", value_type=datetime.datetime) == dt

    def test_uuid_roundtrip(self):
        u = uuid.UUID("12345678-1234-5678-1234-567812345678")
        with SQLite(filename=":memory:") as db:
            db["u"] = u
            assert db.get("u", value_type=uuid.UUID) == u

    def test_decimal_roundtrip(self):
        d = decimal.Decimal("3.14159265358979323846")
        with SQLite(filename=":memory:") as db:
            db["d"] = d
            assert db.get("d", value_type=decimal.Decimal) == d

    def test_persistence(self, tmp_sqlite):
        with SQLite(filename=tmp_sqlite) as db:
            db["persisted"] = "yes"
        with SQLite(filename=tmp_sqlite) as db:
            assert db["persisted"] == "yes"

    def test_uri_invalid_scheme_raises(self):
        with pytest.raises(ValueError):
            SQLite.from_uri("mongo://somehost/db")

    def test_delete_missing_raises(self):
        with SQLite(filename=":memory:") as db:
            with pytest.raises(KeyError):
                del db["ghost"]


# ---------------------------------------------------------------------------
# Memory-specific tests
# ---------------------------------------------------------------------------


class TestMemory:
    def test_from_uri(self):
        with Memory.from_uri("memory://") as db:
            db["a"] = 1
            assert db["a"] == 1

    def test_uri_invalid_scheme_raises(self):
        with pytest.raises(ValueError):
            Memory.from_uri("sqlite:///tmp/x.db")

    def test_closed_store_raises(self):
        db = Memory()
        db.close()
        with pytest.raises(RuntimeError):
            db["k"] = "v"

    def test_scheme(self):
        assert Memory.scheme() == "memory"


# ---------------------------------------------------------------------------
# Shelf-specific tests
# ---------------------------------------------------------------------------


class TestShelf:
    def test_basic_crud(self, tmp_shelf):
        with Shelf(filename=tmp_shelf) as db:
            db["msg"] = "hello"
            assert db["msg"] == "hello"
            del db["msg"]
            assert "msg" not in db

    def test_persistence(self, tmp_shelf):
        with Shelf(filename=tmp_shelf) as db:
            db["saved"] = {"x": 1}
        with Shelf(filename=tmp_shelf) as db:
            assert db["saved"] == {"x": 1}

    def test_from_uri(self, tmp_shelf):
        with Shelf.from_uri(f"shelf://{tmp_shelf}") as db:
            db["y"] = 99
            assert db["y"] == 99

    def test_scheme(self):
        assert Shelf.scheme() == "shelf"

    def test_uri_invalid_scheme_raises(self):
        with pytest.raises(ValueError):
            Shelf.from_uri("memory://")


# ---------------------------------------------------------------------------
# Coercion API
# ---------------------------------------------------------------------------


class TestCoercion:
    def test_default_value_type_on_get(self):
        with SQLite(filename=":memory:", default_value_type=float) as db:
            db["score"] = 9
            result = db.get("score")
            assert result == 9.0
            assert isinstance(result, float)

    def test_key_types_on_get(self):
        # Pydantic v2 TypeAdapter.validate_python is by default strict for
        # primitive types; 7 -> float works (widening), but 123 -> str does not.
        # Use float for both to avoid strict-mode failures.
        with SQLite(filename=":memory:", key_types={"score": float, "count": float}) as db:
            db["score"] = 7
            db["count"] = 3
            assert isinstance(db.get("score"), float)
            assert isinstance(db.get("count"), float)

    def test_default_key_type_fallback(self):
        # int -> float is allowed in non-strict mode
        with SQLite(filename=":memory:", key_types={"score": float}, default_key_type=float) as db:
            db["other"] = 99
            result = db.get("other")
            assert isinstance(result, float)
            assert result == 99.0

    def test_value_type_per_call_overrides(self):
        with SQLite(filename=":memory:", default_value_type=float) as db:
            db["n"] = 5
            # float -> int works in non-strict pydantic
            assert isinstance(db.get("n", value_type=int), int)

    def test_key_types_and_default_value_type_mutually_exclusive(self):
        with pytest.raises(ValueError):
            SQLite(filename=":memory:", key_types={"k": int}, default_value_type=float)

    def test_default_key_type_without_key_types_raises(self):
        with pytest.raises(ValueError):
            SQLite(filename=":memory:", default_key_type=str)

    def test_getitem_unaffected_by_default_value_type(self):
        with SQLite(filename=":memory:", default_value_type=float) as db:
            db["n"] = 5
            # __getitem__ must NOT coerce
            assert db["n"] == 5
