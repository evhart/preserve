"""Tests for the Preserve factory (open, from_uri, open_multi, from_uri_multi, register)."""

from __future__ import annotations

import pytest

import preserve
from preserve.connector import Connector, MultiConnector
from preserve.preserve import Preserve, _reset


class TestPreserveOpen:
    def test_open_memory(self):
        with preserve.open("memory") as db:
            db["k"] = "v"
            assert db["k"] == "v"

    def test_open_sqlite(self):
        with preserve.open("sqlite", filename=":memory:") as db:
            db["n"] = 42
            assert db["n"] == 42

    def test_open_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown connector format"):
            preserve.open("nonexistent_backend")

    def test_open_returns_connector(self):
        db = preserve.open("memory")
        assert isinstance(db, Connector)
        db.close()

    def test_connectors_list(self):
        # preserve.connectors is a submodule; use the submodule function directly.
        from preserve.preserve import connectors as _connectors

        cs = _connectors()
        schemes = {c.scheme() for c in cs}
        assert "memory" in schemes
        assert "sqlite" in schemes
        assert "shelf" in schemes


class TestPreserveFromUri:
    def test_from_uri_memory(self):
        with preserve.from_uri("memory://") as db:
            db["a"] = 1
            assert db["a"] == 1

    def test_from_uri_sqlite(self):
        with preserve.from_uri("sqlite://:memory:") as db:
            db["x"] = "y"
            assert db["x"] == "y"

    def test_from_uri_no_scheme_raises(self):
        with pytest.raises(ValueError, match="Invalid URI"):
            preserve.from_uri("no-scheme-here")

    def test_from_uri_unknown_scheme_raises(self):
        with pytest.raises(ValueError, match="Unknown connector scheme"):
            preserve.from_uri("bogus://host/db")


class TestPreserveOpenMulti:
    def test_open_multi_memory(self):
        with preserve.open_multi("memory") as db:
            db["c"]["k"] = 1
            assert db["c"]["k"] == 1

    def test_open_multi_sqlite(self):
        with preserve.open_multi("sqlite", filename=":memory:") as db:
            db["t"]["n"] = 99
            assert db["t"]["n"] == 99

    def test_open_multi_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown multi-connector format"):
            preserve.open_multi("nonexistent_backend")

    def test_open_multi_returns_multi_connector(self):
        db = preserve.open_multi("memory")
        assert isinstance(db, MultiConnector)
        db.close()

    def test_multi_connectors_list(self):
        mcs = preserve.multi_connectors()
        schemes = {c.scheme() for c in mcs}
        assert "memory" in schemes
        assert "sqlite" in schemes


class TestPreserveFromUriMulti:
    def test_from_uri_multi_memory(self):
        with preserve.from_uri_multi("memory://") as db:
            db["col"]["key"] = "val"
            assert db["col"]["key"] == "val"

    def test_from_uri_multi_sqlite(self, tmp_path):
        path = str(tmp_path / "m.db")
        with preserve.from_uri_multi(f"sqlite://{path}") as db:
            db["tab"]["item"] = 7
            assert db["tab"]["item"] == 7

    def test_from_uri_multi_no_scheme_raises(self):
        with pytest.raises(ValueError, match="Invalid URI"):
            preserve.from_uri_multi("no-scheme")

    def test_from_uri_multi_unknown_scheme_raises(self):
        with pytest.raises(ValueError, match="Unknown multi-connector scheme"):
            preserve.from_uri_multi("bogus://host/db")


class TestPreserveRegister:
    def test_register_custom_connector(self):
        """A connector registered at runtime is immediately usable via open()."""
        from preserve.connectors.memory import Memory

        p = Preserve()
        p.register("custom_mem", Memory)
        assert p.is_registered("custom_mem", Memory)

        db = p.open("custom_mem")
        db["x"] = 42
        assert db["x"] == 42
        db.close()

    def test_register_non_connector_raises(self):
        p = Preserve()
        with pytest.raises(TypeError):
            p.register("bad", object)  # type: ignore[arg-type]

    def test_is_registered_false_for_unknown(self):
        from preserve.connectors.memory import Memory

        p = Preserve()
        assert not p.is_registered("memory", Memory)  # not yet discovered

    def test_reset_clears_registry(self):
        preserve.open("memory").close()  # trigger discovery
        _reset()
        # After reset, registry is empty — open() must re-discover
        with preserve.open("memory") as db:
            db["k"] = 1  # should still work (re-discovery happens automatically)
