"""Shared pytest fixtures for the preserve test suite."""

from __future__ import annotations

import pytest

from preserve.preserve import _reset


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset the preserve connector registry before each test.

    This prevents entry-point discovery state from leaking between tests and
    ensures tests that manipulate the registry start with a clean slate.
    """
    # Re-discovery happens on the first call to open/from_uri inside each test.
    _reset()
    yield
    _reset()


@pytest.fixture()
def tmp_sqlite(tmp_path):
    """Return a path to a temporary SQLite file that is cleaned up after the test."""
    return str(tmp_path / "test.db")


@pytest.fixture()
def tmp_shelf(tmp_path):
    """Return a path to a temporary shelf file that is cleaned up after the test."""
    return str(tmp_path / "test_shelf")
