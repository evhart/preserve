# CHANGELOG

<!-- version list -->

## v2.0.0 (2026-03-16)

### Bug Fixes

- Python 3.9 compatibility
  ([`665ae40`](https://github.com/evhart/preserve/commit/665ae40e393533ffbd694618efbee14b896734a7))

- Use python -m build in semantic-release build command
  ([`f4d1b86`](https://github.com/evhart/preserve/commit/f4d1b8638f2d318bc5ce6a8010a83a456823ff0c))

### Features

- Multi-connector API, coercion system, cache decorator, CI tests
  ([`033b438`](https://github.com/evhart/preserve/commit/033b438955135051a6eb35a1345af999b3dbe169))

### Breaking Changes

- Connector base class gains key_types, default_key_type and default_value_type fields; coercion API
  replaces the previous per-call type argument. cache.py is a new module re-exported from the
  top-level package. CI now requires tests to pass before a release is cut.


## v0.2.0 (2025-02-17)

### Features

* Update libraries and Python version.
  ([ `b35e35d` ](https://github.com/evhart/preserve/commit/b35e35dc1bb1e6683dc5dbd95702685dcc92e5a4))

Update libraries and python version. Pydantic v2 is now used instead of Pydantic v1.

fix(shelf): Partial fix for URI parsing for the Shelf connector.
