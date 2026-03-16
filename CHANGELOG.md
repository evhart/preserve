# CHANGELOG

<!-- version list -->

## v2.0.1 (2026-03-16)

### Bug Fixes

- Trigger release to publish dist artifacts and PyPI upload
  ([`f16ce25`](https://github.com/evhart/preserve/commit/f16ce25ca2d6a99f83cfacfb342a344f6fcc58f0))

### Continuous Integration

- Publish dist to GitHub Releases and PyPI after release
  ([`c907b6e`](https://github.com/evhart/preserve/commit/c907b6e25177311a835b66e632d08db4ecf79326))

### Documentation

- Add badges, remove alpha disclaimer, drop unused mypy.ini
  ([`e481314`](https://github.com/evhart/preserve/commit/e48131422394965725390f3c67dc2cfd495e11a8))


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
