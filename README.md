# ds-protocol-graphql-py-lib

![Python Versions](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13%20|%203.14-blue)
[![PyPI version](https://badge.fury.io/py/ds-protocol-graphql-py-lib.svg?kill_cache=1)](https://badge.fury.io/py/ds-protocol-graphql-py-lib)
[![Build Status](https://github.com/grasp-labs/ds-protocol-graphql-py-lib/actions/workflows/build.yaml/badge.svg)](https://github.com/grasp-labs/ds-protocol-graphql-py-lib/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/grasp-labs/ds-protocol-graphql-py-lib/graph/badge.svg?token=EO3YCNCZFS)](https://codecov.io/gh/grasp-labs/ds-protocol-graphql-py-lib)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

DS package for ds-protocol-graphql-py-lib

## Quick Start

### Quick Setup

```shell
# 1. Install dependencies
uv sync --all-extras --dev

# 2. Install pre-commit hooks
uv run pre-commit install

# 3. Verify setup
make test
```

## Development

### Available Commands

Use the Makefile for all development tasks:

```shell
# Show all available commands
make help

# Code Quality
make lint           # Check code quality with ruff
make format         # Format code with ruff
make type-check     # Run mypy type checking
make security-check # Run security checks with bandit

# Testing
make test          # Run tests
make test-cov      # Run tests with coverage (requires 95%)

# Build and Publish
make build         # Build package
make docs          # Build documentation
make publish-test  # Upload to TestPyPI
make publish       # Upload to PyPI
```

### Version Management

```shell
# Show current version
make version

# Tag and release
make tag           # Create git tag and push (triggers release)
```

> **⚠️ Warning**: The `make tag` command will create a git tag and
> push it to the remote repository, which may trigger automated
> releases. Ensure you have updated `pyproject.toml` with the new version
> and committed all changes before running this command.

### Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality:

```shell
install pre-commit
```

### Building Documentation

```shell
# Build documentation
make docs

# View documentation (macOS)
open docs/build/html/index.html

# View documentation (Linux)
xdg-open docs/build/html/index.html
```

### Testing

```shell
# Run basic tests
make test

# Run tests with coverage (requires 95% coverage)
make test-cov

# Run specific test file
uv run pytest tests/test_example.py -v
```

## Project Structure

```text
.
├── .config/                   # Configuration tooling files
├── .github/
│   ├── workflows/            # CI/CD workflows
│   └── CODEOWNERS            # Code ownership file
├── src/
│   └── ds_protocol_graphql_py_lib/     # Rename to your module name
│       └── __init__.py
├── .pre-commit-config.yaml   # Pre-commit hooks configuration
├── tests/                    # Test files
├── docs/                     # Sphinx documentation
├── LICENSE-APACHE            # License file
├── pyproject.toml            # Project configuration
├── Makefile                  # Development commands
├── codecov.yaml              # Codecov configuration
├── CONTRIBUTING.md           # Contribution guidelines
├── PyPI.md                   # PyPI publishing guide
├── README.md                 # This file
```

## Features

- **Modern Python Tooling**: Uses `uv` for fast dependency management
- **Type Safety**: Strict mypy configuration with full type hints
- **Code Quality**: Ruff for linting and formatting
- **Testing**: Pytest with 95% coverage requirement
- **Documentation**: Sphinx with autoapi for automatic API docs
- **CI/CD**: GitHub Actions for testing, building, and publishing
- **Pre-commit Hooks**: Automated code quality checks
- **Docker Support**: Containerized build environment

## Pagination and incremental reads

`GraphqlDataset.read()` sends one request by default. Set `read.pagination` to
fetch every page of a Relay-style cursor connection inside the same `read()`,
and add `read.incremental` to read only rows after a stored watermark.

```python
GraphqlReadSettings(
    query="""
    query read($first: Int!, $after: String, $since: Int = 0) {
      customers(first: $first, after: $after, filter: { dbId_gt: $since }) {
        pageInfo { hasNextPage }
        edges { cursor node { dbId description } }
      }
    }
    """,
    pagination=PaginationSettings(
        strategy=PaginationStrategy.CURSOR,
        items_path="data.customers.edges",  # page records = output rows
        max_pages=None,                     # optional limit per read()
        cursor=CursorPaginationSettings(
            # last edge's cursor; with pageInfo.endCursor point there instead
            cursor_path="data.customers.edges.-1.cursor",
            has_next_path="data.customers.pageInfo.hasNextPage",
            cursor_variable="after",
            page_size_variable="first",
            page_size=100,
        ),
    ),
    incremental=IncrementalSettings(variable="since", watermark_path="dbId"),
)
```

- **Output rows** come from `items_path`. A list under `edges` is read as Relay
  edges: each row is the node's fields plus the edge's own fields, without
  `cursor`; edges with a `null` node are skipped.
- **Checkpoint.** `dataset.checkpoint` holds the state between runs; the caller
  stores and restores it:
  - `checkpoint["pagination"]`: the cursor to continue from after a failed
    read. Removed when a read completes.
  - `checkpoint["incremental"]["watermark"]`: sent as `$since` on the next
    read. Moves only after a complete read, to the highest value read so far,
    and never backwards.
- **Failures and resume.** A failed read keeps the pages it fetched in `output`
  and leaves the watermark unchanged. The next `read()` continues after the
  saved cursor with the same `$since`, so no page is skipped or read twice.
- **`max_pages`** limits the pages fetched by one `read()`. Hitting it raises
  `ReadError` and keeps the cursor; the next read fetches the next batch.
- **Incremental requires pagination**; settings with `incremental` but no
  `pagination` are rejected.
- **First run:** no watermark is sent yet, so give the variable a default in
  the query (`$since: Int = 0`) when the API treats a `null` bound as "match
  nothing".

Full examples:

- [Xledger](examples/08_paginate_incremental_xledger.py)
- [Visma Business NXT](examples/09_paginate_incremental_visma_bnxt.py)

## Requirements

- Python 3.9+
- [uv](https://github.com/astral-sh/uv) package manager
- Make (for development commands)

## Documentation

- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [PyPI.md](PyPI.md) - PyPI publishing guide
- [README.md](README.md) - This file

## License

This package is licensed under the Apache License 2.0.
See [LICENSE-APACHE](LICENSE-APACHE) for details.
