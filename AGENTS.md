# Repository Guidelines

## Project Structure & Module Organization

Acquirium is a Python package under `src/acquirium`. Core areas include `Client/` for user-facing API helpers, `Server/` for FastAPI endpoints and manager logic, `Storage/` for graph and timeseries backends, `BuiltinDrivers/` for built-in ingest drivers, and `TextMatch/` for ontology matching. Tests live in `tests/`, with service-free unit tests in `tests/unit/` and integration-style tests alongside shared fixtures in `tests/conftest.py`. Runtime configuration and compose files are at the repository root, including `Makefile`, `compose.yaml`, and `compose.testing.yaml`. Documentation lives in `docs/`.

## Build, Test, and Development Commands

- `uv sync --locked --all-extras`: install the locked development environment.
- `uv run acquirium server`: run the server locally when external services are already available.
- `make up`: build and start the full server stack.
- `make down`: stop the local stack.
- `make testing-up`: start the isolated testing stack.
- `make testing-down`: stop the testing stack.
- `make unit-test`: run unit tests only.
- `make integration-test`: start test services and run non-unit tests.
- `make test`: run the full compose-backed test workflow.

Use `COMPOSE="podman compose"` or `make testing-up COMPOSE="podman compose"` when using Podman instead of Docker.

## Coding Style & Naming Conventions

Use Python 3.12 syntax, four-space indentation, and type hints for public interfaces and nontrivial data structures. Keep modules focused on existing package boundaries rather than adding broad utility layers. Prefer clear snake_case names for functions, variables, files, and test functions. Built-in drivers should live in `src/acquirium/BuiltinDrivers/` and expose explicit driver classes.

## Testing Guidelines

Tests use `pytest`. Name files `test_*.py` and tests `test_*`. Unit tests should not require external services. Integration tests may assume the testing compose stack and should read host/port settings from `tests/conftest.py` environment variables. Add focused tests for storage, API, and driver behavior when changing ingestion, stream registration, or query semantics.

## Commit & Pull Request Guidelines

Recent commits use short imperative summaries, for example `Fix stale doc references` or `Guard insert_graph path check against ENAMETOOLONG`. Keep commits scoped and avoid mixing unrelated infrastructure, docs, and feature changes. PRs should describe behavior changes, list test commands run, call out migration or configuration effects, and include screenshots only for UI changes.

## Security & Configuration Tips

Do not commit real database DSNs, MQTT credentials, generated caches, or local test output. Prefer environment variables such as `PG_DSN`, `ACQUIRIUM_TEST_PG_DSN`, `ACQUIRIUM_TEST_SERVER_HOST`, and `ACQUIRIUM_TEST_SERVER_PORT` for local configuration.
