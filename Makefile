SHELL := /bin/bash

COMPOSE ?= docker compose
TEST_COMPOSE ?= COMPOSE_PROJECT_NAME=acquirium_test $(COMPOSE) -f compose.testing.yaml

# Default; user can override: make up ACQUIRIUM_RECREATE=true
ACQUIRIUM_RECREATE ?= false
ACQUIRIUM_HOST ?= localhost
ACQUIRIUM_PORT ?= 8000
ACQUIRIUM_HOST_PORT ?= $(ACQUIRIUM_PORT)
ACQUIRIUM_HEALTH_URL := http://$(ACQUIRIUM_HOST):$(ACQUIRIUM_PORT)/health
ACQUIRIUM_HEALTH_TIMEOUT_SEC ?= 600
ACQUIRIUM_HEALTH_SLEEP_SEC ?= 2
# Optional flag: make up RECREATE=true
ifeq ($(RECREATE),true)
ACQUIRIUM_RECREATE := true
endif

export ACQUIRIUM_RECREATE

# Config file mounted into the acquirium container; override per-target for testing.
ACQUIRIUM_CONFIG_FILE ?= acquirium.toml
export ACQUIRIUM_CONFIG_FILE

MQTT_HOST_PORT ?= 1883
export MQTT_HOST_PORT
export ACQUIRIUM_HOST_PORT
TEST_PYTEST_ENV := ACQUIRIUM_TEST_SERVER_HOST=localhost ACQUIRIUM_TEST_SERVER_PORT=8010
# Number of slow test cases reported by `make test-timing`. Override with
# `make test-timing PYTEST_DURATIONS=0` to list every test duration.
PYTEST_DURATIONS ?= 20

.PHONY: up rebuild down no-server-up no-server-down test test-timing testing-up testing-down watertap-up watertap-down logs ps

up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server up -d --build
	@$(MAKE) wait-health

wait-health:
	@echo "Waiting for Acquirium server health check: $(ACQUIRIUM_HEALTH_URL)"
	@deadline=$$(( $$(date +%s) + $(ACQUIRIUM_HEALTH_TIMEOUT_SEC) )); \
	while true; do \
		if curl -fsS "$(ACQUIRIUM_HEALTH_URL)" >/dev/null 2>&1; then \
			echo "Acquirium server is ready."; \
			break; \
		fi; \
		if [ $$(date +%s) -ge $$deadline ]; then \
			echo "ERROR: Acquirium server did not become ready within $(ACQUIRIUM_HEALTH_TIMEOUT_SEC)s"; \
			echo "Tip: check logs with: $(COMPOSE) logs -f acquirium_server"; \
			exit 1; \
		fi; \
		sleep $(ACQUIRIUM_HEALTH_SLEEP_SEC); \
	done
# Always enable for rebuild
rebuild: ACQUIRIUM_RECREATE := true
rebuild:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server build --no-cache
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server up -d --force-recreate

down:
	$(COMPOSE) down --remove-orphans

# Core tests need no external database or container services.
test:
	uv sync --locked --extra mqtt
	uv run pytest tests/unit tests/integration/test_native_runtime.py tests/integration/test_client_and_drivers.py tests/integration/test_soft_sensors.py tests/integration/test_streaming_workload.py tests/integration/test_execution_profiling.py tests/integration/test_experiment_models.py tests/test_experiment_*.py tests/test_schema_migrations.py

test-timing:
	uv sync --locked --extra mqtt
	uv run pytest tests/unit tests/integration/test_native_runtime.py tests/integration/test_client_and_drivers.py tests/integration/test_soft_sensors.py tests/integration/test_streaming_workload.py tests/integration/test_execution_profiling.py tests/integration/test_experiment_models.py tests/test_experiment_*.py tests/test_schema_migrations.py --durations=$(PYTEST_DURATIONS)

testing-up:
	$(TEST_COMPOSE) --profile server --profile test rm -sf acquirium mosquitto >/dev/null 2>&1 || true
	$(TEST_COMPOSE) --profile server --profile test up -d --build


testing-down:
	$(TEST_COMPOSE) --profile server --profile test down --remove-orphans

unit-test:
	uv run pytest tests/unit/ -v --tb=short

unit-test-cov:
	uv run pytest tests/unit/ -v --tb=short --cov=acquirium --cov-report=term-missing

integration-test:
	$(MAKE) testing-up
	$(MAKE) wait-health ACQUIRIUM_PORT=8010
	status=0; \
	$(TEST_PYTEST_ENV) uv run pytest tests/ -v --tb=short --ignore=tests/unit || status=$$?; \
	$(MAKE) testing-down; \
	exit $$status
