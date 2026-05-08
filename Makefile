SHELL := /bin/bash

COMPOSE ?= docker compose
TEST_COMPOSE ?= COMPOSE_PROJECT_NAME=acquirium_test $(COMPOSE) -f compose.testing.yaml

# Default; user can override: make up ACQUIRIUM_RECREATE=true
ACQUIRIUM_RECREATE ?= false
ACQUIRIUM_HOST ?= localhost
ACQUIRIUM_PORT ?= 8000
ACQUIRIUM_HOST_PORT ?= $(ACQUIRIUM_PORT)
ACQUIRIUM_HEALTH_URL := http://$(ACQUIRIUM_HOST):$(ACQUIRIUM_PORT)/health
ACQUIRIUM_HEALTH_TIMEOUT_SEC ?= 180
ACQUIRIUM_HEALTH_SLEEP_SEC ?= 2
# Optional flag: make up RECREATE=true
ifeq ($(RECREATE),true)
ACQUIRIUM_RECREATE := true
endif

export ACQUIRIUM_RECREATE

# Config file mounted into the acquirium container; override per-target for testing.
ACQUIRIUM_CONFIG_FILE ?= acquirium.toml
export ACQUIRIUM_CONFIG_FILE

POSTGRES_DB ?= acquirium
POSTGRES_HOST_PORT ?= 5432
PG_DSN ?= postgresql://acquirium:acquirium@timescaledb:5432/$(POSTGRES_DB)
TSDB_VOLUME ?= acquirium_tsdb_data
MQTT_HOST_PORT ?= 1883
ACQUIRIUM_APP_NETWORK ?= acquirium_acquirium_net

export POSTGRES_DB
export POSTGRES_HOST_PORT
export PG_DSN
export TSDB_VOLUME
export MQTT_HOST_PORT
export ACQUIRIUM_HOST_PORT
export ACQUIRIUM_APP_NETWORK

TEST_PYTEST_ENV := ACQUIRIUM_TEST_SERVER_HOST=localhost ACQUIRIUM_TEST_SERVER_PORT=8010 ACQUIRIUM_TEST_PG_DSN=postgresql://acquirium:acquirium@localhost:55432/acquirium_test

.PHONY: up rebuild down no-server-up no-server-down test testing-up testing-down watertap-up watertap-down logs ps

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

# Infrastructure-only mode for running Acquirium externally on the host.
# Starts TimescaleDB and Grafana, but not the Acquirium server container.
no-server-up:
	$(COMPOSE) --profile no-server up -d timescaledb grafana

no-server-down:
	$(COMPOSE) --profile no-server rm -sf grafana timescaledb

# Always tear down even on failure
test:
	uv sync --locked --all-extras
	status=0; \
	$(TEST_COMPOSE) --profile server --profile test rm -sf timescaledb acquirium mosquitto testing_service >/dev/null 2>&1 || true; \
	$(TEST_COMPOSE) --profile server --profile test up -d --build || status=$$?; \
	if [ $$status -eq 0 ]; then $(TEST_PYTEST_ENV) uv run pytest tests || status=$$?; fi; \
	$(MAKE) testing-down; \
	exit $$status

testing-up:
	$(TEST_COMPOSE) --profile server --profile test rm -sf timescaledb acquirium mosquitto testing_service >/dev/null 2>&1 || true
	$(TEST_COMPOSE) --profile server --profile test up -d --build


testing-down:
	$(TEST_COMPOSE) --profile server --profile test down --remove-orphans

watertap-up: ACQUIRIUM_RECREATE := true
watertap-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server --profile watertap_simulation up -d --build

watertap-down:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server --profile watertap_simulation down --remove-orphans

benicia-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server --profile benicia_simulation up -d --build

benicia-down:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server --profile benicia_simulation down --remove-orphans

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

watertap-gui-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server --profile watertap_gui up -d --build

watertap-gui-down:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile server --profile watertap_gui down --remove-orphans
