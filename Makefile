SHELL := /bin/bash

COMPOSE ?= docker compose

# Default; user can override: make up ACQUIRIUM_RECREATE=true
ACQUIRIUM_RECREATE ?= false
ACQUIRIUM_HOST ?= localhost
ACQUIRIUM_PORT ?= 8000
ACQUIRIUM_HEALTH_URL := http://$(ACQUIRIUM_HOST):$(ACQUIRIUM_PORT)/health
ACQUIRIUM_HEALTH_TIMEOUT_SEC ?= 180
ACQUIRIUM_HEALTH_SLEEP_SEC ?= 2
# Optional flag: make up RECREATE=true
ifeq ($(RECREATE),true)
ACQUIRIUM_RECREATE := true
endif

export ACQUIRIUM_RECREATE

.PHONY: up rebuild down test watertap-up watertap-down logs ps

up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) up -d --build
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
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) build --no-cache
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) up -d --force-recreate

down:
	$(COMPOSE) down --remove-orphans

# Always enable for test; always tear down even on failure
test: ACQUIRIUM_RECREATE := true
test:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile test up -d --build; \
	uv run pytest tests; \
	$(MAKE) testing-down

testing-up: ACQUIRIUM_RECREATE := true
testing-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile test up -d --build	


testing-down:
	$(COMPOSE) --profile test down --remove-orphans

watertap-up: ACQUIRIUM_RECREATE := true
watertap-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile watertap_simulation up -d --build

watertap-down:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile watertap_simulation down --remove-orphans

benicia-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile benicia_simulation up -d --build

benicia-down:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile benicia_simulation down --remove-orphans

watertap-gui-up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile watertap_gui up -d --build

watertap-gui-down:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) --profile watertap_gui down --remove-orphans