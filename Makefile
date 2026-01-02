SHELL := /bin/bash

COMPOSE ?= docker compose

# Default; user can override: make up ACQUIRIUM_RECREATE=true
ACQUIRIUM_RECREATE ?= false

# Optional flag: make up RECREATE=true
ifeq ($(RECREATE),true)
ACQUIRIUM_RECREATE := true
endif

export ACQUIRIUM_RECREATE

.PHONY: up rebuild down test watertap-up watertap-down logs ps

up:
	ACQUIRIUM_RECREATE=$(ACQUIRIUM_RECREATE) $(COMPOSE) up -d --build

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
