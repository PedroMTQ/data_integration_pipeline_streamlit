SHELL := /bin/bash
.ONESHELL:
MAKEFLAGS += --no-print-directory

.PHONY: install activate update test format check-uv audit docs docs-serve test-connections docker-up docker-down docker-reset streamlit airflow-import

DOCS_DIR := tmp/audits/gx/uncommitted/data_docs/local_site
DOCS_PORT := 8080
DEV_ENV_FILE := config/env_files/dev.env
COMPOSE_FILES := -f docker-compose-es.yaml -f docker-compose-minio.yaml -f docker-compose-postgres.yaml -f docker-compose-airflow.yaml
COMPOSE_INFRA_FILES := -f docker-compose-es.yaml -f docker-compose-minio.yaml -f docker-compose-postgres.yaml
BUILD_COMPOSE_FILES := -f docker-compose-build.yaml  -f docker-compose-airflow.yaml
MKDOCS_DEPS := --with mkdocs --with mkdocs-material --with mkdocs-mermaid-zoom
MERMAID_JS := docs/assets/javascripts/mermaid.min.js
MERMAID_VERSION := 10.7.0
$(MERMAID_JS):
	@mkdir -p $$(dirname $(MERMAID_JS))
	curl -sL "https://cdn.jsdelivr.net/npm/mermaid@$(MERMAID_VERSION)/dist/mermaid.min.js" -o $(MERMAID_JS)


# This block defines the "dictionary" logic once.
# We use $$ everywhere so Make passes the dollar signs to the Bash shell.
define SETUP_ENV_VARS
	declare -A ENV
	ENV["PACKAGE"]=$$(grep -m 1 'name' pyproject.toml | sed -E 's/name = "(.*)"/\1/')
	ENV["PYTHON_VERSION"]=$$(grep -m 1 'python' pyproject.toml | sed -nE 's/.*[~^]=?([0-9]+\.[0-9]+).*/\1/p')
	ENV["ENV_DIR"]="${HOME}/envs/$${ENV["PACKAGE"]}"
	ENV["ENV_PATH"]="$${ENV["ENV_DIR"]}/bin/activate"
endef


# --- 0. Requirement Check ---
check-uv:
	@command -v uv >/dev/null 2>&1 || { \
		echo >&2 "Error: 'uv' is not installed."; \
		echo >&2 "Please install it via: wget -qO- https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	}

install:
	@$(SETUP_ENV_VARS)
	if [ ! -d "$${ENV["ENV_DIR"]}" ]; then
		echo "Installing environment for $${ENV["PACKAGE"]}..."
		uv venv "$${ENV["ENV_DIR"]}" --python "$${ENV["PYTHON_VERSION"]}"
		source scripts/load_dot_env_dev.sh 2>/dev/null || true
		ER_EXTRA="er-$${ER_BACKEND_ENGINE:-duckdb}"
		echo "Using ER engine: $$ER_EXTRA"
		source "$${ENV["ENV_PATH"]}" && uv sync --active --extra models --extra auditor --extra storage --extra embedding --extra "$$ER_EXTRA" --extra streamlit
		echo "Install complete. Use 'make activate' to activate it"
	else
		echo "Environment for $${ENV["PACKAGE"]} already exists at $${ENV["ENV_DIR"]}. Use 'make activate' to activate it, or 'make update' to update it."
	fi
	unset ENV

activate:
	@if [ "$$IN_MAKE_SHELL" = "true" ]; then
		echo "Already inside a sub-shell, either exit the sub-shell with 'exit'/ctrl+d or start a new terminal session."
		exit 0
	fi
	$(SETUP_ENV_VARS)
	if [ ! -f "$${ENV["ENV_PATH"]}" ]; then
		echo "Error: Environment not found. Run 'make install' first."
		exit 1
	fi
	echo "--- Entering $${ENV["PACKAGE"]} environment and sourcing .env (type 'exit' to leave) ---"
	bash --rcfile <(echo "source ~/.bashrc; source scripts/load_dot_env_dev.sh; source $${ENV["ENV_PATH"]}; export IN_MAKE_SHELL=true")
	unset ENV

update:
	@$(SETUP_ENV_VARS)
	if [ ! -f "$${ENV["ENV_PATH"]}" ]; then
		echo "Error: Environment not found. Run 'make install' first."
		exit 1
	fi
	echo "Updating environment for $${ENV["PACKAGE"]}..."
	rm uv.lock
	uv venv  --clear "$${ENV["ENV_DIR"]}" --python "$${ENV["PYTHON_VERSION"]}"
	source "$${ENV["ENV_PATH"]}" && uv sync --active --all-extras
	unset ENV
	echo "Update complete."


test:
	@echo "Running tests via uv..."
	@$(SETUP_ENV_VARS)
	bash --rcfile <(echo "source ~/.bashrc; source scripts/load_dot_env_dev.sh; source $${ENV["ENV_PATH"]}; export IN_MAKE_SHELL=true") \
	-c "uv run --all-extras --group tests pytest -q"
	unset ENV

format:
	@echo "Running ruff formatter/linter via temporary uv environment..."
	uv run --with ruff ruff check . --fix
	uv run --with ruff ruff format .


docs: $(MERMAID_JS)
	@echo "Building documentation..."
	uv run $(MKDOCS_DEPS) mkdocs build

docs-serve: $(MERMAID_JS)
	@echo "Serving documentation at http://localhost:8000 ..."
	uv run $(MKDOCS_DEPS) mkdocs serve

test-connections:
	@$(SETUP_ENV_VARS)
	source scripts/load_dot_env_dev.sh
	source "$${ENV["ENV_PATH"]}"
	python scripts/test_connections.py


docker-build:
	@echo "Building Airflow + Repo image..."
	if ! docker compose --env-file $(DEV_ENV_FILE) $(BUILD_COMPOSE_FILES) build; then
		echo "docker compose build failed"
		exit 1
	fi

docker-up:
	@echo "Starting Airflow + ES + MinIO + Postgres..."
	if ! docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) up -d --remove-orphans; then
		echo "docker compose up failed. Printing setup container logs..." >&2
		docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) logs --no-color setup || true
		exit 1
	fi


docker-down:
	@echo "Stopping Airflow + ES + MinIO + Postgres..."
	docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) down

docker-reset:
	@echo "Stopping and removing all project containers, networks, and named volumes..."
	docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) down -v --remove-orphans --timeout 10
	rm -rf tmp/compose_init/sentinel_*
	@echo "Docker reset complete. Run 'make docker-up' for a clean start."


# these are helper functions for dev 
docker-infra-up:
	@echo "Starting ES + MinIO + Postgres..."
	if ! docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_INFRA_FILES) up -d --remove-orphans; then
		echo "docker compose up failed. Printing setup container logs..." >&2
		docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_INFRA_FILES) logs --no-color setup || true
		exit 1
	fi

airflow-import:
	@echo "Importing Airflow connections and variables (config/*.json) via scheduler..."
	@for i in $$(seq 1 60); do \
		if docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) exec -T airflow-scheduler airflow version >/dev/null 2>&1; then \
			echo "Scheduler is ready (attempt $$i)."; \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then echo "Timed out waiting for airflow-scheduler."; exit 1; fi; \
		sleep 2; \
	done
	docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) exec -T airflow-scheduler airflow connections import /opt/airflow/config/airflow_connections.json --overwrite
	docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) exec -T airflow-scheduler airflow variables import /opt/airflow/config/airflow_vars.json -a overwrite
	docker compose --env-file $(DEV_ENV_FILE) $(COMPOSE_FILES) exec -T airflow-scheduler airflow variables import /opt/airflow/config/dip_vars.json -a overwrite
	@echo "Airflow connections and variables import finished."
