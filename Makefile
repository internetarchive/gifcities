SHELL = /bin/bash
.SHELLFLAGS = -o pipefail -c

.PHONY: help
help: ## Print info about all commands
	@echo "Commands:"
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[01;32m%-20s\033[0m %s\n", $$1, $$2}'

.venv:
	python3 -mvenv .venv
	.venv/bin/pip install uv==0.2.34
	.venv/bin/uv pip install -e .[dev]

.PHONY: dep
dep: .venv ## Install dependencies using pip install -e to .venv

.PHONY: freeze
freeze: dep
	.venv/bin/uv pip compile --generate-hashes -o requirements.txt pyproject.toml
	.venv/bin/uv pip compile --generate-hashes --extra dev -o dev.requirements.txt pyproject.toml
	#.venv/bin/uv pip compile --generate-hashes --extra ci -o ci.requirements.txt pyproject.toml

.PHONY: audit
audit: dep
	.venv/bin/pip-audit

.PHONY: lint
lint: dep ## Run ruff check and mypy
	.venv/bin/ruff check src/gifcities/ tests/
	.venv/bin/mypy src/gifcities/ tests/ --ignore-missing-imports --disable-error-code call-arg --disable-error-code arg-type --disable-error-code assignment

.PHONY: fmt
fmt: dep ## Run ruff format on all source code
	.venv/bin/ruff format src/gifcities tests/

.PHONY: test
test: dep ## Run all tests and lints
	.venv/bin/pytest

.PHONY: coverage
coverage: dep ## Run all tests with coverage
	.venv/bin/pytest --cov --cov-report=term --cov-report=html

.PHONY: serve
serve: dep ## Run web service locally, with reloading
	ENV_FOR_DYNACONF=development .venv/bin/uvicorn gifcities.web:app --reload --port 9829

.PHONY: clean
clean: ## Clean cached files
	find src -type d -name "__pycache__" -exec rm -rf {} \;
	find src -type d -name "*.egg-info" -exec rm -rf {} \;
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
