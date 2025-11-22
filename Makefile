SHELL := /bin/bash
# =============================================================================
# Variables
# =============================================================================

.DEFAULT_GOAL:=help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory

# Define colors and formatting
BLUE := $(shell printf "\033[1;34m")
GREEN := $(shell printf "\033[1;32m")
RED := $(shell printf "\033[1;31m")
YELLOW := $(shell printf "\033[1;33m")
NC := $(shell printf "\033[0m")
INFO := $(shell printf "$(BLUE)ℹ$(NC)")
OK := $(shell printf "$(GREEN)✓$(NC)")
WARN := $(shell printf "$(YELLOW)⚠$(NC)")
ERROR := $(shell printf "$(RED)✖$(NC)")

.PHONY: help
help:				## Display this help text for Makefile
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# =============================================================================
# Developer Utils
# =============================================================================

.PHONY: sync
sync:					## Sync dependencies
	@uv sync --dev

.PHONY: install
install: destroy clean				## Install dependencies
	@echo "${INFO} Starting fresh installation..."
	@if uv venv -p 3.12; then \
		echo "${OK} Virtual environment created ✨"; \
	else \
		echo "${ERROR} Failed to create virtual environment ❌" >&2; \
		exit 1; \
	fi
	@if [ -f uv.lock ]; then \
		echo "${INFO} uv.lock found, syncing in locked mode... 🔒"; \
		uv sync --dev --locked; \
	else \
		echo "${INFO} uv.lock not found, syncing without lock... 🚀"; \
		make sync; \
	fi
	@echo "${OK} Installation complete! 🎉"

.PHONY: lock
lock:					## Rebuild lockfiles from scratch, updating all dependencies
	@echo "${INFO} Rebuilding lockfiles from scratch... 🔄"
	@if uv lock --upgrade; then \
		echo "${OK} Lockfiles rebuilt and updated to latest versions ✨"; \
	else \
		echo "${ERROR} Failed to rebuild lockfiles ❌" >&2; \
		exit 1; \
	fi

.PHONY: upgrade
upgrade:       					## Upgrade all dependencies to the latest stable versions
	@echo "${INFO} Updating all dependencies... 🔄"
	$(MAKE) lock
	$(MAKE) sync
	@echo "${INFO} Updating pre-commit hooks 🔄"
	@if uv run pre-commit autoupdate; then \
		echo "${OK} Pre-commit hooks updated ✨"; \
	else \
		echo "${ERROR} Failed to update pre-commit hooks ❌" >&2; \
		exit 1; \
	fi

.PHONY: clean
clean:					## Cleanup temporary build artifacts
	@echo "${INFO} Cleaning working directory... 🧹"
	@rm -rf .pytest_cache tests/.pytest_cache tests/**/.pytest_cache
	@rm -rf .ruff_cache .mypy_cache .hypothesis build/ dist/ .eggs/
	@rm -rf .coverage coverage.xml coverage.json htmlcov/
	@rm -rf .unasyncd_cache/ .auto_pytabs_cache node_modules
	@find . -name '*.egg-info' -exec rm -rf {} +
	@find . -type f -name '*.egg' -exec rm -f {} +
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '*~' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -rf {} +
	@find . -name '.ipynb_checkpoints' -exec rm -rf {} +
	$(MAKE) docs-clean
	@echo "${OK} Working directory cleaned ✨"

.PHONY: destroy
destroy:					## Destroy the virtual environment
	@echo "${INFO} Destroying virtual environment... 🗑️"
	@rm -rf .venv
	@echo "${OK} Virtual environment destroyed 🗑️"

# =============================================================================
# Tests, Linting, Coverage
# =============================================================================

.PHONY: mypy
mypy:					## Run mypy
	@echo "${INFO} Running mypy... 🔍"
	@if uv run dmypy run; then \
		echo "${OK} Mypy checks passed ✨"; \
	else \
		echo "${ERROR} Mypy checks failed ❌" >&2; \
		exit 1; \
	fi

.PHONY: mypy-nocache
mypy-nocache:					## Run Mypy without cache
	@echo "${INFO} Running mypy without cache... 🔍"
	@if uv run dmypy run -- --cache-dir=/dev/null; then \
		echo "${OK} Mypy checks completed successfully ✨"; \
	else \
		echo "${ERROR} Mypy checks failed ❌" >&2; \
		exit 1; \
	fi

.PHONY: pyright
pyright:					## Run pyright
	@echo "${INFO} Running Pyright... 🔍"
	@if uv run pyright; then \
		echo "${OK} Pyright checks passed ✨"; \
	else \
		echo "${ERROR} Pyright checks failed ❌" >&2; \
		exit 1; \
	fi

.PHONY: type-check
type-check: mypy pyright				## Run all type checking

.PHONY: pre-commit
pre-commit: 					## Runs pre-commit hooks; includes ruff formatting and linting, codespell
	@echo "${INFO} Running pre-commit hooks... 🛠️"
	@if uv run pre-commit run; then \
		echo "${OK} Pre-commit hooks passed ✨"; \
	else \
		echo "${ERROR} Pre-commit hooks failed ❌" >&2; \
		exit 1; \
	fi

.PHONY: slots-check
slots-check: 					## Check for slots usage in classes
	@echo "${INFO} Checking for slots usage in classes"
	@if uv run slotscheck src; then \
		echo "${OK} Slots check passed ✨"; \
	else \
		echo "${ERROR} Slots check failed: missing or incorrect __slots__ usage ❌" >&2; \
		exit 1; \
	fi

.PHONY: lint
lint: pre-commit type-check slots-check			## Run all linting

.PHONY: coverage
coverage:  					## Run the tests and generate coverage report
	@echo "${INFO} Running tests with coverage... 🧪"
	@if uv run pytest tests --cov -n auto; then \
		echo "${OK} Tests passed with coverage ✨"; \
	else \
		echo "${ERROR} Tests failed during coverage run ❌" >&2; \
		exit 1; \
	fi
	@echo "${INFO} Generating coverage reports... 📊"
	@uv run coverage html
	@uv run coverage xml
	@echo "${OK} Coverage report generated in html/ and coverage.xml ✨"

.PHONY: test
test:  					## Run the tests
	@echo "${INFO} Running test cases... 🧪"
	@if uv run pytest tests; then \
		echo "${OK} All tests passed ✨"; \
	else \
		echo "${ERROR} Some tests failed ❌" >&2; \
		exit 1; \
	fi

.PHONY: test-all
test-all: test					## Run all tests

.PHONY: fix
fix: 					## Run ruff to auto-fix issues
	@echo "${INFO} Running ruff to auto-fix issues... 🛠️"
	@if uv run ruff check src tests --fix; then \
		echo "${OK} Ruff auto-fix completed ✨"; \
	else \
		echo "${ERROR} Ruff auto-fix encountered issues ❌" >&2; \
		exit 1; \
	fi

.PHONY: check
check: fix lint test-all				## Run all linting and tests

.PHONY: check-all
check-all: fix lint test-all coverage			## Run all linting, tests, and coverage checks

# =============================================================================
# Docs
# =============================================================================

docs-clean: 					## Dump the existing built docs
	@echo "${INFO} Cleaning documentation build assets... 🧹"
	@rm -rf docs/_build
	@echo "${OK} Documentation build assets removed ✨"

docs: docs-clean 				## Dump the existing built docs and rebuild them
	@echo "${INFO} Building documentation... 📚"
	@uv run sphinx-build -M html docs docs/_build/ -E -a -j auto --keep-going

docs-serve: docs 				## Serve the docs locally
	@echo "${INFO} Starting live documentation server... 🔧"
	@echo "${INFO} Documentation built successfully, serving static files... 🚀"
	@uv run sphinx-autobuild docs docs/_build/ -j auto --watch src --watch docs --watch tests --open-browser --port=0 --delay 5