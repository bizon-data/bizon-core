.PHONY: install install-ci format lint test help
.DEFAULT_GOAL := help

help:
	@echo "install:      Install all dependencies with dev and test groups"
	@echo "install-ci:   Install dependencies for CI (no dev)"
	@echo "format:       Format code with Ruff"
	@echo "lint:         Lint code with Ruff"
	@echo "test:         Run tests with pytest"

install:
	pip install uv==0.13.8
	uv sync --all-extras --all-groups
	uv run pre-commit install

install-ci:
	pip install uv==0.13.8
	uv sync --locked --no-dev --all-extras --group test

format:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff check .

test:
	uv run --no-dev --group test pytest tests/
