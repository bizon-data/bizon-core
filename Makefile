.PHONY : install install-ci format
.DEFAULT_GOAL := help

help:
	@echo "install: Install all dependencies"
	@echo "install-ci: Install all dependencies for CI"
	@echo "format: Run pre-commit hooks"

install:
	pip install poetry
	poetry install --with dev --with test --all-extras
	pre-commit install

format:
	pre-commit run --all-files