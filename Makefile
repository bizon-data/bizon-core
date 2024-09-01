.DEFAULT_GOAL := help

install:
	pip install poetry
	poetry install --all-extras
	pre-commit install

format:
	pre-commit run --all-files