.DEFAULT_GOAL := help

install:
	poetry install
	pre-commit install

format:
	pre-commit run --all-files