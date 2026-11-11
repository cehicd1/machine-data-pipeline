PYTHON ?= python3.13
VENV   := .venv
BIN    := $(VENV)/bin

.PHONY: setup data run test lint clean

setup:  ## Create virtualenv and install package with dev dependencies
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

data:   ## Generate synthetic raw CSV into data/raw/
	$(BIN)/python -m machine_pipeline.generate

run:    ## Run bronze -> silver -> gold
	$(BIN)/python -m machine_pipeline.pipeline

test:   ## Run test suite
	$(BIN)/pytest

lint:   ## Lint and check formatting
	$(BIN)/ruff check src tests databricks
	$(BIN)/ruff format --check src tests databricks

clean:  ## Remove generated data and caches
	rm -rf data/raw data/bronze data/silver data/gold data/quarantine
	rm -rf .pytest_cache .ruff_cache spark-warehouse
