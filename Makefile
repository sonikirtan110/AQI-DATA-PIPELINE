# AQI Data Engineering Pipeline Makefile
# Reference: https://github.com/sonikirtan110/aqi-pipeline

.PHONY: help install init dbt-run dbt-test test lint fetch airflow airflow-stop clean logs full-run

help:
	@echo "      AQI Pipeline Makefile"
	@echo "=========================================="
	@echo "Setup commands:"
	@echo "  make install       - Install all project dependencies with uv"
	@echo "  make init          - Initialize dbt project"
	@echo ""
	@echo "Development commands:"
	@echo "  make fetch         - Fetch OGD API data and upload to S3"
	@echo "  make dbt-run       - Run all dbt models (silver + gold)"
	@echo "  make dbt-test      - Run dbt data quality tests"
	@echo "  make test          - Run pytest unit tests"
	@echo "  make lint          - Lint code with ruff"
	@echo ""
	@echo "Orchestration:"
	@echo "  make airflow       - Start Airflow locally (Docker must be running)"
	@echo "  make airflow-stop  - Stop Airflow"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean         - Remove __pycache__ and .pytest_cache"
	@echo "  make logs          - Show recent logs"

install:
	@echo "Installing dependencies with uv..."
	uv sync

init:
	@echo "Initializing dbt project..."
	cd dbt && uv run dbt debug
	@echo "✓ dbt project initialized"

fetch:
	@echo "Fetching OGD data and uploading to S3..."
	uv run python include/scripts/fetch_ogd.py

dbt-run:
	@echo "Running dbt models (silver → gold)..."
	cd dbt && uv run dbt run

dbt-test:
	@echo "Running dbt data quality tests..."
	cd dbt && uv run dbt test

test:
	@echo "Running pytest unit tests..."
	uv run pytest tests/ -v

lint:
	@echo "Linting code with ruff..."
	uv run ruff check include/ dags/ tests/

airflow:
	@echo "Starting Apache Airflow (http://localhost:8080 → admin/admin)..."
	cd aqi_airflow && astro dev start

airflow-stop:
	@echo "Stopping Apache Airflow..."
	cd aqi_airflow && astro dev stop

clean:
	@echo "Cleaning up..."
	@powershell -Command "Get-ChildItem -Path '.' -Include '__pycache__', '.pytest_cache' -Recurse -Directory -Force | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -Command "Get-ChildItem -Path '.' -Include '*.pyc' -Recurse -File -Force | Remove-Item -Force -ErrorAction SilentlyContinue"
	@echo "✓ Cleaned"

logs:
	@echo "Recent logs from dbt and pipeline..."
	@powershell -Command "Get-ChildItem dbt/logs/*.log -ErrorAction SilentlyContinue | ForEach-Object { Write-Host '---' $_.Name; Get-Content $_.FullName -Tail 20 }"

full-run: fetch dbt-run dbt-test
	@echo "Full pipeline run complete."

.DEFAULT_GOAL := help
