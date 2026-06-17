.PHONY: venv reqs test test-cov lint python-version ingest ingest-day2 dq dq-day2 silver silver-day2 gold pipeline pipeline-day2 incremental pipeline-reset docker-build docker-up docker-down docker-logs docker-pipeline docker-pipeline-reset

# Uses pyenv — see .python-version (3.14.5)
# Note: Python 3.14+ installs python3 only (no python shim)
PYTHON := $(shell pyenv which python3)

python-version:
	@echo "pyenv local: $$(cat .python-version)"
	@$(PYTHON) --version

venv: python-version
	$(PYTHON) -m venv .venv

reqs:
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt -r requirements-dev.txt

test:
	.venv/bin/pytest -s tests/

test-cov:
	.venv/bin/pytest -s tests/ --cov=src --cov-report=term-missing

lint:
	.venv/bin/ruff check src tests

ingest:
	.venv/bin/python -m src.ingestion

ingest-day2:
	DATA_SNAPSHOT=data_day2 .venv/bin/python -m src.ingestion

dq:
	.venv/bin/python -m src.dq

dq-day2:
	DATA_SNAPSHOT=data_day2 .venv/bin/python -m src.dq

silver:
	.venv/bin/python -m src.transform

silver-day2:
	DATA_SNAPSHOT=data_day2 .venv/bin/python -m src.transform

gold:
	.venv/bin/python -m src.transform.gold

pipeline: ingest dq silver gold

pipeline-day2: ingest-day2 dq-day2 silver-day2 gold

incremental:
	.venv/bin/python -m src.pipeline

pipeline-reset:
	PIPELINE_RESET=true .venv/bin/python -m src.pipeline

# Docker / Airflow (Step 7) — compose and Dockerfile are authored in-repo
docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-pipeline:
	docker compose --profile pipeline run --rm pipeline

docker-pipeline-reset:
	docker compose --profile pipeline run --rm -e PIPELINE_RESET=true pipeline