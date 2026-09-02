PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PYTHON_ENV = PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src

.PHONY: install install-dev coordinator worker gateway-worker mock-worker telegram test lint format-check e2e e2e-command e2e-gateway

install:
	$(PYTHON) -m pip install --no-build-isolation -e .

install-dev:
	$(PYTHON) -m pip install --no-build-isolation -e '.[dev]'

coordinator:
	$(PYTHON_ENV) $(PYTHON) -m hermes.coordinator --host 127.0.0.1 --port 8000

worker:
	$(PYTHON_ENV) $(PYTHON) -m hermes.worker

gateway-worker:
	$(PYTHON_ENV) $(PYTHON) -m hermes.gateway_worker

mock-worker:
	HERMES_WORKER_DEFAULT_AGENT=codex HERMES_WORKER_AGENTS_JSON='{"codex":["$(PYTHON)","-m","hermes.mock_hermes","-q","{prompt}"]}' $(PYTHON_ENV) $(PYTHON) -m hermes.worker

telegram:
	$(PYTHON_ENV) $(PYTHON) -m hermes.telegram_bot

test:
	$(PYTHON_ENV) $(PYTHON) -m unittest discover -s tests -v

lint:
	$(PYTHON) -m ruff check .

format-check:
	$(PYTHON) -m ruff format --check .

e2e: e2e-command e2e-gateway

e2e-command:
	$(PYTHON_ENV) $(PYTHON) scripts/e2e_mock.py

e2e-gateway:
	$(PYTHON_ENV) $(PYTHON) scripts/e2e_gateway_mock.py
