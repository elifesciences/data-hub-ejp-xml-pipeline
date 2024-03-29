#!/usr/bin/make -f

DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml -f docker-compose.ci.override.yml
DOCKER_COMPOSE_DEV = docker-compose -f docker-compose.yml -f docker-compose.dev.override.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)


VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = PYTHONPATH=dags $(VENV)/bin/python

PYTEST_WATCH_MODULES = tests/unit_test

venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi

venv-create:
	python3 -m venv $(VENV)

venv-activate:
	chmod +x venv/bin/activate
	bash -c "venv/bin/activate"

dev-install:
	$(PIP) install --disable-pip-version-check -r requirements.build.txt
	SLUGIFY_USES_TEXT_UNIDECODE=yes \
	$(PIP) install --disable-pip-version-check -r requirements.txt
	$(PIP) install --disable-pip-version-check -r requirements.dev.txt
	$(PIP) install --disable-pip-version-check -e . --no-deps

dev-venv: venv-create dev-install

dev-flake8:
	$(PYTHON) -m flake8 ejp_xml_pipeline dags tests

dev-pylint:
	$(PYTHON) -m pylint ejp_xml_pipeline dags tests

dev-mypy:
	$(PYTHON) -m mypy ejp_xml_pipeline dags tests

dev-lint: dev-flake8 dev-pylint dev-mypy

dev-unittest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/unit_test


dev-dagtest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/dag_validation_test

dev-integration-test: dev-install
	$(VENV)/bin/airflow upgradedb
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/integration_test

dev-watch:
	$(PYTHON) -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)


dev-test: dev-lint dev-unittest dev-dagtest

build:
	$(DOCKER_COMPOSE) build data-hub-dags

build-dev:
	$(DOCKER_COMPOSE) build data-hub-dags-dev

flake8:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m flake8 ejp_xml_pipeline dags tests

pylint:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pylint ejp_xml_pipeline dags tests

mypy:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m mypy ejp_xml_pipeline dags tests

lint: flake8 pylint mypy

dagtest:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pytest -p no:cacheprovider $(ARGS) tests/dag_validation_test

unittest:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pytest -p no:cacheprovider $(ARGS) tests/unit_test

test: lint unittest

watch:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)

airflow-start:
	$(DOCKER_COMPOSE) up worker webserver

airflow-stop:
	$(DOCKER_COMPOSE) down

test-exclude-e2e: build-dev
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev ./run_test.sh

clean:
	$(DOCKER_COMPOSE) down -v

airflow-db-migrate:
	$(DOCKER_COMPOSE) run --rm  webserver db migrate

airflow-initdb:
	$(DOCKER_COMPOSE) run --rm  webserver db init


end2end-test:
	$(MAKE) clean
	$(MAKE) airflow-db-migrate
	$(MAKE) airflow-initdb
	$(DOCKER_COMPOSE) run --rm  test-client
	$(MAKE) clean


ci-test-exclude-e2e: build-dev
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		test-exclude-e2e


ci-build-and-end2end-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build-dev \
		end2end-test

ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
