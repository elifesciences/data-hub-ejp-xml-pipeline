#!/usr/bin/make -f

DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml -f docker-compose.ci.override.yml
DOCKER_COMPOSE_DEV = docker-compose -f docker-compose.yml -f docker-compose.dev.override.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)


VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

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
	$(PYTHON) -m flake8 ejp_xml_pipeline tests

dev-pylint:
	$(PYTHON) -m pylint ejp_xml_pipeline tests

dev-mypy:
	$(PYTHON) -m mypy ejp_xml_pipeline tests

dev-lint: dev-flake8 dev-pylint dev-mypy

dev-unittest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/unit_test

dev-watch:
	$(PYTHON) -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)


dev-test: dev-lint dev-unittest


dev-run-ejp-xml-pipeline:
	EJP_XML_CONFIG_FILE_PATH=sample_data_config/ejp-xml-data-pipeline.config.yaml \
		$(PYTHON) -m ejp_xml_pipeline.cli



build:
	$(DOCKER_COMPOSE) build data-hub-pipelines

build-dev:
	$(DOCKER_COMPOSE) build data-hub-pipelines-dev

flake8:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines-dev \
		python -m flake8 ejp_xml_pipeline tests

pylint:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines-dev \
		python -m pylint ejp_xml_pipeline tests

mypy:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines-dev \
		python -m mypy ejp_xml_pipeline tests

lint: flake8 pylint mypy

unittest:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines-dev \
		python -m pytest -p no:cacheprovider $(ARGS) tests/unit_test

test: lint unittest

watch:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines-dev \
		python -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)

test-exclude-e2e: build-dev
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines-dev ./run_test.sh

clean:
	$(DOCKER_COMPOSE) down -v

data-hub-pipelines-run-ejp-xml-pipeline:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines \
	python -m ejp_xml_pipeline.cli


end2end-test:
	$(DOCKER_COMPOSE) run --rm  test-client


ci-build-main-image:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build

ci-test-exclude-e2e: build-dev
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		test-exclude-e2e


ci-build-and-end2end-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build-dev \
		end2end-test

ci-clean:
	$(DOCKER_COMPOSE_CI) down -v


retag-push-image:
	docker tag  $(EXISTING_IMAGE_REPO):$(EXISTING_IMAGE_TAG) $(IMAGE_REPO):$(IMAGE_TAG)
	docker push  $(IMAGE_REPO):$(IMAGE_TAG)