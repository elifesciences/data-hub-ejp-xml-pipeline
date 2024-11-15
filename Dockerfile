FROM python:3.9-slim
ARG install_dev=n

USER root

RUN apt-get update \
  && apt-get install gcc -yqq \
  && rm -rf /var/lib/apt/lists/*

ENV PIP_NO_CACHE_DIR=1

WORKDIR /ejp_xml_pipeline

COPY requirements.build.txt ./
RUN pip install --disable-pip-version-check -r requirements.build.txt --user

COPY requirements.txt ./
RUN pip install --disable-pip-version-check -r requirements.txt --user

COPY requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --disable-pip-version-check --user -r requirements.dev.txt; fi

COPY ejp_xml_pipeline ./ejp_xml_pipeline
COPY setup.py ./setup.py
RUN pip install -e . --user --no-dependencies

COPY .pylintrc ./.pylintrc
COPY .flake8 ./.flake8
COPY tests ./tests
COPY mypy.ini ./
COPY run_test.sh ./
