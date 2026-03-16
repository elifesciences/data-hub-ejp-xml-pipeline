FROM python:3.12-slim
ARG install_dev=n

USER root
WORKDIR /ejp_xml_pipeline

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV VENV=/opt/venv
ENV VIRTUAL_ENV=${VENV} PYTHONUSERBASE=${VENV} PATH=${VENV}/bin:$PATH

COPY pyproject.toml uv.lock ./
RUN if [ "${install_dev}" = "y" ]; \
  then uv sync --active --frozen --dev; \
  else uv sync --active --frozen --no-dev; \
  fi

COPY .pylintrc .flake8 mypy.ini ./
COPY ejp_xml_pipeline ./ejp_xml_pipeline
COPY tests ./tests
COPY run_test.sh ./
