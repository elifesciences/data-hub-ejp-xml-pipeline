#!/bin/bash

set -e

# avoid issues with .pyc/pyo files when mounting source directory
export PYTHONOPTIMIZE=

echo "running pylint"
PYLINTHOME=/tmp/datahub-dags-pylint \
 pylint tests/ ejp_xml_pipeline/

echo "running flake8"
flake8 tests/ ejp_xml_pipeline/

echo "running mypy"
mypy tests/ ejp_xml_pipeline/

pytest tests/unit_test/ -p no:cacheprovider -s --disable-warnings

if [[ $1  &&  $1 == "with-end-to-end" ]]; then
    echo "running end to end tests"
    pytest tests/end2end_test/ -p no:cacheprovider -s
fi

echo "done"
