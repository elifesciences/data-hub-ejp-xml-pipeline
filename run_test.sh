#!/bin/bash

set -e

# avoid issues with .pyc/pyo files when mounting source directory
export PYTHONOPTIMIZE=

echo "running pylint"
PYLINTHOME=/tmp/datahub-dags-pylint \
 pyhton -m pylint tests/ ejp_xml_pipeline/

echo "running flake8"
pyhton -m flake8 tests/ ejp_xml_pipeline/

echo "running mypy"
pyhton -m mypy tests/ ejp_xml_pipeline/

echo "running unit tests"s
pyhton -m pytest tests/unit_test/ -p no:cacheprovider -s --disable-warnings

if [[ $1  &&  $1 == "with-end-to-end" ]]; then
    echo "running end to end tests"
    pyhton -m pytest tests/end2end_test/ -p no:cacheprovider -s
fi

echo "done"
