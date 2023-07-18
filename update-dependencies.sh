#!/bin/bash
# updates Pipfile.lock and regenerates the requirements.txt files.
# modified from https://github.com/elifesciences/update-python-dependencies to suit data team.
set -e

# replace any existing venv
rm -rf venv/

# whatever your preferred version of python is, eLife needs to support python3.8 (Ubuntu 20.04)
python3.8 -m venv venv

# prefer using wheels to compilation
source venv/bin/activate
pip install pip wheel --upgrade

export VIRTUAL_ENV="venv"

# updates the Pipfile.lock file and then installs the newly updated dependencies.
# the envvar is necessary otherwise pipenv will use it's own .venv directory.
pipenv update --dev

datestamp=$(date -I)
echo "# file generated $datestamp - see update-dependencies.sh" > requirements.txt
pipenv requirements --exclude-markers | grep -v '^-i ' >> requirements.txt

echo "# file generated $datestamp - see update-dependencies.sh" > requirements.dev.txt
pipenv requirements --exclude-markers --dev-only | grep -v '^-i ' >> requirements.dev.txt
