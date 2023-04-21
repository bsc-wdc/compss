#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${SCRIPT_DIR}/../../src/pycompss/"

##################
## Check Pylint ##
##################

bandit -r -c ../../pyproject.toml .

cd "${CURRENT_DIR}"

# Exit all ok
exit 0
