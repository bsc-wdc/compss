#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${SCRIPT_DIR}/../../src/pycompss/"

##################
## Check Pylint ##
##################

find . -type f -name "*.py" -not -path "./tests/*" | xargs pylint

cd "${CURRENT_DIR}"

# Exit all ok
exit 0
