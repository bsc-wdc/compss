#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${SCRIPT_DIR}/../../src/pycompss/"

##################
## Check Pylint ##
##################

prospector  # analyses current directory

cd "${CURRENT_DIR}"

# Exit all ok
exit 0
