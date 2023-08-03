#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

##########################################
## Check typing (with metics) with mypy ##
##########################################

mypy --pretty --html-report . --txt-report . --check-untyped-defs --warn-redundant-casts --ignore-missing-imports --exclude 'pycompss\/((tests\/)|(dds\/)|(util\/objects\/replace.py)|(api\/local.py)|(api\/commons\/private_tasks.py)|(api\/software.py)|(functions\/data_tasks.py)|(streams\/)|(interactive.py)|(util\/interactive\/events.py)||(util\/interactive\/state.py)|(__main__.py))$' ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] Mypy metrics failed with exit value: $ev"
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Exit all ok
exit 0