#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

############################
## Check typing with mypy ##
############################

mypy --pretty --ignore-missing-imports --allow-redefinition --exclude 'pycompss\/((tests\/)|(dds\/)|(streams\/)||(interactive.py)|(util\/interactive\/events.py)||(util\/interactive\/state.py)|(__main__.py))$' ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] Mypy check failed with exit value: $ev"
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] Mypy check success"

# Exit all ok
exit 0
