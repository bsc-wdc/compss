#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

#######################
## Check black style ##
#######################

flake8 ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] flake8 check failed with exit value: $ev"
  echo ""
  echo "Please, run:"
  echo "    black --line-length 79 $(pwd)/pycompss"
  echo "Fix any missing error message."
  echo "And then, review changes and push them again."
  echo ""
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] flake8 check success"

# Exit all ok
exit 0
