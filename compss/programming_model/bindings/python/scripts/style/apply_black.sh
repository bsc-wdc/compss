#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

#######################
## Check black style ##
#######################

black --line-length 79 ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] black check failed with exit value: $ev"
  echo ""
  echo "Please, run:"
  echo "    black --line-length 79 $(pwd)/pycompss"
  echo "Then, review changes and push them again."
  echo ""
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] black applied successfully"

# Exit all ok
exit 0
