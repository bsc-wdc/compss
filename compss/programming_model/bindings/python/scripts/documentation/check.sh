#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

######################
## Check pydocstyle ##
######################

pydocstyle --match='(?!data_type|test_|heapq3).*\.py' ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] pycodestyle check failed with exit value: $ev"
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] pycodestyle check success"

# Exit all ok
exit 0
