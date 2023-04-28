#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

######################
## Check pydocstyle ##
######################

pydocstyle --match='(?!data_type|test_).*\.py' ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] pydocstyle check failed with exit value: $ev"
  echo ""
  echo "Please, fix the issues and push them again."
  echo "The issues can be checked manually by running:"
  echo "    $(pwd)/../scripts/style/check_documentation.sh"
  echo ""
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] pydocstyle check success"

# Exit all ok
exit 0
