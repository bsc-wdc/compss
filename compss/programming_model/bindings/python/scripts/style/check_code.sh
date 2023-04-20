#!/bin/bash

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

#######################
## Check pycodestyle ##
#######################

pycodestyle --max-line-length=79 --statistics --count  --ignore=E203,W503 ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] pycodestyle check failed with exit value: $ev"
  echo ""
  echo "Please, fix the issues and push them again."
  echo "The issues can be checked manually by running:"
  echo "    $(pwd)/../scripts/style/check_code.sh"
  echo ""
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] pycodestyle check success"

# Exit all ok
exit 0
