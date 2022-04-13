#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

#################
## Check Black ##
#################

black --check ./pycompss/
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] Black check failed with exit value: $ev"
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

echo "[OK] Black check success"

# Exit all ok
exit 0
