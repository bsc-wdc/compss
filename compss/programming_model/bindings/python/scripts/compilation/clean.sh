#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

#########################
## Compilation cleanup ##
#########################

rm -rf build
# rm -rf .mypy_cache
find .. -type f -name "*\.so" -exec rm {} \;
find .. -type d -name "*.mypy_cache" -exec rm -rf {} \;
rm -rf index.* mypy-html.css html/

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Exit all ok
exit 0
