#!/bin/bash -e

#
# FUNCTIONS AND VARIABLES
#

source unittests_commons.sh

#
# MAIN
#

# shellcheck disable=SC2164
cd "${RUN_DIR}"

pytest -c ${COVERAGE_PROCESS_START} pycompss/tests/integration_fail/test_*.py

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Exit all ok
exit 0
