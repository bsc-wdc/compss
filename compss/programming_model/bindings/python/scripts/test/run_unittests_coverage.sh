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

# Run unittests
pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss pycompss/tests/unittests/
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED unittests"
  exit ${exit_code}
fi
clean_unittests

# Run integration unittests
pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss --cov-append pycompss/tests/integration/test_*.py
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED integration unittests"
  exit ${exit_code}
fi
clean_integration_unittests

# Run notebooks unittesting
pytest -c ${COVERAGE_PROCESS_START} --verbose --nbval --cov=pycompss --cov-append pycompss/tests/jupyter/notebook/simple.ipynb
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED notebook unittests"
  exit ${exit_code}
fi

# Generate xml report for coverage upload
python3 -m coverage xml
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] XML generation failed with exit value: $ev"
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Get coverage report
mv "${RUN_DIR}/.coverage" "${CURRENT_DIR}/."
mv "${RUN_DIR}/coverage.xml" "${CURRENT_DIR}/."

# Exit all ok
exit 0
