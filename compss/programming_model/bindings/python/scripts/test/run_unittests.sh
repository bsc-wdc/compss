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
pytest -c ${COVERAGE_PROCESS_START} --verbose pycompss/tests/unittests/
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED unittests"
  exit ${exit_code}
fi
clean_unittests

# Run integration unittests
pytest -c ${COVERAGE_PROCESS_START} --verbose pycompss/tests/integration/test_*.py
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED integration unittests"
  exit ${exit_code}
fi
clean_integration_unittests

# FAILS
# pytest --cov=pycompss --cov-append pycompss/tests/integration/test_dds_examples.py
# pytest --cov=pycompss --cov-append pycompss/tests/integration_fail/test_launch_stream_objects.py

# Run notebooks unittesting
pytest -c ${COVERAGE_PROCESS_START} --verbose --nbval pycompss/tests/jupyter/notebook/simple.ipynb
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED notebook unittests"
  exit ${exit_code}
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Exit all ok
exit 0
