#!/bin/bash -e

#---------------------------------------------------
# FUNCTIONS AND VARIABLES
#---------------------------------------------------

source unittests_commons.sh

#---------------------------------------------------
# MAIN
#---------------------------------------------------

run_unittests_coverage () {
  # shellcheck disable=SC2164
  cd "${RUN_DIR}"

  if [ "${unittests}" = "true" ]; then
    # Run unittests
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss pycompss/tests/unittests/
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED unittests"
      exit ${exit_code}
    fi
    clean_unittests
  fi

  if [ "${integration_unittests}" = "true" ]; then
    # Run integration unittests
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss --cov-append pycompss/tests/integration/test_*.py
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED integration unittests"
      exit ${exit_code}
    fi
    clean_integration_unittests
  fi

  if [ "${jupyter_unittests}" = "true" ]; then
    # Run notebooks unittesting
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --nbval --cov=pycompss --cov-append pycompss/tests/jupyter/notebook/simple.ipynb
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED notebook unittests"
      exit ${exit_code}
    fi
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
}

#---------------------------------------------------
# MAIN EXECUTION
#---------------------------------------------------
get_args "$@"
check_args
log_parameters
run_unittests_coverage

# END
echo "INFO: SUCCESS: Python unittests OK"
# Normal exit
exit 0
