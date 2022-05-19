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

  coverage_files=""

  if [ "${unittests}" = "true" ]; then
    # Run unittests
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss pycompss/tests/unittests/
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED unittests"
      exit ${exit_code}
    fi
    clean_unittests
    mv .coverage .coverage_unittests
    coverage_files=".coverage_unittests"
  fi

  if [ "${integration_unittests}" = "true" ]; then
    # Run integration unittests
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss pycompss/tests/integration/
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED integration unittests"
      exit ${exit_code}
    fi
    clean_integration_unittests
    mv .coverage .coverage_integration
    coverage_files="${coverage_files} .coverage_integration"
  fi

  if [ "${jupyter_unittests}" = "true" ]; then
    # Run notebooks unittesting
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --nbval --cov=pycompss pycompss/tests/jupyter/notebook/simple.ipynb
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED notebook unittests"
      exit ${exit_code}
    fi
    mv .coverage .coverage_notebooks
    coverage_files="${coverage_files} .coverage_notebooks"
  fi

  # Combine partial reports
  python3 -m coverage combine ${coverage_files}

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

  # Show final report
  python3 -m coverage report
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
