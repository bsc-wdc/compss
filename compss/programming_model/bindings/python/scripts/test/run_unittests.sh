#!/bin/bash -e

#---------------------------------------------------
# FUNCTIONS AND VARIABLES
#---------------------------------------------------

source unittests_commons.sh

#---------------------------------------------------
# MAIN
#---------------------------------------------------

run_unittests () {
  # shellcheck disable=SC2164
  cd "${RUN_DIR}"

  if [ "${unittests}" = "true" ]; then
    # Run unittests
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose pycompss/tests/unittests/
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED unittests"
      exit ${exit_code}
    fi
    clean_unittests
  fi

  if [ "${integration_unittests}" = "true" ]; then
    # Run integration unittests
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose pycompss/tests/integration/
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED integration unittests"
      exit ${exit_code}
    fi
    clean_integration_unittests
  fi

  if [ "${jupyter_unittests}" = "true" ]; then
    # Run notebooks unittesting
    python3 -m pytest -c ${COVERAGE_PROCESS_START} --verbose --nbval pycompss/tests/jupyter/notebook/simple.ipynb
    exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
      echo "ERROR: FAILED notebook unittests"
      exit ${exit_code}
    fi
  fi

  # shellcheck disable=SC2164
  cd "${CURRENT_DIR}"
}

#---------------------------------------------------
# MAIN EXECUTION
#---------------------------------------------------
get_args "$@"
check_args
log_parameters
run_unittests

# END
echo "INFO: SUCCESS: Python unittests OK"
# Normal exit
exit 0
