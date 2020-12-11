#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run_python2() {
    echo "[INFO] Running Coverage2 tests."
    python2 -m coverage run --rcfile="${SCRIPT_DIR}/coverage.cfg" nose_tests.py False
    ev=$?
    if [ "$ev" -ne 0 ]; then
      echo "[ERROR] Coverage2 tests FAILED failed with exit value: $ev"
      exit $ev
    fi
    python2 -m coverage combine
    mv .coverage .coverage.python2
    echo "[INFO] Coverage2 tests finished."
    # python2 -m coverage report -m
  }

  run_python3() {
    echo "[INFO] Running Coverage3 tests."
    python3 -m coverage run --rcfile="${SCRIPT_DIR}/coverage.cfg" nose_tests.py False
    ev=$?
    if [ "$ev" -ne 0 ]; then
      echo "[ERROR] Coverage3 tests FAILED failed with exit value: $ev"
      exit $ev
    fi
    python3 -m coverage combine --append
    python3 -m coverage report -m
    echo "[INFO] Coverage3 tests finished."
  }


  #
  # MAIN
  #
  CURRENT_DIR="$(pwd)"
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  # shellcheck disable=SC2164
  cd "${SCRIPT_DIR}"

  export COVERAGE_PROCESS_START=${SCRIPT_DIR}/coverage.cfg

  # Force pytest coverage to take the sources instead of the installation
  export PYTHONPATH=${SCRIPT_DIR}:${SCRIPT_DIR}/src:${PYTHONPATH}

  # Run coverage on pycompss folder
  run_python2

  # Generate XML file
#  python2 -m coverage xml
#  ev=$?
#  if [ "$ev" -ne 0 ]; then
#    echo "[ERROR] Coverage2 XML generation failed with exit value: $ev"
#    exit $ev
#  fi

  # Run coverage on pycompss folder
  run_python3

  # Generate XML file
  python3 -m coverage xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Coverage3 XML generation failed with exit value: $ev"
    exit $ev
  fi

  # Move .coverage to be combined later
  mv .coverage .coverage.python2_3

  # shellcheck disable=SC2164
  cd "${CURRENT_DIR}"
  # Exit all ok
  exit 0
