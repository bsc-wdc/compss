#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run_python3() {
    echo "[INFO] Running Coverage3 tests."
    python3 -m coverage run --rcfile="${SCRIPT_DIR}/coverage.cfg" nose_tests.py False
    ev=$?
    if [ "$ev" -ne 0 ]; then
      echo "[ERROR] Coverage3 tests FAILED failed with exit value: $ev"
      exit $ev
    fi
    python3 -m coverage combine
    python3 -m coverage report -m
    echo "[INFO] Coverage3 tests finished."
  }

  clean_temps() {
    echo "[INFO] Cleaning"
    rm src/pycompss/tests/dds/dataset/pickles/00001
    rm src/pycompss/tests/dds/dataset/tmp/00001
    git co src/pycompss/tests/dds/dataset/pickles/00000
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
  run_python3
  clean_temps

  # Generate XML file
  python3 -m coverage xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Coverage3 XML generation failed with exit value: $ev"
    exit $ev
  fi

  # Move .coverage to be combined later
  mv .coverage .coverage.base

  # shellcheck disable=SC2164
  cd "${CURRENT_DIR}"
  # Exit all ok
  exit 0
