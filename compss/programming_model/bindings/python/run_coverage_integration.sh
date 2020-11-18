#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run_python2() {
    # -a appends this coverage to the one produced by coverage_run.sh
    python2 -m coverage run -a --rcfile="${SCRIPT_DIR}/coverage.cfg" nose_tests.py True
    python2 -m coverage combine --append
    python2 -m coverage report -m
  }

  # Run a coverage report for a module
  run_python3() {
    # -a appends this coverage to the one produced by coverage_run.sh
    python3 -m coverage run -a --rcfile="${SCRIPT_DIR}/coverage.cfg" nose_tests.py True
    python3 -m coverage combine --append
    python3 -m coverage report -m
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
  python2 -m coverage xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Integration coverage2 XML generation failed with exit value: $ev"
    exit $ev
  fi

  # Run coverage on pycompss folder
  run_python3

  # Generate XML file
  python3 -m coverage xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Integration coverage3 XML generation failed with exit value: $ev"
    exit $ev
  fi

  # RUN COVERAGE WITH NOTEBOOKS
  pytest --nbval -v --cov=${SCRIPT_DIR}/src --cov-append --cov-report=xml src/pycompss/tests/resources/notebook/simple.ipynb
  python3 -m coverage report -m

#  # Fix paths
#  sed -i s#${COMPSS_HOME}/Bindings/python/2/#src/#g coverage.xml
#  sed -i s#${COMPSS_HOME}/Bindings/python/3/#src/#g coverage.xml
#  # shellcheck disable=SC2001
#  COMPSS_HOME_DOTS=$(echo "$COMPSS_HOME" | sed 's,/,.,g')
#  sed -i s#${COMPSS_HOME_DOTS}.Bindings.python.2.#src.#g coverage.xml
#  sed -i s#${COMPSS_HOME_DOTS}.Bindings.python.3.#src.#g coverage.xml

  # shellcheck disable=SC2164
  cd "${CURRENT_DIR}"
  # Exit all ok
  exit 0
