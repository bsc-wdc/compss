#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run_python2() {
    # -a appends this coverage to the one produced by coverage_run.sh
    coverage2 run -a \
                 --source="src/pycompss" \
                 --omit="/usr/lib/*" \
                 --omit="src/pycompss/api/tests_parallel/*" \
                 nose_tests.py True
    coverage2 combine --append
    coverage2 report -m
  }

  # Run a coverage report for a module
  run_python3() {
    # -a appends this coverage to the one produced by coverage_run.sh
    coverage3 run -a \
                 --source="src/pycompss" \
                 --omit="/usr/lib/*" \
                 --omit="src/pycompss/api/tests_parallel/*" \
                 nose_tests.py True
    coverage3 combine --append
    coverage3 report -m
  }


  #
  # MAIN
  #
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  export COVERAGE_PROCESS_START=${SCRIPT_DIR}/coverage.cfg

  # Run coverage on pycompss folder
  run_python2

  # Generate XML file
  coverage2 xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Integration coverage2 XML generation failed with exit value: $ev"
    exit $ev
  fi

  # Run coverage on pycompss folder
  run_python3

  # Generate XML file
  coverage3 xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Integration coverage3 XML generation failed with exit value: $ev"
    exit $ev
  fi

  # RUN COVERAGE WITH NOTEBOOKS

  # Force pytest coverage to take the sources instead of the installation
  export PYTHONPATH=${SCRIPT_DIR}:$PYTHONPATH
  pytest --nbval -v --cov=${SCRIPT_DIR}/src/pycompss/ --cov-append --cov-report=xml src/pycompss/tests/resources/notebook/simple.ipynb
  coverage3 report -m

  # Exit all ok
  exit 0
