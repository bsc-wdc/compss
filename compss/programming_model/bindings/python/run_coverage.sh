#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  export COVERAGE_PROCESS_START=$(pwd)/coverage.cfg

  # Run a coverage report for a module
  run() {
    coverage run --rcfile=$(pwd)/coverage.cfg nose_tests.py False
                 # --source="src/pycompss" \
                 # --omit="/usr/lib/*" \
                 # --omit="src/pycompss/api/tests_parallel/*" \
                 # --concurrency=multiprocessing \  # not allowed as flag with the others
                 # --omit="src/pycompss/util/translators/*" \
    coverage combine
    coverage report -m
  }


  #
  # MAIN
  #

  # Run coverage on pycompss folder
  run

  # Generate XML file
  coverage xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
    echo "[ERROR] Coverage XML generation failed with exit value: $ev"
    exit $ev
  fi

  # Exit all ok
  exit 0
