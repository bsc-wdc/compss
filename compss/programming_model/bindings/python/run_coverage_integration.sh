#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run() {
    # -a appends this coverage to the one produced by coverage_run.sh
    coverage2 run -a \
                 --source="src/pycompss" \
                 --omit="/usr/lib/*" \
                 --omit="src/pycompss/api/tests_parallel/*" \
                 nose_tests.py True
                 # --omit="src/pycompss/api/local.py" \
                 # --omit="src/pycompss/tests/api/test_local.py" \
                 # --omit="src/pycompss/util/objects/replace.py" \
                 # --omit="src/pycompss/util/translators/*" \
    coverage3 run -a \
                 --source="src/pycompss" \
                 --omit="/usr/lib/*" \
                 --omit="src/pycompss/api/tests_parallel/*" \
                 nose_tests.py True
                 # --omit="src/pycompss/api/local.py" \
                 # --omit="src/pycompss/tests/api/test_local.py" \
                 # --omit="src/pycompss/util/objects/replace.py" \
                 # --omit="src/pycompss/util/translators/*" \
    coverage report -m
  }


  #
  # MAIN
  #
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  cd ${SCRIPT_DIR}

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
