#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run() {
    coverage run --omit="/usr/lib/*" nose_tests.py
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

