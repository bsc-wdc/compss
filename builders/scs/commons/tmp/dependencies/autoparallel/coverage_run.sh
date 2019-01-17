#!/bin/bash

  #
  # HELPER FUNCTIONS
  #

  # Run a coverage report for a module
  run() {
    coverage run --omit="*/tests/*,/usr/lib/*" nose_tests.py
    coverage report -m
  }


  #
  # MAIN
  #

  # Run coverage on pycompss folder
  (cd pycompss || exit 1; run)

  # Combine all reports
  coverage combine \
          pycompss/.coverage 
  ev=$?
  if [ "$ev" -ne 0 ]; then
          echo "[ERROR] Coverage combine failed with exit value: $ev"
          exit $ev
  fi

  # Show report to user
  coverage report -m

  # Generate XML file
  coverage xml
  ev=$?
  if [ "$ev" -ne 0 ]; then
          echo "[ERROR] Coverage XML generation failed with exit value: $ev"
          exit $ev
  fi

  # Exit all ok
  exit 0

