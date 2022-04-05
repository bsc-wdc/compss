#!/bin/bash -e

#
# FUNCTIONS
#

clean_unittests() {
  echo "[INFO] Cleaning unittests"
  # Remove temporary files
}

clean_integration_unittests() {
  echo "[INFO] Cleaning integration unittests"
  # Reestablish dds dataset
  rm pycompss/tests/unittests/dds/dataset/pickles/00001
  rm pycompss/tests/unittests/dds/dataset/tmp/00001
  #git co pycompss/tests/unittests/dds/dataset/pickles/00000
  # Remove temporary files
  [ -e outfile_1 ] && rm outfile_1
  [ -e outfile_2 ] && rm outfile_2
  rm compss*.err
  rm compss*.out
}

# Current script directory
CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Coverage configuration
COVERAGE_PROCESS_START=${SCRIPT_DIR}/coverage.cfg

# Run directory (where pycompss folder is)
RUN_DIR="${SCRIPT_DIR}/../../src/"

# Prepare PYTHONPATH
export PYTHONPATH=${RUN_DIR}:${PYTHONPATH}
