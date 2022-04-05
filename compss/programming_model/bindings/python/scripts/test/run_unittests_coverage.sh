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

#
# MAIN
#

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Coverage configuration
COVERAGE_PROCESS_START=${SCRIPT_DIR}/coverage.cfg

RUN_DIR="${SCRIPT_DIR}/../../src/"
# shellcheck disable=SC2164
cd "${RUN_DIR}"

# Prepare PYTHONPATH
export PYTHONPATH=${RUN_DIR}:${PYTHONPATH}

# Run unittests
pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss pycompss/tests/unittests/
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED unittests"
  exit ${exit_code}
fi
clean_unittests

# Run integration unittests
pytest -c ${COVERAGE_PROCESS_START} --verbose --cov=pycompss --cov-append pycompss/tests/integration/test_*.py
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED integration unittests"
  exit ${exit_code}
fi
clean_integration_unittests

# FAILS
# pytest --cov=pycompss --cov-append pycompss/tests/integration/test_dds_examples.py
# pytest --cov=pycompss --cov-append pycompss/tests/integration_fail/test_launch_stream_objects.py

# Run notebooks unittesting
pytest -c ${COVERAGE_PROCESS_START} --verbose --nbval --cov=pycompss --cov-append pycompss/tests/jupyter/notebook/simple.ipynb

# Generate xml report for coverage upload
python3 -m coverage xml
ev=$?
if [ "$ev" -ne 0 ]; then
  echo "[ERROR] XML generation failed with exit value: $ev"
  exit $ev
fi

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Get coverage report
mv "${RUN_DIR}/.coverage" "${CURRENT_DIR}/."
mv "${RUN_DIR}/coverage.xml" "${CURRENT_DIR}/."

# Exit all ok
exit 0
