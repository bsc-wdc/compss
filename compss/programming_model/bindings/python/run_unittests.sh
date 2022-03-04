#!/bin/bash -e


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

# WARN: nosetests only recognises *.py files in mode 6xx

# Run unit tests (Last boolean is to disable integration tests)
python3 nose_tests.py -s -v False
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED unittests"
  exit ${exit_code}
fi
clean_temps

# Clean all __pycache__
# Not necessary for normal unittests, probably for integration.
# find . -type d -name '__pycache__' -exec rm -rf {} \;

# Only with setuptools
# python setup.py nosetests -

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"
