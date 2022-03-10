#!/bin/bash -e


#
# MAIN
#
CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}"

# WARN: nosetests only recognises *.py files in mode 6xx

# Run unit tests (Last boolean is to enable integration tests)
python3 nose_tests.py -s -v True
exit_code=$?
if [ ${exit_code} -ne 0 ]; then
  echo "ERROR: FAILED unittests with Python 3"
  exit ${exit_code}
else
  rm compss-*
fi

# Only with setuptools
# python setup.py nosetests -

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"
