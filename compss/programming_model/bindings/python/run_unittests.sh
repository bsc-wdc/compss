#!/bin/bash -e

  # WARN: nosetests only recognises *.py files in mode 6xx

  # Run unit tests (Last boolean is to disable integration tests)
  python2 nose_tests.py -s -v False
  exit_code=$?
  if [ ${exit_code} -ne 0 ]; then
    echo "ERROR: FAILED unittests with Python 2"
    exit ${exit_code}
  fi

  python3 nose_tests.py -s -v False
  exit_code=$?
  if [ ${exit_code} -ne 0 ]; then
    echo "ERROR: FAILED unittests with Python 3"
    exit ${exit_code}
  fi

  # Only with setuptools
  # python setup.py nosetests -
