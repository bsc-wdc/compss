#!/bin/bash -e

  # WARN: nosetests only recognises *.py files in mode 6xx

  # Run unit tests (Last boolean is to disable integration tests)
  python nose_tests.py -s -v False

  # Only with setuptools
  # python setup.py nosetests -
