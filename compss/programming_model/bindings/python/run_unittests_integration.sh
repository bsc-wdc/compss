#!/bin/bash -e

  # WARN: nosetests only recognises *.py files in mode 6xx

  # Run unit tests (Last boolean is to enable integration tests)
  python nose_tests.py -s -v True

  # Only with setuptools 
  # python setup.py nosetests -
