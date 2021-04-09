#!/bin/bash

#########################
## Compilation cleanup ##
#########################

rm -rf build
# rm -rf .mypy_cache
find . -type f -name "*\.so" -exec rm {} \;
find . -type d -name "*.mypy_cache" -exec rm -rf {} \;

# TODO: CHANGE THE SOURCE NAMES FROM *.py_source TO *.py