#!/bin/bash

##########################################
## Check typing (with metics) with mypy ##
##########################################

mypy --pretty --html-report . --txt-report . --check-untyped-defs --warn-redundant-casts --ignore-missing-imports --exclude 'pycompss\/((tests\/)|(dds\/)|(streams\/)||(interactive.py)|(__main__.py))$' ./pycompss/

