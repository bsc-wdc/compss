#!/bin/bash

############################
## Check typing with mypy ##
############################

mypy --pretty --ignore-missing-imports --exclude 'pycompss\/((tests\/)|(dds\/)|(streams\/)||(interactive.py)|(__main__.py))$' ./pycompss/

