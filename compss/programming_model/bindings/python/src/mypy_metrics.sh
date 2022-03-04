#!/bin/bash

##########################################
## Check typing (with metics) with mypy ##
##########################################

mypy --pretty --html-report . --txt-report . --check-untyped-defs --warn-redundant-casts --ignore-missing-imports --exclude 'pycompss\/((tests)\/|(dds)\/|(functions\/data_tasks.py)|(functions\/elapsed_time.py)|(streams\/)|(runtime\/launch.py)|(util\/interactive\/events.py)|(interactive.py)|(__main__.py))$' ./pycompss/

