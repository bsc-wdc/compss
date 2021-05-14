#!/bin/bash

###################################
## Compilation with mypyc script ##
###################################

mypyc --ignore-missing-imports --exclude 'pycompss\/((tests)\/|(dds)\/|(streams\/)|(runtime\/launch.py)|(util/interactive/events.py)|(__main__.py))$' ./pycompss/
