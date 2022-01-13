#!/bin/bash -e

pycompss environment change default
pycompss run -d $(pwd)/src/matmul/matmul_files.py 4 4