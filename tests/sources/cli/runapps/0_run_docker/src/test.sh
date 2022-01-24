#!/bin/bash -e

pycompss init -n docker_run_app docker
pycompss run src/matmul/matmul_files.py 4 4