#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.decaf import decaf
from pycompss.api.parameter import FILE_OUT


@decaf(working_dir=".", runner="mpirun", df_script="$PWD/src/decaf/test.py", df_executor="test.sh", df_lib="lib")
@task(param=FILE_OUT)
def myDecafAll(param):
    pass


def main():
    from pycompss.api.api import compss_barrier
    myDecafAll("outFileAll")
    compss_barrier()
    print("Finished")


if __name__ == '__main__':
    main()
