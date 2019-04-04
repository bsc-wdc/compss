#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.decaf import decaf
from pycompss.api.constraint import constraint
from pycompss.api.parameter import FILE_OUT


@constraint(computing_units="2")
@decaf(runner="mpirun", computing_nodes=2, df_script="$PWD/src/decaf/test-2.py", df_executor="test-2.sh", df_lib="lib")
@task(param=FILE_OUT)
def myDecafConstrained(param):
    pass


def main():
    from pycompss.api.api import compss_barrier
    myDecafConstrained("outFileConstrained")
    compss_barrier()
    print("Finished")


if __name__ == '__main__':
    main()
