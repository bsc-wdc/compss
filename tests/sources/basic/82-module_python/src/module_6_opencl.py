#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.opencl import opencl


@opencl(kernel="date")
@task()
def myDate(dprefix, param):
    pass


def main():
    from pycompss.api.api import compss_barrier
    myDate("-d", "next friday")
    compss_barrier()
    print("Finished")


if __name__ == '__main__':
    main()
