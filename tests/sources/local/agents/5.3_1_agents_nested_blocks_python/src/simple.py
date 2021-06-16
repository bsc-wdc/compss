#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *
import sys


@task(N=IN, returns=int)
def count(N):
    val = 0
    if N > 1:
        val = count(N - 1)
    else:
        val = N
    val = compss_wait_on(val)
    print("testValue: " + str(val))
    return val + 1


@task()
def main(argv):
    if len(sys.argv) > 0:
        k = int(argv[0])
        val = count(k)
        val = compss_wait_on(val)
        print("testValue: " + str(val))
    


if __name__ == "__main__":
    main(sys.argv[1:])