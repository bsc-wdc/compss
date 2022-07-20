#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
    This file represents PyCOMPSs Testbench.
    It implements all functionalities in order to evaluate the PyCOMPSs features.
"""

# Imports
from time import sleep
from pycompss.api.api import compss_barrier
from pycompss.api.task import task
from pycompss.api.constraint import constraint

@task(returns=1)
def increment_out(value):
    sleep(1)
    return value + "1"

@constraint(is_local=True)
@task(returns=1)
def increment_local(value):
    sleep(1)
    return value + "1"

def main():
    sleep(3)
    for i in range(0,10):
        result = increment_out("1234")
    compss_barrier()
    for i in range(0,10):
        result = increment_local("1234")

if __name__ == "__main__":
    main()
