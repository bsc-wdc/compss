#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier
from pycompss.api.decaf import decaf
from pycompss.api.constraint import constraint


@decaf(df_script="$PWD/decaf/test-auto.py")
@task(param=FILE_OUT)
def myDecaf(param):
    pass


@decaf(working_dir=".", runner="mpirun", df_script="$PWD/decaf/test.py", df_executor="test.sh", df_lib="lib")
@task(param=FILE_OUT)
def myDecafAll(param):
    pass


@constraint(computing_units="2")
@decaf(runner="mpirun", computing_nodes=2, df_script="$PWD/decaf/test-2.py", df_executor="test-2.sh", df_lib="lib")
@task(param=FILE_OUT)
def myDecafConstrained(param):
    pass


class testDecafDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDecaf("outFile")
        compss_barrier()

    def testFunctionalUsageAll(self):
        myDecafAll("outFileAll")
        compss_barrier()

    def testFunctionalUsageWithConstraint(self):
        myDecafConstrained("outFileConstrained")
        compss_barrier()
