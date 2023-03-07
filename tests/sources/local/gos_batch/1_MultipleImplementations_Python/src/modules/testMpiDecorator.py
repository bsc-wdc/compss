#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import os

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier, compss_open, compss_wait_on
from pycompss.api.mpi import mpi
from pycompss.api.constraint import constraint


@mpi(binary="date", working_dir="${WK_DIR}", runner="mpirun", processes=4)
@task()
def myDate(dprefix, param):
    pass


@constraint(computing_units="2")
@mpi(binary="date", working_dir="/home/bsc19/bsc19409/tmpTests/", runner="mpirun", processes=2, scale_by_cu=True)
@task()
def myDateConstrained(dprefix, param):
    pass


@mpi(binary="date", working_dir="${WK_DIR}", runner="mpirun",)
@task()
def myDateDef(dprefix="-d", param="next wednesday"):
    pass


@mpi(binary="date", params="{{dprefix}} {{param}}", working_dir="${WK_DIR}", runner="mpirun",)
@task()
def myDateDef_2(dprefix="-d", param="last wednesday"):
    pass


@constraint(computing_units="2")
@mpi(binary="date", working_dir="${WK_DIR}", runner="mpirun", processes="2", scale_by_cu=True)
@task()
def myDateConstrainedWithEnvVar(dprefix, param):
    pass


@mpi(binary="date", working_dir=".", runner="mpirun", processes=1)
@task(returns=int)
def myReturn():
    pass




class testMpiDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        compss_barrier()

    def testDefaultValue(self):
        myDateDef()
        compss_barrier()

    def testDefaultValue_2(self):
        myDateDef_2()
        compss_barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        compss_barrier()

    def testFunctionalUsageWithEnvVarConstraint(self):
        myDateConstrainedWithEnvVar("-d", "next tuesday")
        compss_barrier()

    def testReturn(self):
        ev = myReturn()
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

