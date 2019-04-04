#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.opencl import opencl
from pycompss.api.constraint import constraint


@opencl(kernel="date", working_dir="/tmp")
@task()
def myDate(dprefix, param):
    pass


@constraint(computing_units="2")
@opencl(kernel="date", working_dir="/tmp")
@task()
def myDateConstrained(dprefix, param):
    pass


class testOpenclDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        compss_barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        compss_barrier()
