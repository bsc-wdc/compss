#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import numpy as np

from pycompss.api.api import compss_open
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on

from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.parameter import *
from pycompss.api.mpi import mpi


@constraint(computing_units="2")
@mpi(binary="date", working_dir="/tmp", runner="mpirun", processes=2, scale_by_cu=True)
@task()
def myDateConstrained(dprefix, param):
    pass


class TestMPI(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in primitive-type
    parameters.
    """

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        compss_barrier()
