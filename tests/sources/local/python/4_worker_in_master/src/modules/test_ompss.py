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
from pycompss.api.ompss import ompss


@constraint(computing_units="$CUS")
@ompss(binary="date", working_dir="/tmp")
@task()
def myDateConstrainedWithEnvVar(dprefix, param):
    pass


class TestOmpSs(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in primitive-type
    parameters.
    """

    def testFunctionalUsageWithEnvVarConstraint(self):
        myDateConstrainedWithEnvVar("-d", "next tuesday")
        compss_barrier()
