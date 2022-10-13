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
from pycompss.api.julia import julia
from pycompss.api.constraint import constraint


@julia(script="$PWD/julia/test.jl")
@task()
def myJulia():
    pass


class testJuliaDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myJulia()
        compss_barrier()
