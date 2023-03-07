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
from pycompss.api.api import compss_barrier, compss_open, compss_wait_on, TaskGroup
from pycompss.api.binary import binary
from pycompss.api.exceptions import COMPSsException
from pycompss.api.constraint import constraint


@binary(binary="date")
@task(returns=str)
@task()
def myDate(dprefix, param):
    pass


@binary(binary="date")
@task(returns=str)
def myDateDef(dprefix="-d", param="next wednesday"):
    pass


@binary(binary="${REMOTE_BINARY}")
@task(returns=int)
def checkStringParam(string_param):
    pass

class testBinaryDecorator(unittest.TestCase):

    def testScript(self):
        string_param = "This is a string."
        exit_value1 = checkStringParam(string_param)
        exit_value1 = compss_wait_on(exit_value1)
        self.assertEqual(exit_value1, 0)
    def testFunctionalUsage(self):
        compssDay = myDate("-d", "friday")
        compssDay = compss_wait_on(compssDay)
        #self.assertEqual("Fri",compssDay[0:3])

    def testDefaultValue(self):
        compssDay = myDateDef()
        compssDay = compss_wait_on(compssDay)
        #self.assertEqual("Wed",compssDay[0:3])
