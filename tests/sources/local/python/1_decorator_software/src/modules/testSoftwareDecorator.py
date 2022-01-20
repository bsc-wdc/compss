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
from pycompss.api.api import compss_barrier, compss_wait_on
from pycompss.api.software import software


@software(config_file=os.getcwd() + "/src/config/mpi_basic.json")
@task()
def my_date(d_prefix, param):
    pass


@software(config_file=os.getcwd() + "/src/config/binary_basic.json")
@task()
def my_date_binary(d_prefix, param):
    pass


@software(config_file=os.getcwd() + "/src/config/mpi_constrained.json")
@task()
def my_date_constrained(d_prefix, param):
    pass


@software(config_file=os.getcwd() + "/src/config/mpi_file_in.json")
@task(file=FILE_IN)
def my_sed_in(expression, file):
    pass


@software(config_file=os.getcwd() + "/src/config/mpi_param.json")
@task(returns=int)
def mpi_with_param(string_param):
    pass


class TestSoftwareDecorator(unittest.TestCase):

    def testFunctionalUsageMPI(self):
        my_date("-d", "next friday")
        compss_barrier()

    def testFunctionalUsageBinary(self):
        my_date_binary("-d", "next saturday")
        compss_barrier()

    def testFunctionalUsageWithConstraint(self):
        my_date_constrained("-d", "next monday")
        compss_barrier()

    def testFileManagementIN(self):
        infile = "src/infile"
        my_sed_in('s/Hi/HELLO/g', infile)
        compss_barrier()

    def testStringParams(self):
        string_param = "this is a string with spaces"
        exit_value1 = mpi_with_param(string_param)
        exit_value2 = mpi_with_param(string_param)
        exit_value1 = compss_wait_on(exit_value1)
        exit_value2 = compss_wait_on(exit_value2)
        self.assertEqual(exit_value1, 0)
        self.assertEqual(exit_value2, 0)
