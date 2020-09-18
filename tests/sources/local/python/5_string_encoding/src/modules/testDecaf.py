#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
"""

# Imports
import os
import unittest
from pycompss.api.task import task
from pycompss.api.decaf import decaf
from pycompss.api.api import TaskGroup
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *
from pycompss.api.exceptions import COMPSsException


@decaf(df_script=os.getcwd() + "/src/scripts/checkString.py")
@task(returns=1, param=IN)
def basic(param):
    pass


@decaf(df_script=os.getcwd() + "/src/scripts/failFirstTime.py", fail_by_exit_value=True)
@task(returns=str, param=IN)
def fail_first_time(param):
    pass


class testDecaf(unittest.TestCase):

    def testDecafBasic(self):
        param = "testing string"
        expected = 0
        result = basic(param)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testDecafBasicMultipletimes(self):
        times = 10
        param = "testing string"
        expected = [0] * times
        result = []
        for i in range(times):
            result.append(basic(param))
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testDecafResubmission(self):
        check_file = "/tmp/has_failed.task"
        if os.path.isfile(check_file):
            os.remove(check_file)  # remove if exists
        param = "testing string"
        expected = 0
        result = fail_first_time(param)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)
