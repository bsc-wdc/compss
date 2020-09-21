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
from pycompss.api.api import TaskGroup
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *
from pycompss.api.exceptions import COMPSsException


@task(returns=str, param=IN)
def basic(param):
    assert param == "testing string"
    return param


@task(returns=str, param=IN)
def fail_first_time(param):
    check_file = "/tmp/has_failed.task"
    assert param == "testing string"
    if os.path.isfile(check_file):
        # If file exists == OK
        os.remove(check_file)  # Clean on Resubmission
        return param
    else:
        # If file does not exist == FAIL
        with open(check_file, 'w') as f:
            f.write(param)
        raise Exception("Intended exception")


class testStandard(unittest.TestCase):

    def testStandardBasic(self):
        param = "testing string"
        expected = param
        result = basic(param)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testStandardBasicMultipletimes(self):
        times = 10
        param = "testing string"
        expected = [param] * times
        result = []
        for i in range(times):
            result.append(basic(param))
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testStandardResubmission(self):
        check_file = "/tmp/has_failed.task"
        if os.path.isfile(check_file):
            os.remove(check_file)  # remove if exists
        param = "testing string"
        expected = param
        result = fail_first_time(param)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)
