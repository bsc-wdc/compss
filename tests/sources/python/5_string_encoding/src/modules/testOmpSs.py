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
from pycompss.api.ompss import ompss
from pycompss.api.api import TaskGroup
from pycompss.api.api import compss_wait_on
from pycompss.api.api import compss_wait_on_file
from pycompss.api.parameter import *
from pycompss.api.exceptions import COMPSsException


@ompss(binary="./checkString.sh", working_dir=os.getcwd() + '/src/scripts/')
@task(returns=1, param=IN)
def basic(param):
    pass


@ompss(binary="./checkString.sh", working_dir=os.getcwd() + '/src/scripts/')
@task(returns=1, param=IN, out_result={Type: FILE_OUT_STDOUT}, err_result={Type: FILE_OUT_STDERR})
def advanced(param, out_result, err_result):
    pass


@ompss(binary="./failFirstTime.sh", working_dir=os.getcwd() + '/src/scripts/', fail_by_exit_value=True)
@task(returns=str, param=IN)
def fail_first_time(param):
    pass


class testOmpSs(unittest.TestCase):

    def testOmpSsBasic(self):
        param = "testing string"
        expected = 0
        result = basic(param)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testOmpSsBasicMultipletimes(self):
        times = 10
        param = "testing string"
        expected = [0] * times
        result = []
        for i in range(times):
            result.append(basic(param))
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testOmpSsAdvanced(self):
        # Checks that stdout and stderr are correctly set
        param = "testing string"
        outfile = "outfile.out"
        errfile = "errfile.err"
        expected = 0
        result = advanced(param, outfile, errfile)
        result = compss_wait_on(result)
        compss_wait_on_file(outfile)
        compss_wait_on_file(errfile)
        if not os.path.isfile(outfile):
            # out file does not exist ==> error
            raise Exception("Expected output file does not exist.")
        if not os.path.isfile(errfile):
            # err file does not exist ==> error
            raise Exception("Expected error file does not exist.")
        os.remove(outfile)
        os.remove(errfile)
        self.assertEqual(result, expected)

    def testOmpSsResubmission(self):
        check_file = "/tmp/has_failed.task"
        if os.path.isfile(check_file):
            os.remove(check_file)  # remove if exists
        param = "testing string"
        expected = 0
        result = fail_first_time(param)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)
