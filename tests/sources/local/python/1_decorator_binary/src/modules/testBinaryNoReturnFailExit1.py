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
from pycompss.api.api import compss_wait_on, TaskGroup
from pycompss.api.binary import binary
from pycompss.api.exceptions import COMPSsException


@binary(binary="./checkString.sh", working_dir=os.getcwd() + '/src/scripts/', fail_by_exit_value=True)
@task()
def checkStringParam2(string_param):
    pass


class testBinaryNoReturnFailExit1(unittest.TestCase):
    def testBinaryNoReturnFailExit1(self):
        group_name = "binary"
        fail = False
        try:
            with TaskGroup(group_name, implicit_barrier=True):
                string_param = "This is a string. 1"
                exit_value = checkStringParam2(string_param)
                exit_value = compss_wait_on(exit_value)
            fail = True
        except COMPSsException as e:
            print("Captured compss exception" + str(e))
        if fail:
            raise Exception
