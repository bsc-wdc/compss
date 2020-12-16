#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs default object value Testbench
=======================================
"""

# Imports
import os
import unittest

from pycompss.api.task import task
from pycompss.api.parameter import FILE_INOUT
from pycompss.api.parameter import FILE_OUT
from pycompss.api.on_failure import on_failure
from pycompss.api.api import compss_wait_on
from pycompss.api.api import compss_open
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_delete_file


def generate_empty(msg, name):
    empty_file = "/tmp/empty_file_" + name
    with open(empty_file, 'w') as f:
        f.write("EMPTY FILE " + msg)
    return empty_file


@on_failure(management="IGNORE", value=generate_empty("INOUT", "inout.tmp"))
@task(value=FILE_INOUT)
def i_will_fail_file_inout(value):
    raise Exception("Task failed on purpose to test FILE_INOUT default value.")


@on_failure(management="IGNORE", value=generate_empty("OUT", "out.tmp"))
@task(value=FILE_OUT)
def i_will_fail_file_out(value):
    raise Exception("Task failed on purpose to test FILE_OUT default value.")



class testDefaultFileValue(unittest.TestCase):


    def test_default_file_inout(self):
        initial_file = "my_inout_file.txt"
        with open(initial_file, 'w') as f:
            f.write("INITIAL FILE INOUT")
        i_will_fail_file_inout(initial_file)
        with compss_open(initial_file) as f:
            content = f.read()
        assert content == "EMPTY FILE INOUT", "ERROR: Wrong file inout (%s != EMPTY FILE INOUT)" % content
        compss_delete_file(initial_file)

    def test_default_file_out(self):
        initial_file = "my_out_file.txt"
        i_will_fail_file_out(initial_file)
        with compss_open(initial_file) as f:
            content = f.read()
        assert content == "EMPTY FILE OUT", "ERROR: Wrong file inout (%s != EMPTY FILE OUT)" % content
        compss_delete_file(initial_file)
