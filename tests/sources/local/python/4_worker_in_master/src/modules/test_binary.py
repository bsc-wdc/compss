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
from pycompss.api.parameter import *
from pycompss.api.binary import binary


@binary(binary="echo", working_dir=".")
@task(result={Type: FILE_OUT_STDOUT})
def create_input(content, result):
    pass


@binary(binary="sed", working_dir=".")
@task(file=FILE_INOUT)
def mySedINOUT(flag, expression, file):
    pass


class TestBinary(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in primitive-type
    parameters.
    """

    def test_binary_INOUT(self):
        message = 'Hi, this is a test.'
        f = "myfile.txt"
        create_input(message, f)
        mySedINOUT('-i', 's/Hi/HELLO/g', f)
        with compss_open(f, "r") as f_result:
            content_r = f_result.read()
        # Check if there are no Hi words, and instead there is HELLO
        if 'Hi' in content_r:
            self.fail("INOUT File failed.")
        if not 'HELLO' in content_r:
            self.fail("INOUT File failed.")
