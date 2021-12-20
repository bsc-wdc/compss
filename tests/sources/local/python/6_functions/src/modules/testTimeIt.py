#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest
import random

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.functions.elapsed_time import timeit

@timeit()
@task(returns=1)
def increment(value):
    return value + 1


class testTimeIt(unittest.TestCase):

    def testTimeIt(self):
        value = 1
        result = increment(value)
        result = compss_wait_on(result)
        self.assertEqual(len(result), 2)  # Check that returns a tuple with (result, timeit)
        self.assertEqual(result[0], value + 1)
        self.assertGreater(result[1], 0)
