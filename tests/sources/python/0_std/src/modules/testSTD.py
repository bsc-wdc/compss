#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.api import compss_wait_on
from pycompss.api.task import task


@task(returns=int)
def std_task(value):
    import hello
    hello.greet("FROM PYTHON TO C")
    return value + 1


class testSTD(unittest.TestCase):
    '''
    Check stdout and stderr
    '''

    # we have arguments
    def testStdoutStderr(self):
        result = std_task(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 2)
