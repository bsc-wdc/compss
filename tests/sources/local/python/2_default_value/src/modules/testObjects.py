#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs default object value Testbench
=======================================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.parameter import Type
from pycompss.api.parameter import Direction
from pycompss.api.parameter import Default
from pycompss.api.api import compss_wait_on


# Python 2 and 3 compliant
@task(returns={Type:int, Default:0}, on_failure="IGNORE")
def i_will_fail(value):
    raise Exception("Task failed on purpose to test returns default.")
    return value + 1

# Python 3 with type hinting
@task(returns={Default:0}, on_failure="IGNORE")
def i_will_fail_type_hint(value: int) -> int:
    raise Exception("Task failed on purpose to test returns default.")
    return value + 1


class foo(object):
    def __init__(self, value):
        self.value = value
    def increment(self):
        self.value += 1
    def get_value(self):
        return self.value

# Differentiate from parameter default and parameter out default
@task(returns=int, on_failure="IGNORE", my_object={Direction:OUT, Default: foo(0)})
def i_will_fail_out_parameter(value, my_object=foo(1)):
    raise Exception("Task failed on purpose to test out parameter.")
    return value + 1


class testDefaultObjectValue(unittest.TestCase):

    def test_default_return_value(self):
        initial_value = 1
        result = i_will_fail(initial_value)
        result = compss_wait_on(result)
        assert result == 0, "ERROR: Result error (%s != 0)" % result
