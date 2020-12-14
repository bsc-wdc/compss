#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs default object value Testbench
=======================================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.on_failure import on_failure
from pycompss.api.api import compss_wait_on


# Python 2 and 3 compliant
@on_failure(management="IGNORE", returns=0)
@task(returns=int)
def i_will_fail(value):
    raise Exception("Task failed on purpose to test returns default.")
    return value + 1

# Python 3 with type hinting
# @on_failure(management="IGNORE", returns=0)
# @task()
# def i_will_fail_type_hint(value: int) -> int:
#     raise Exception("Task failed on purpose to test returns default.")
#     return value + 1


# class foo(object):
#     def __init__(self, value):
#         self.value = value
#     def increment(self):
#         self.value += 1
#     def get_value(self):
#         return self.value
#
# @on_failure(management="IGNORE", returns=0, my_object=foo(0))
# @task(returns=int, my_object=OUT)
# def i_will_fail_out_parameter(value, my_object=foo(1)):
#     raise Exception("Task failed on purpose to test out parameter.")
#     return value + 1


class testDefaultObjectValue(unittest.TestCase):

    def test_default_return_value(self):
        initial_value = 1
        result = i_will_fail(initial_value)
        result = compss_wait_on(result)
        assert result == 0, "ERROR: Result error (%s != 0)" % result
