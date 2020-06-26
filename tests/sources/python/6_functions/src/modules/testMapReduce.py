#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import sys
import unittest
import random

if sys.version_info >= (3, 0):
    import functools
    reduce = functools.reduce

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on

from pycompss.functions.reduce import merge_reduce
from pycompss.functions.reduce import merge_n_reduce


@task(returns=1)
def increment(value):
    return value + 1

@task(returns=1)
def accumulate(a, b):
    return a + b

@task(returns=1)
def accumulate_n(*elements):
    return sum(elements)


class testMapReduce(unittest.TestCase):

    def testMapReduce(self):
        # Checks compatibility with builtin map and reduce
        initial = [1, 2, 3, 4, 5]
        partial = list(map(increment, initial))
        result = reduce(accumulate, partial)
        result = compss_wait_on(result)
        self.assertEqual(result, 20)

    def testMapMergeReduce(self):
        initial = [1, 2, 3, 4, 5]
        partial = list(map(increment, initial))
        result = merge_reduce(accumulate, partial)
        result = compss_wait_on(result)
        self.assertEqual(result, 20)

    def testMapMergeNReduce(self):
        initial = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        partial = list(map(increment, initial))
        result = merge_n_reduce(accumulate_n, 4, partial)
        result = compss_wait_on(result)
        self.assertEqual(result, 65)
