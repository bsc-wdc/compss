#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import sys

from pycompss.api.task import task
from pycompss.functions.reduce import merge_reduce
from pycompss.api.api import compss_wait_on

if sys.version_info >= (3, 0):
    from functools import reduce


@task(returns=int)
def sumTask(x, y):
    return x + y

lambdaFunction = lambda x, y: x + y


class testFunctions(unittest.TestCase):

    def test_merge_reduce_seq(self):
        data = [1, 2, 3, 4]
        res = merge_reduce(lambdaFunction, data)
        self.assertEqual(res, 10)

    def test_simple_reduce_seq(self):
        data = [1, 2, 3, 4]
        res = reduce(lambdaFunction, data)
        self.assertEqual(res, 10)

    def test_simple_reduce(self):
        data = [1, 2, 3, 4]
        res = reduce(sumTask, data)
        res = compss_wait_on(res)
        self.assertEqual(res, 10)

    def test_merge_reduce(self):
        data = [1, 2, 3, 4]
        res = merge_reduce(sumTask, data)
        res = compss_wait_on(res)
        data = compss_wait_on(data)
        self.assertEqual(res, 10)

    def test_map_seq(self):
        data = [1, 2, 3, 4]
        res = list(map(lambdaFunction, data, data))
        if sys.version_info >= (3, 0):
            cor = list(map(lambdaFunction, data, data))
        else:
            cor = map(lambdaFunction, data, data)
        self.assertSequenceEqual(res, cor)

    def test_map(self):
        data = [1, 2, 3, 4]
        res = list(map(sumTask, data, data))
        res = compss_wait_on(res)
        if sys.version_info >= (3, 0):
            cor = list(map(sumTask, data, data))
        else:
            cor = map(sumTask, data, data)
        cor = compss_wait_on(cor)
        self.assertSequenceEqual(res, cor)
