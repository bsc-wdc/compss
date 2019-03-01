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
from pycompss.functions.reduce import merge_reduce, simple_reduce
from pycompss.functions.map import map


@task(returns=int)
def sumTask(x, y):
    return x + y


class testFunctions(unittest.TestCase):

    def setUp(self):
        self.data = [1, 2, 3, 4]
        self.tuples = [(1, [4, 5, 6]), (2, [6, 7, 8]), (1, [1, 2, 3]), (2, [1, 1, 1])]
        self.dicts = dict(self.tuples)
        self.lambdaFunction = lambda x, y: x + y
        self.methodFunction = sumTask

    def test_merge_reduce_seq(self):
        res = merge_reduce(self.lambdaFunction, self.data)
        self.assertEqual(res, sum(self.data))

    def test_simple_reduce_seq(self):
        res = simple_reduce(self.lambdaFunction, self.data)
        self.assertEqual(res, sum(self.data))

    def test_simple_reduce(self):
        from pycompss.api.api import compss_wait_on
        res = simple_reduce(self.methodFunction, self.data)
        res = compss_wait_on(res)
        self.assertEqual(res, sum(self.data))

    def test_merge_reduce(self):
        from pycompss.api.api import compss_wait_on
        res = merge_reduce(self.methodFunction, self.data)
        res = compss_wait_on(res)
        self.assertEqual(res, sum(self.data))

    @unittest.skip("not implemented yet")
    def test_merge_reduceByClass_tuples(self):
        res = merge_reduceByClass(self.methodFunction, self.tuples)
        val = [[4, 5, 6, 1, 2, 3], [6, 7, 8, 1, 1, 1]]
        self.assertEqual(res, val)

    @unittest.skip("not implemented yet")
    def test_merge_reduceByClass_dicts(self):
        res = merge_reduceByClass(self.methodFunction, self.dicts)
        val = [[4, 5, 6, 1, 2, 3], [6, 7, 8, 1, 1, 1]]
        self.assertEqual(res, val)

    def test_map_seq(self):
        res = list(map(self.lambdaFunction, self.data, self.data))
        if sys.version_info >= (3, 0):
            cor = list(map(self.lambdaFunction, self.data, self.data))
        else:
            cor = map(self.lambdaFunction, self.data, self.data)
        self.assertSequenceEqual(res, cor)

    def test_map(self):
        from pycompss.api.api import compss_wait_on
        res = list(map(self.methodFunction, self.data, self.data))
        res = compss_wait_on(res)
        if sys.version_info >= (3, 0):
            cor = list(map(self.methodFunction, self.data, self.data))
        else:
            cor = map(self.methodFunction, self.data, self.data)
        cor = compss_wait_on(cor)
        self.assertSequenceEqual(res, cor)

    def tearDown(self):
        self.data = None
        self.tuples = None
        self.dicts = None
        self.lambdaFunction = None
        self.methodFunction = None
