#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
"""

# Imports
import unittest
import numpy as np
from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *
from pycompss.api.on_failure import on_failure


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True})
def supported_ndarray(param):
    return param + 1


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True})
def supported_none(param):
    return param


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True})
def supported_list(param):
    result = []
    for i in param:
        result.append(i + 1)
    return result


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True})
def supported_tuple(param):
    result = []
    for i in param:
        result.append(i + 1)
    return tuple(result)


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True}, cache_returns=False)
def supported_ndarray_no_cache_return(param):
    return param + 1


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True}, cache_returns=False)
def supported_none_no_cache_return(param):
    return param


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True}, cache_returns=False)
def supported_list_no_cache_return(param):
    result = []
    for i in param:
        result.append(i + 1)
    return result


@on_failure(management ='FAIL')
@task(returns=1, param={Cache: True}, cache_returns=False)
def supported_tuple_no_cache_return(param):
    result = []
    for i in param:
        result.append(i + 1)
    return tuple(result)


class testTypes(unittest.TestCase):

    def testCacheNumpyArray(self):
        in_array = np.zeros((2, 2))
        expected = np.ones((2, 2))
        result = supported_ndarray(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result.all(), expected.all())

    def testCacheEmptyNumpyArray(self):
        in_array = np.array([])
        result = supported_ndarray(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result.size, 0)

    def testCacheNoneNumpyArray(self):
        in_array = None
        result = supported_none(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result, None)

    def testCacheList(self):
        in_list = [1, 2, 3, 4]
        expected = [2, 3, 4, 5]
        result = supported_list(in_list)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testCacheTuple(self):
        in_tuple = (1, 2, 3, 4)
        expected = (2, 3, 4, 5)
        result = supported_tuple(in_tuple)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testCacheNumpyArray_no_cache_return(self):
        in_array = np.zeros((3, 3))
        expected = np.ones((3, 3))
        result = supported_ndarray_no_cache_return(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result.all(), expected.all())

    def testCacheEmptyNumpyArray_no_cache_return(self):
        in_array = np.array([])
        result = supported_ndarray_no_cache_return(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result.size, 0)

    def testCacheNoneNumpyArray_no_cache_return(self):
        in_array = None
        result = supported_none_no_cache_return(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result, None)

    def testCacheList_no_cache_return(self):
        in_list = [4, 3, 2, 1]
        expected = [5, 4, 3, 2]
        result = supported_list_no_cache_return(in_list)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)

    def testCacheTuple_no_cache_return(self):
        in_tuple = (4, 3, 2, 1)
        expected = (5, 4, 3, 2)
        result = supported_tuple_no_cache_return(in_tuple)
        result = compss_wait_on(result)
        self.assertEqual(result, expected)
