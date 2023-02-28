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
from pycompss.api.api import compss_barrier
from pycompss.api.parameter import *
from pycompss.api.on_failure import on_failure


def create_ndarray(size):
    return np.zeros((size, size))


@on_failure(management ='FAIL')
@task(returns=1, obj={Cache: True}, cache_returns=False)
def increment_ndarray(obj):
    result = obj + 1
    return result


@on_failure(management ='FAIL')
@task(returns=1, cache_returns=True)
def create_ndarray_task_cache_return(size):
    return np.zeros((size, size))




class testHitMiss(unittest.TestCase):

    def testGenerateNoCacheConsumeOne(self):
        size = 4
        in_array = create_ndarray(size)
        expected = np.ones((size, size))
        # This produces one insertion into cache due to cache miss (+1 insertion)
        result = increment_ndarray(in_array)
        result = compss_wait_on(result)
        self.assertEqual(result.all(), expected.all())


    def testGenerateTaskConsumeMultiple(self):
        size = 5
        iterations = 4
        # Stores the result in cache (+1 insertion)
        in_array = create_ndarray_task_cache_return(size)
        expected = np.ones((size, size))
        results = []
        # Next 4 tasks do hit in cache
        for _ in range(iterations):
            results.append(increment_ndarray(in_array))
        results = compss_wait_on(results)
        for i in range(iterations):
            self.assertEqual(results[i].all(), expected.all())

    def testGenerateNoCacheConsumeMultiple(self):
        size = 6
        iterations = 4
        in_array = create_ndarray(size)
        expected = np.ones((size, size))
        results = []
        # This will cause only one insertion.
        # And +4 misses
        for _ in range(iterations):
            results.append(increment_ndarray(in_array))
        results = compss_wait_on(results)
        for i in range(iterations):
            self.assertEqual(results[i].all(), expected.all())
