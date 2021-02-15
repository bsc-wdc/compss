#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import numpy as np

from pycompss.api.task import task
from pycompss.api.parameter import INOUT
from pycompss.api.parameter import Cache
from pycompss.api.api import compss_wait_on


@task(returns=np.ndarray)
def generate_np_array():
    return np.random.randint(10, size=(4,4))


@task(returns=list)
def generate_list():
    return np.random.randint(10, size=(4)).tolist()


@task(returns=list)
def generate_tuple():
    return tuple(np.random.randint(10, size=(4)))


@task(returns=int)
def compute_2d(nparray):
    return sum(sum(nparray + 1))


@task(returns=int)
def compute_1d(nparray):
    result = 0
    for x in range(len(nparray)):
        result = result + nparray[x] + 1
    return result


@task(returns=1, value={Cache: False})
def store_in_cache(value):
    # It will cache the return
    return value


@task(value=INOUT)
def increment(value):
    # Increment cached value
    for x in range(len(value)):
        for y in range(len(value[x])):
            value[x][y] = value[x][y] + 1


@task(value=INOUT)
def increment_lst(value):
    # Increment cached value
    for x in range(len(value)):
        value[x] = value[x] + 1


class testCache(unittest.TestCase):
    """
    Currently evaluates that the worker does not crash with cache enabled.
    Checks the accesses to cache globally afterwards from the logs.

    TODO: Check cache usage.
    """

    def testReturnReuseNPArray(self):
        to_cache = generate_np_array()
        results = []
        for i in range(20):
            results.append(compute_2d(to_cache))
        results = compss_wait_on(results)
        to_cache = compss_wait_on(to_cache)
        expected_result = sum(sum(to_cache + 1))
        if not all(x == expected_result for x in results):
            raise Exception("Unexpected result from task using cache with np.array.")


    def testINOUTReuseNPArray(self):
        iterations = 20
        initial = np.random.randint(10, size=(4,4))
        cached = store_in_cache(initial)
        for i in range(iterations):
            increment(cached)
        cached = compss_wait_on(cached)
        expected_result = initial + iterations
        if not (cached == expected_result).all():
            raise Exception("Unexpected result from task reusing cache with np.array.")


    def testReturnReuseList(self):
        # can not be 2D since it is not supported by SharedMemoryManager.ShareableList
        to_cache = generate_list()
        results = []
        for i in range(20):
            results.append(compute_1d(to_cache))
        results = compss_wait_on(results)
        to_cache = compss_wait_on(to_cache)
        expected_result = 0
        for x in range(len(to_cache)):
            expected_result = expected_result + to_cache[x] + 1
        if not all(x == expected_result for x in results):
            raise Exception("Unexpected result from task using cache with list.")


    def testINOUTReuseList(self):
        # can not be 2D since it is not supported by SharedMemoryManager.ShareableList
        iterations = 20
        initial = np.random.randint(10, size=(8)).tolist()
        cached = store_in_cache(initial)
        for i in range(iterations):
            increment_lst(cached)
        cached = compss_wait_on(cached)
        for i in range(iterations):
            for x in range(len(initial)):
                initial[x] = initial[x] + 1
        equals = True
        for x in range(len(initial)):
            if initial[x] != cached[x]:
                equals = False
        if not equals:
            raise Exception("Unexpected result from task reusing cache with list.")


    def testReturnReuseTuple(self):
        # can not be 2D since it is not supported by SharedMemoryManager.ShareableList
        to_cache = generate_tuple()
        results = []
        for i in range(20):
            results.append(compute_1d(to_cache))
        results = compss_wait_on(results)
        to_cache = compss_wait_on(to_cache)
        expected_result = 0
        for x in range(len(to_cache)):
            expected_result = expected_result + to_cache[x] + 1
        if not all(x == expected_result for x in results):
            raise Exception("Unexpected result from task using cache with tuple.")



    # Tuple can not be INOUT
