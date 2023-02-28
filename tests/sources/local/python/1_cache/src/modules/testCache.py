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
from pycompss.api.on_failure import on_failure
from pycompss.api.parameter import *
from pycompss.api.parameter import Cache
from pycompss.api.api import compss_wait_on


@on_failure(management ='FAIL')
@task(returns=np.ndarray)
def generate_np_array():
    return np.random.randint(10, size=(4,4))


@on_failure(management ='FAIL')
@task(returns=list)
def generate_list():
    return np.random.randint(10, size=(4)).tolist()


@on_failure(management ='FAIL')
@task(returns=list)
def generate_tuple():
    return tuple(np.random.randint(10, size=(4)))


@on_failure(management ='FAIL')
@task(returns=int)
def compute_2d(nparray):
    return sum(sum(nparray + 1))


@on_failure(management ='FAIL')
@task(returns=int)
def compute_1d(nparray):
    result = 0
    for x in range(len(nparray)):
        result = result + nparray[x] + 1
    return result


@on_failure(management ='FAIL')
@task(returns=1, value={Cache: False})
def store_in_cache(value):
    # It will cache the return
    return value


@on_failure(management ='FAIL')
@task(value=INOUT)
def increment(value):
    # Increment cached value
    for x in range(len(value)):
        for y in range(len(value[x])):
            value[x][y] = value[x][y] + 1


@on_failure(management ='FAIL')
@task(value=INOUT)
def increment_lst(value):
    # Increment cached value
    for x in range(len(value)):
        value[x] = value[x] + 1


@on_failure(management ='FAIL')
@task(list={Type: COLLECTION_IN, Cache: True}, cache_returns=True)
def numpy_col_in(list):
    return list


@on_failure(management ='FAIL')
@task(list={Type: COLLECTION_OUT, Cache: True})
def numpy_col_out(list):
    blocks = np.array_split(np.arange(25), 5)
    for i, block in enumerate(blocks):
        list[i] = block


@on_failure(management ='FAIL')
@task(blocks={Type: COLLECTION_INOUT, Cache: True})
def numpy_col_inout(blocks):
    for i, block in enumerate(blocks):
        blocks[i] = -block


class testCache(unittest.TestCase):
    """
    Currently evaluates that the worker does not crash with cache enabled.
    Checks the accesses to cache globally afterwards from the logs.

    TODO: Check cache usage.
    """

    def testReturnReuseNPArray(self):
        to_cache = generate_np_array()
        results = []
        for _ in range(20):
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


    def testNumpyCollectionIN(self):
        blocks = np.array_split(np.arange(25), 5)
        result = numpy_col_in(blocks)
        result = np.array(compss_wait_on(result))

        self.assertEqual(np.all(np.array(blocks) == result), True)

    
    def testNumpyCollectionOUT(self):
        blocks = [object() for _ in range(5)]
        numpy_col_out(blocks)
        np_blocks = np.array_split(np.arange(25), 5)

        result = np.array(compss_wait_on(blocks))
        self.assertEqual(np.all(np.array(np_blocks) == result), True)


    def testNumpyCollectionINOUT(self):
        blocks = np.array_split(np.arange(25), 5)
        numpy_col_inout(blocks)
        np_blocks = np.array_split(-np.arange(25), 5)

        result = np.array(compss_wait_on(blocks))
        self.assertEqual(np.all(np.array(np_blocks) == result), True)


    # Tuple can not be INOUT
