#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs NUMBA Testbench
========================
"""

# Imports
import unittest

from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.implement import implement

import numpy as np
import time

from numba import jit
from numba import njit
from numba import generated_jit, types


@constraint(computing_units=4)
@task(returns=1)
def increment(value):
    return value + 1


@task(returns=1)
def calcul(value):
    return np.cos(value) ** 2 + np.sin(value) ** 2


@task(returns=1, numba=True)
def calcul_jit(value):
    return np.cos(value) ** 2 + np.sin(value) ** 2


@task(returns=1)
def calcul2(value):
    r = np.empty_like(value)
    n = len(value)
    for i in range(n):
        r[i] = np.cos(value[i]) ** 2 + np.sin(value[i]) ** 2
    return r


@task(returns=1, numba='njit')
def calcul2_njit(value):
    r = np.empty_like(value)
    n = len(value)
    for i in range(n):
        r[i] = np.cos(value[i]) ** 2 + np.sin(value[i]) ** 2
    return r


@task(returns=1)
def calcul3(vector):
    for i in range(len(vector)):
        vector[i] += 1
    return vector

@task(returns=1,
      numba='vectorize',
      numba_signature=["float64(float64)"],
      numba_flags={'target': 'cpu'})
def calcul3_vectorize(value):
    return value + 1


@task(returns=1)
def calcul4(vector, value):
    for i in range(len(vector)):
        vector[i] += value
    return vector

@task(returns=1,
      numba='guvectorize',
      numba_signature=['(int64[:], int64, int64[:])'],
      numba_declaration='(n),()->(n)')
def calcul4_guvectorize(x, y, res):
    for i in range(x.shape[0]):
        res[i] = x[i] + y


@task(returns=1)
def calcul5(a):
    for x in range(len(a)):
        for y in range(len(a[x])):
            a[x][y] = 0.25 * (a[x + 0, y + 1] + a[x + 1, y + 0] + a[x + 0, y - 1] + a[x -1, y + 0])
    return a


@task(returns=1,
      numba='stencil')
def calcul5_stencil(a):
    return 0.25 * (a[0, 1] + a[1, 0] + a[0, -1] + a[-1, 0])



class testPerformanceNumba(unittest.TestCase):

    ##############################################
    # Performance tests
    ##############################################

    @unittest.skip("PERFORMANCE NOT GOOD")
    def testNumbaJit(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Test jit
        no_jit_start_time = time.time()
        result = calcul(50000)
        result = compss_wait_on(result)
        no_jit_time = time.time() - no_jit_start_time
        jit_start_time = time.time()
        result_jit = calcul_jit(50000)
        result_jit = compss_wait_on(result_jit)
        jit_time = time.time() - jit_start_time
        print("NO JIT TIME: " + str(no_jit_time))
        print("JIT TIME   : " + str(jit_time))
        self.assertGreater(no_jit_time, jit_time)
        self.assertEqual(result, result_jit)

    @unittest.skip("PERFORMANCE NOT GOOD")
    def testNumbaNjit(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Test njit
        no_njit_start_time = time.time()
        result = calcul2(np.ones((500, 500)))
        result = compss_wait_on(result)
        no_njit_time = time.time() - no_njit_start_time
        njit_start_time = time.time()
        result_njit = calcul2_njit(np.ones((500, 500)))
        result_njit = compss_wait_on(result_njit)
        njit_time = time.time() - njit_start_time
        print("NO NJIT TIME: " + str(no_njit_time))
        print("NJIT TIME   : " + str(njit_time))
        self.assertGreater(no_njit_time, njit_time)
        self.assertListEqual(result.tolist(), result_njit.tolist())

    @unittest.skip("PERFORMANCE NOT GOOD")
    def testNumbaVectorize(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Test vectorize
        no_vectorize_start_time = time.time()
        result = calcul3(np.ones((5000000)))
        result = compss_wait_on(result)
        no_vectorize_time = time.time() - no_vectorize_start_time
        vectorize_start_time = time.time()
        result_vectorized = calcul3_vectorize(np.ones((5000000)))
        result_vectorized = compss_wait_on(result_vectorized)
        vectorize_time = time.time() - vectorize_start_time
        print("NO VECTORIZE TIME: " + str(no_vectorize_time))
        print("VECTORIZE TIME   : " + str(vectorize_time))
        self.assertGreater(no_vectorize_time, vectorize_time)
        self.assertListEqual(result.tolist(), result_vectorized.tolist())

    def testNumbaGuvectorize(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Test guvectorize
        no_guvectorize_start_time = time.time()
        result = calcul4(np.arange(800000), 2)
        result = compss_wait_on(result)
        no_guvectorize_time = time.time() - no_guvectorize_start_time
        guvectorize_start_time = time.time()
        result_guvectorized = calcul4_guvectorize(np.arange(800000), 2)
        result_guvectorized = compss_wait_on(result_guvectorized)
        guvectorize_time = time.time() - guvectorize_start_time
        print("NO GUVECTORIZE TIME: " + str(no_guvectorize_time))
        print("GUVECTORIZE TIME   : " + str(guvectorize_time))
        self.assertGreater(no_guvectorize_time, guvectorize_time)
        self.assertListEqual(result.tolist(), result_guvectorized.tolist())

    @unittest.skip("CALCUL5 NOT WORKING")
    def testNumbaStencil(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Test guvectorize
        no_stencil_start_time = time.time()
        result = calcul5(np.arange(100).reshape((10, 10)))
        result = compss_wait_on(result)
        no_stencil_time = time.time() - no_stencil_start_time
        stencil_start_time = time.time()
        result_stencil = calcul5_stencil(np.arange(100).reshape((10, 10)))
        result_stencil = compss_wait_on(result_stencil)
        stencil_time = time.time() - stencil_start_time
        print("NO STENCIL TIME: " + str(no_stencil_time))
        print("STENCIL TIME   : " + str(stencil_time))
        self.assertGreater(no_stencil_time, stencil_time)
        self.assertListEqual(result.tolist(), result_stencil.tolist())
