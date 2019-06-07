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
import random

from numba import jit
from numba import njit
from numba import generated_jit, types


@constraint(computing_units=4)
@task(returns=1)
def increment(value):
    return value + 1


@task(returns=1)
def calcul(a):
    trace = 0
    for i in range(a.shape[0]):
        trace += np.tanh(a[i, i])
    return a + trace


@task(returns=1, numba={'nopython': True})  # flag to make it go fast
def calcul_jit(a):
    trace = 0
    for i in range(a.shape[0]):
        trace += np.tanh(a[i, i])
    return a + trace


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
def calcul5(x):
    out = np.empty_like(x)
    for i in range(1, x.shape[0] - 1):
        for j in range(1, x.shape[1] - 1):
            out[i, j] = (x[i + -1, j + -1] + x[i + -1, j + 0] + x[i + -1, j + 1] +
                         x[i +  0, j + -1] + x[i +  0, j + 0] + x[i +  0, j + 1] +
                         x[i +  1, j + -1] + x[i +  1, j + 0] + x[i +  1, j + 1]) // 9
    return out


@task(returns=1,
      numba='stencil')
def calcul5_stencil(x):
    return (x[-1, -1] + x[-1, 0] + x[-1, 1] +
            x[ 0, -1] + x[ 0, 0] + x[ 0, 1] +
            x[ 1, -1] + x[ 1, 0] + x[ 1, 1]) // 9


class testPerformanceNumba(unittest.TestCase):

    ##############################################
    # Performance tests
    ##############################################

    def testNumbaJit(self):
        # Launch a first task to start the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Run one task without jit on each worker process
        heat_1 = calcul(np.arange(1000000).reshape(1000, 1000))
        heat_2 = calcul(np.arange(1000000).reshape(1000, 1000))
        heat_3 = calcul(np.arange(1000000).reshape(1000, 1000))
        heat_4 = calcul(np.arange(1000000).reshape(1000, 1000))
        compss_barrier()
        # Test without jit
        no_jit_start_time = time.time()
        result = calcul(np.arange(1000000).reshape(1000, 1000))
        result = compss_wait_on(result)
        no_jit_time = time.time() - no_jit_start_time
        # Force numba compilation in the 4 worker processes
        heat_1 = calcul_jit(np.arange(1000000).reshape(1000, 1000))
        heat_2 = calcul_jit(np.arange(1000000).reshape(1000, 1000))
        heat_3 = calcul_jit(np.arange(1000000).reshape(1000, 1000))
        heat_4 = calcul_jit(np.arange(1000000).reshape(1000, 1000))
        compss_barrier()
        # Test jit without considering compilation time
        jit_start_time = time.time()
        result_jit = calcul_jit(np.arange(1000000).reshape(1000, 1000))
        result_jit = compss_wait_on(result_jit)
        jit_time = time.time() - jit_start_time
        # Check results
        print("NO JIT TIME: " + str(no_jit_time))
        print("JIT TIME   : " + str(jit_time))
        self.assertGreater(no_jit_time, jit_time)
        self.assertListEqual(result.tolist(), result_jit.tolist())

    def testNumbaNjit(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Run one task without njit on each worker process
        heat_1 = calcul2(np.ones((500, 500)))
        heat_2 = calcul2(np.ones((500, 500)))
        heat_3 = calcul2(np.ones((500, 500)))
        heat_4 = calcul2(np.ones((500, 500)))
        compss_barrier()
        # Test without njit
        no_njit_start_time = time.time()
        result = calcul2(np.ones((500, 500)))
        result = compss_wait_on(result)
        no_njit_time = time.time() - no_njit_start_time
        # Force numba compilation in the 4 worker processes
        heat_1 = calcul2_njit(np.ones((500, 500)))
        heat_2 = calcul2_njit(np.ones((500, 500)))
        heat_3 = calcul2_njit(np.ones((500, 500)))
        heat_4 = calcul2_njit(np.ones((500, 500)))
        compss_barrier()
        # Test njit without considering compilation time
        njit_start_time = time.time()
        result_njit = calcul2_njit(np.ones((500, 500)))
        result_njit = compss_wait_on(result_njit)
        njit_time = time.time() - njit_start_time
        # Check results
        print("NO NJIT TIME: " + str(no_njit_time))
        print("NJIT TIME   : " + str(njit_time))
        self.assertGreater(no_njit_time, njit_time)
        self.assertListEqual(result.tolist(), result_njit.tolist())

    def testNumbaVectorize(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Run one task without vectorize on each worker process
        heat_1 = calcul3(np.ones((500000)))
        heat_2 = calcul3(np.ones((500000)))
        heat_3 = calcul3(np.ones((500000)))
        heat_4 = calcul3(np.ones((500000)))
        compss_barrier()
        # Test vectorize
        no_vectorize_start_time = time.time()
        result = calcul3(np.ones((500000)))
        result = compss_wait_on(result)
        no_vectorize_time = time.time() - no_vectorize_start_time
        # Force numba compilation in the 4 worker processes
        heat_1 = calcul3_vectorize(np.ones((500000)))
        heat_2 = calcul3_vectorize(np.ones((500000)))
        heat_3 = calcul3_vectorize(np.ones((500000)))
        heat_4 = calcul3_vectorize(np.ones((500000)))
        compss_barrier()
        # Test vectorize without considering compilation time
        vectorize_start_time = time.time()
        result_vectorized = calcul3_vectorize(np.ones((500000)))
        result_vectorized = compss_wait_on(result_vectorized)
        vectorize_time = time.time() - vectorize_start_time
        # Check results
        print("NO VECTORIZE TIME: " + str(no_vectorize_time))
        print("VECTORIZE TIME   : " + str(vectorize_time))
        self.assertGreater(no_vectorize_time, vectorize_time)
        self.assertListEqual(result.tolist(), result_vectorized.tolist())

    def testNumbaGuvectorize(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Test without guvectorize
        no_guvectorize_start_time = time.time()
        result = calcul4(np.arange(800000), 2)
        result = compss_wait_on(result)
        no_guvectorize_time = time.time() - no_guvectorize_start_time
        # Test with guvectorize
        guvectorize_start_time = time.time()
        result_guvectorized = calcul4_guvectorize(np.arange(800000), 2)
        result_guvectorized = compss_wait_on(result_guvectorized)
        guvectorize_time = time.time() - guvectorize_start_time
        # Check results
        # NOTE: This use case is faster even considering the compilation time
        print("NO GUVECTORIZE TIME: " + str(no_guvectorize_time))
        print("GUVECTORIZE TIME   : " + str(guvectorize_time))
        self.assertGreater(no_guvectorize_time, guvectorize_time)
        self.assertListEqual(result.tolist(), result_guvectorized.tolist())

    @unittest.skip("Use case works but performance improvement too weak")
    def testNumbaStencil(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        # Run one task without vectorize on each worker process
        heat_1 = calcul5(np.arange(1000000).reshape((1000, 1000)))
        heat_2 = calcul5(np.arange(1000000).reshape((1000, 1000)))
        heat_3 = calcul5(np.arange(1000000).reshape((1000, 1000)))
        heat_4 = calcul5(np.arange(1000000).reshape((1000, 1000)))
        compss_barrier()
        # Test without stencil
        no_stencil_start_time = time.time()
        result = calcul5(np.arange(1000000).reshape((1000, 1000)))
        result = compss_wait_on(result)
        no_stencil_time = time.time() - no_stencil_start_time
        # Force numba compilation in the 4 worker processes
        heat_1 = calcul5_stencil(np.arange(1000000).reshape((1000, 1000)))
        heat_2 = calcul5_stencil(np.arange(1000000).reshape((1000, 1000)))
        heat_3 = calcul5_stencil(np.arange(1000000).reshape((1000, 1000)))
        heat_4 = calcul5_stencil(np.arange(1000000).reshape((1000, 1000)))
        # Test witht stencil
        stencil_start_time = time.time()
        result_stencil = calcul5_stencil(np.arange(1000000).reshape((1000, 1000)))
        result_stencil = compss_wait_on(result_stencil)
        stencil_time = time.time() - stencil_start_time
        # Check results
        print("NO STENCIL TIME: " + str(no_stencil_time))
        print("STENCIL TIME   : " + str(stencil_time))
        self.assertGreater(no_stencil_time, stencil_time)
        self.assertListEqual(result.tolist(), result_stencil.tolist())
