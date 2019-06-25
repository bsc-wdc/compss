#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs NUMBA Performance Testbench
====================================
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


@task(returns=1, numba='njit')  # @njit
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
    # These tests run a set of tasks that are not using numba and measure
    # the time taken to process them.
    # Then runs a set of the same amount but with tasks using numba, also
    # measuring the time to process them.
    # The evaluation is performed in two ways:
    #    - The time to process the set of tasks has to be lower with numba tasks
    #    - The result of the last task is compared between implementations to
    #      ensure that they retrieve the same result.
    # Considerations:
    #     - Numba requires a compilation time. For this reason it is submitted
    #       a set of tasks. More than 4 is required since there are 4 workers.
    #     - Time to bring the result object is not included since numba only
    #       optimizes computation time.
    ##############################################

    def testNumbaJit(self):
        # Launch a first task to start the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        repetitions = 100
        # Test without jit
        no_jit_start_time = time.time()
        for i in range(repetitions):
            out = calcul(np.arange(1000000).reshape(1000, 1000))
        compss_barrier()
        no_jit_time = time.time() - no_jit_start_time
        last_result = compss_wait_on(out)
        # Test jit
        jit_start_time = time.time()
        for i in range(repetitions):
            out_jit = calcul_jit(np.arange(1000000).reshape(1000, 1000))
        compss_barrier()
        jit_time = time.time() - jit_start_time
        last_result_jit = compss_wait_on(out_jit)
        # Check results
        print("NO JIT TIME: " + str(no_jit_time))
        print("JIT TIME   : " + str(jit_time))
        self.assertGreater(no_jit_time, jit_time)
        # Just check the last result of both tasks to ensure that the
        # results are equal
        self.assertListEqual(last_result.tolist(), last_result_jit.tolist())

    def testNumbaNjit(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        repetitions = 200
        # Test without njit
        no_njit_start_time = time.time()
        for i in range(repetitions):
            # out = calcul2(np.ones((1000, 1000)))
            out = calcul2(np.arange(1000000).reshape(1000, 1000))
        compss_barrier()
        no_njit_time = time.time() - no_njit_start_time
        last_result = compss_wait_on(out)
        # Test njit
        njit_start_time = time.time()
        for i in range(repetitions):
            # out_njit = calcul2_njit(np.ones((1000, 1000)))
            out_njit = calcul2_njit(np.arange(1000000).reshape(1000, 1000))
        compss_barrier()
        njit_time = time.time() - njit_start_time
        last_result_njit = compss_wait_on(out_njit)
        # Check results
        print("NO NJIT TIME: " + str(no_njit_time))
        print("NJIT TIME   : " + str(njit_time))
        self.assertGreater(no_njit_time, njit_time)
        # Just check the last result of both tasks to ensure that the
        # results are equal
        self.assertListEqual(last_result.tolist(), last_result_njit.tolist())

    def testNumbaVectorize(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        repetitions = 100
        # Test vectorize
        no_vectorize_start_time = time.time()
        for i in range(repetitions):
            out = calcul3(np.ones((500000)))
        compss_barrier()
        no_vectorize_time = time.time() - no_vectorize_start_time
        last_result = compss_wait_on(out)
        # Test vectorize
        vectorize_start_time = time.time()
        for i in range(repetitions):
            out_vectorize = calcul3_vectorize(np.ones((500000)))
        compss_barrier()
        vectorize_time = time.time() - vectorize_start_time
        last_result_vectorize = compss_wait_on(out_vectorize)
        # Check results
        print("NO VECTORIZE TIME: " + str(no_vectorize_time))
        print("VECTORIZE TIME   : " + str(vectorize_time))
        self.assertGreater(no_vectorize_time, vectorize_time)
        # Just check the last result of both tasks to ensure that the
        # results are equal
        self.assertListEqual(last_result.tolist(), last_result_vectorize.tolist())

    def testNumbaGuvectorize(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        repetitions = 1  # Even a single one is faster (considering the compilation time)
        # Test without guvectorize
        no_guvectorize_start_time = time.time()
        for i in range(repetitions):
            out = calcul4(np.arange(800000), 2)
        compss_barrier()
        no_guvectorize_time = time.time() - no_guvectorize_start_time
        last_result = compss_wait_on(out)
        # Test with guvectorize
        guvectorize_start_time = time.time()
        for i in range(repetitions):
            out = calcul4_guvectorize(np.arange(800000), 2)
        compss_barrier()
        guvectorize_time = time.time() - guvectorize_start_time
        last_result_guvectorize = compss_wait_on(out)
        # Check results
        print("NO GUVECTORIZE TIME: " + str(no_guvectorize_time))
        print("GUVECTORIZE TIME   : " + str(guvectorize_time))
        self.assertGreater(no_guvectorize_time, guvectorize_time)
        self.assertListEqual(last_result.tolist(), last_result_guvectorize.tolist())

    @unittest.skip("Do not run with the previous tests -> the result fails, numba issue?. Individually works.")
    def testNumbaStencil(self):
        # Launch a first task to heat the worker if this test is run individually
        heat = increment(1)
        heat = compss_wait_on(heat)
        repetitions = 10
        # Test without stencil
        no_stencil_start_time = time.time()
        for i in range(repetitions):
            # out = calcul5(np.arange(1000000).reshape((1000, 1000)))
            out = calcul5(np.ones((1000, 1000)))
        compss_barrier()
        no_stencil_time = time.time() - no_stencil_start_time
        last_result = compss_wait_on(out)
        # Test with stencil
        stencil_start_time = time.time()
        for i in range(repetitions):
            # out_stencil = calcul5_stencil(np.arange(1000000).reshape((1000, 1000)))
            out_stencil = calcul5_stencil(np.ones((1000, 1000)))
        compss_barrier()
        stencil_time = time.time() - stencil_start_time
        last_result_stencil = compss_wait_on(out_stencil)
        # Check results
        print("NO STENCIL TIME: " + str(no_stencil_time))
        print("STENCIL TIME   : " + str(stencil_time))
        self.assertGreater(no_stencil_time, stencil_time)
        # Just check the last result of both tasks to ensure that the
        # results are equal
        self.assertListEqual(last_result.tolist(), last_result_stencil.tolist())
