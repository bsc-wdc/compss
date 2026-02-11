#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs NUMBA Syntax with Constraints Testbench
================================================
"""

# Imports
import unittest

from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from .external import external
from .external import externalc
from .external import example

import numpy as np

from numba import jit
from numba import njit


@constraint(computing_units="2")
@task(returns=1, numba=True)
def add(value1, value2):
    return value1 + value2


@constraint(computing_units="2")
@task(returns=1, numba={'cache': True, 'nopython': True})
def multiply(value1, value2):
    return value1 * value2


@constraint(computing_units="2")
@task(returns=1)
@jit
def increment1(value):
    return value + 1


@constraint(computing_units="2")
@task(returns=1)
@jit(nopython=True)
def increment2(value):
    return value + 1


@constraint(computing_units="2")
@task(returns=1, numba='jit')
def addJit(value1, value2):
    return value1 + value2


@constraint(computing_units="2")
@task(returns=1)
@njit
def decrement1(value):
    return value - 1


@constraint(computing_units="2")
@task(returns=1)
@njit(cache=True)
def decrement2(value):
    return value - 1


@constraint(computing_units="2")
@task(returns=1, numba='njit')
def subtractNjit(value1, value2):
    return value1 - value2


@constraint(computing_units="2")
@task(returns=1,
      numba='vectorize',
      numba_signature=['float64(float64, float64)'],
      numba_flags={'target': 'cpu'})
def vectorized_add(x, y):
    return x + y


@constraint(computing_units="2")
@task(returns=1,
      numba='guvectorize',
      numba_signature=['(int64[:], int64, int64[:])'],
      numba_declaration='(n),()->(n)')
def guvectorized_add(x, y, res):
    for i in range(x.shape[0]):
        res[i] = x[i] + y


@constraint(computing_units="2")
@task(returns=1,
      numba='stencil')
def kernel1(a):
    return 0.25 * (a[0, 1] + a[1, 0] + a[0, -1] + a[-1, 0])


@constraint(computing_units="2")
@task(returns=1,
      numba='cfunc',
      numba_signature='float64(float64)')
def integrand(t):
    return np.exp(-t) / t**2


class testSyntaxWConstraintsNumba(unittest.TestCase):

    ##############################################
    # Supported syntax with constraints tests
    ##############################################

    def testNumbaBase(self):
        result = add(2, 3)
        result = compss_wait_on(result)
        self.assertEqual(result, 5)

    def testNumbaBaseParameter(self):
        result = multiply(2, 3)
        result = compss_wait_on(result)
        self.assertEqual(result, 6)

    def testJit1(self):
        result = increment1(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 2)

    def testJit2(self):
        result = increment2(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 2)

    def testJit3(self):
        result = addJit(4, 5)
        result = compss_wait_on(result)
        self.assertEqual(result, 9)

    def testNJit1(self):
        result = decrement1(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 0)

    def testNJit2(self):
        result = decrement2(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 0)

    def testNJit2(self):
        result = subtractNjit(5, 4)
        result = compss_wait_on(result)
        self.assertEqual(result, 1)

    def testVectorize(self):
        matrix = np.arange(6)
        result = vectorized_add(matrix, matrix)
        result = compss_wait_on(result)
        self.assertEqual(result.tolist(), [0.0, 2.0, 4.0, 6.0, 8.0, 10.0])

    def testGuvectorize(self):
        matrix = np.arange(5)
        result = guvectorized_add(matrix, 2)
        result = compss_wait_on(result)
        self.assertEqual(result.tolist(), [2, 3, 4, 5, 6])

    def testStencil(self):
        matrix = np.arange(25).reshape((5, 5))
        result = kernel1(matrix)
        result = compss_wait_on(result)
        self.assertEqual(result.tolist(), [[0, 0, 0, 0, 0],
                                           [0, 6, 7, 8, 0],
                                           [0, 11, 12, 13, 0],
                                           [0, 16, 17, 18, 0],
                                           [0, 0, 0, 0, 0]])

    def testCfunc(self):
        result = integrand(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 5.152884056096394e-12)

    def testExternalFunc(self):
        result = external(8)
        result = compss_wait_on(result)
        self.assertEqual(result, 4)

    def testExternalFuncConstrained(self):
        result = externalc(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 5)

    def testExternalClassFuncs(self):
        obj = example(10)
        v = obj.subtract(obj.v, 5)
        v = obj.calcul(v, 50)
        result = compss_wait_on(v)
        self.assertEqual(result, 30)
