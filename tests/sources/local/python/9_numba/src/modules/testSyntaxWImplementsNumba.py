#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs NUMBA Syntax with Implements Testbench
===============================================
"""

# Imports
import unittest

from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.implement import implement
from .external import external
from .external import externalc
from .external import example

import numpy as np

from numba import jit
from numba import njit
from numba import generated_jit, types



@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def increment_slow1(value):
    return value + 1 + 1  # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="increment_slow1")
@constraint(computing_units="1")
@task(returns=1)
@jit(nopython=True)
def increment_fast1(value):
    return value + 1


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def increment_slow2(value):
    return value + 1 + 1  # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="increment_slow2")
@constraint(computing_units="1")
@task(returns=1, numba='jit')
def increment_fast2(value):
    return value + 1


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def decrement_slow1(value):
    return value - 1 + 1  # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="decrement_slow1")
@constraint(computing_units="1")
@task(returns=1)
@njit(cache=True)
def decrement_fast1(value):
    return value - 1


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def decrement_slow2(value):
    return value - 1 + 1  # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="decrement_slow2")
@constraint(computing_units="1")
@task(returns=1, numba='njit')
def decrement_fast2(value):
    return value - 1


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
@generated_jit(nopython=True)
def is_missing_slow1(x):
    return -1 # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="is_missing_slow1")
@constraint(computing_units="1")
@task(returns=1)
@generated_jit(nopython=True)
def is_missing_fast1(x):
    """
    Return True if the value is missing, False otherwise.
    """
    if isinstance(x, types.Float):
        return lambda x: np.isnan(x)
    elif isinstance(x, (types.NPDatetime, types.NPTimedelta)):
        # The corresponding Not-a-Time value
        missing = x('NaT')
        return lambda x: x == missing
    else:
        return lambda x: False


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
@generated_jit(nopython=True)
def is_missing_slow2(x):
    return -1 # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="is_missing_slow2")
@constraint(computing_units="1")
@task(returns=1, numba='generated_jit')
def is_missing_fast2(x):
    """
    Return True if the value is missing, False otherwise.
    """
    if isinstance(x, types.Float):
        return lambda x: np.isnan(x)
    elif isinstance(x, (types.NPDatetime, types.NPTimedelta)):
        # The corresponding Not-a-Time value
        missing = x('NaT')
        return lambda x: x == missing
    else:
        return lambda x: False

@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def vectorized_add_slow(x, y):
    return x + y + 1 # wrong result if chooses this

@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="vectorized_add_slow")
@constraint(computing_units="1")
@task(returns=1,
      numba='vectorize',
      numba_signature=['float64(float64, float64)'],
      numba_flags={'target': 'cpu'})
def vectorized_add_fast(x, y):
    return x + y


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def guvectorized_add_slow(x, y, res):
    return -1 # wrong result if chooses this


@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="guvectorized_add_slow")
@constraint(computing_units="1")
@task(returns=1,
      numba='guvectorize',
      numba_signature=['(int64[:], int64, int64[:])'],
      numba_declaration='(n),()->(n)')
def guvectorized_add_fast(x, y, res):
    for i in range(x.shape[0]):
        res[i] = x[i] + y


@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def kernel_slow(a):
     return -1 # wrong result if chooses this

@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="kernel_slow")
@constraint(computing_units="1")
@task(returns=1,
      numba='stencil')
def kernel_fast(a):
    return 0.25 * (a[0, 1] + a[1, 0] + a[0, -1] + a[-1, 0])

@constraint(computing_units="100")  # force to choose the implementation
@task(returns=1)
def integrand_slow(t):
     return -1 # wrong result if chooses this

@implement(source_class="modules.testSyntaxWImplementsNumba",
           method="integrand_slow")
@constraint(computing_units="1")
@task(returns=1,
      numba='cfunc',
      numba_signature='float64(float64)')
def integrand_fast(t):
    return np.exp(-t) / t**2



class testSyntaxWImplementsNumba(unittest.TestCase):

    ##############################################
    # Supported syntax with implements tests
    ##############################################

    def testJit1(self):
        result = increment_slow1(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 2)

    def testJit2(self):
        result = increment_slow2(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 2)

    def testNJit1(self):
        result = decrement_slow1(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 0)

    def testNJit2(self):
        result = decrement_slow2(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 0)

    def testGeneratedJit(self):
        result = is_missing_slow1(5)
        result = compss_wait_on(result)
        self.assertEqual(result, False)

    def testGeneratedJit2(self):
        result = is_missing_slow2(5)
        result = compss_wait_on(result)
        self.assertEqual(result, False)

    def testVectorize(self):
        matrix = np.arange(6)
        result = vectorized_add_slow(matrix, matrix)
        result = compss_wait_on(result)
        self.assertEqual(result.tolist(), [0.0, 2.0, 4.0, 6.0, 8.0, 10.0])

    def testGuvectorize(self):
        matrix = np.arange(5)
        result = guvectorized_add_slow(matrix, 2)
        result = compss_wait_on(result)
        self.assertEqual(result.tolist(), [2, 3, 4, 5, 6])

    def testStencil(self):
        matrix = np.arange(25).reshape((5, 5))
        result = kernel_slow(matrix)
        result = compss_wait_on(result)
        self.assertEqual(result.tolist(), [[0, 0, 0, 0, 0],
                                           [0, 6, 7, 8, 0],
                                           [0, 11, 12, 13, 0],
                                           [0, 16, 17, 18, 0],
                                           [0, 0, 0, 0, 0]])

    def testCfunc(self):
        result = integrand_slow(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 5.152884056096394e-12)
