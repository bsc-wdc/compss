#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on
import numpy as np

@task(c = COLLECTION_IN, returns = 1)
def select_element(c, i):
    return c[i]

@task(c = {Type: COLLECTION_IN, Depth: 4}, returns = 1)
def select_element_from_matrix(c, i, j, k, l):
    return c[i][j][k][l]

@task(returns = 1)
def generate_object(seed):
    np.random.seed(seed)
    return np.random.rand(5)

@task(c = COLLECTION_INOUT)
def increase_elements(c):
    for elem in c:
        elem += 1.0

@task(e = INOUT)
def increase_element(e):
    e += 1.0

class testCollectionFunctions(unittest.TestCase):

    def testMasterGenerationIn(self):
        matrix = [
            np.random.rand(5) for _ in range(10)
        ]
        fifth_row = compss_wait_on(select_element(matrix, 4))

        self.assertTrue(
            np.allclose(
                matrix[4],
                fifth_row
            )
        )

    def testWorkerGenerationIn(self):
        matrix = [
            generate_object(i) for i in range(10)
        ]
        fifth_row = compss_wait_on(select_element(matrix, 4))

        print(fifth_row)

        self.assertTrue(
            np.allclose(
                compss_wait_on(matrix[4]),
                fifth_row
            )
        )

    def testDepthWorkerGenerationIn(self):
        two_two_two_two_matrix = \
            [
                [
                    [
                        [
                            generate_object(8 * i + 4 * j + 2 * k + l) for l in range(2)
                        ] for k in range(2)
                    ] for j in range(2)
                ] for i in range(2)
            ]
        zero_one_zero_one = select_element_from_matrix(two_two_two_two_matrix, 0, 1, 0, 1)

        import numpy as np
        np.random.seed(4 + 1)

        should = np.random.rand(5)

        self.assertTrue(
            np.allclose(
                compss_wait_on(zero_one_zero_one),
                should
            )
        )


    def testWorkerGenerationInout(self):
        # Generate ten random vectors with pre-determined seed
        ten_random_vectors = [generate_object(i) for i in range(10)]
        increase_elements(ten_random_vectors)
        increase_element(ten_random_vectors[4])
        # Pick the fifth vector from a COLLECTION_IN parameter
        fifth_vector = compss_wait_on(select_element(ten_random_vectors, 4))
        import numpy as np
        np.random.seed(4)
        self.assertEqual(
            np.allclose(
                fifth_vector,
                np.random.rand(5)
            )
        )
