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

@task(returns = 1)
def generate_object(seed):
    np.random.seed(seed)
    return np.random.rand(5)

class testCollectionInFunctions(unittest.TestCase):

    def testMasterGeneration(self):
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

    def testWorkerGeneration(self):
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
