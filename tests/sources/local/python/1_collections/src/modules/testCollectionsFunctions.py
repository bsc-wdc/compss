#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import numpy as np

from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *
from pycompss.api.task import task

from my_class import MyClass


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


@task(kol_out={Type: COLLECTION_OUT, Depth: 1})
def append(kol_out, value):
    # Requires depth because appends into the object
    for i in range(len(kol_out)):
        kol_out[i].append(value)
    return True


@task(kol_out=COLLECTION_OUT)
def modify_obj(kol_out, value):
    for i in range(len(kol_out)):
        kol_out[i].set_test(value)
    return True


@task(kol_out={Type: COLLECTION_OUT, Depth: 2})
def modify_deep_obj(kol_out, to_append, to_modify):
    # Requires depth because appends into the object
    for i in range(len(kol_out)):
        kol_out[i][0].append(to_append)
        kol_out[i][1].set_test(to_modify)
    return True


@task(kol_out=COLLECTION_OUT)
def modify_deep_obj_no_depth(kol_out, to_modify):
    for i in range(len(kol_out)):
        kol_out[i][0].set_test(to_modify)
    return True


class testCollectionFunctions(unittest.TestCase):

    def testCollectionOut(self):

        array_1 = [[MyClass()] for _ in range(5)]
        array_2 = [MyClass() for _ in range(5)]
        array_3 = [[[i, i * 2], MyClass()] for i in range(5)]
        array_4 = [[MyClass()] for _ in range(5)]

        for i in range(5):
            append(array_1, "appended")
            modify_obj(array_2, "modified")
            modify_deep_obj(array_3, "appended", "modified")
            modify_deep_obj_no_depth(array_4, "submodified")

        array_1 = compss_wait_on(array_1)
        array_2 = compss_wait_on(array_2)
        array_3 = compss_wait_on(array_3)
        array_4 = compss_wait_on(array_4)

        appends = True
        modifies = True
        deep_modifies = True
        deep_modifies_no_depth = True

        for i in range(5):

            if appends:
                appends = "appended" in array_1[i]

            if modifies:
                modifies = array_2[i].get_test() == "modified"

            if deep_modifies:
                _0 = "appended" in array_3[i][0]
                _1 = array_3[i][1].get_test() == "modified"
                deep_modifies = _0 and _1

            if deep_modifies_no_depth:
                deep_modifies_no_depth = array_4[i][0].get_test() == "submodified"

        self.assertTrue(appends)
        self.assertTrue(modifies)
        self.assertTrue(deep_modifies)
        self.assertTrue(deep_modifies_no_depth)

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
        self.assertTrue(
            np.allclose(
                fifth_vector,
                np.random.rand(5) + 2.0
            )
        )
