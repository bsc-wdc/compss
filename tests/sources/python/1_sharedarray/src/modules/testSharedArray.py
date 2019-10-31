#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import tempfile
import numpy as np

from os.path import basename

from pycompss.util.sharedmemory.shma import SHAREDARRAY_AVAILABLE,  \
                                            serialize_to_shm,       \
                                            deserialize_from_shm,   \
                                            delete_shma,            \
                                            clear_shma

class testSharedArray(unittest.TestCase):

    SHMA_TEST_ARRAY_SHAPE = (50, 50)

    @unittest.skipIf(False == SHAREDARRAY_AVAILABLE, 'No SHM capabilities available.')
    def testSharedArray(self):
        """
        Tests de-/serialization of np.ndarrays of double using shared memory.
        """

        serialize_to_shm(testSharedArray.arr, testSharedArray.tmp_file)

        arr_test1 = deserialize_from_shm(testSharedArray.tmp_file)
        self.assertTrue(testSharedArray.SHMA_TEST_ARRAY_SHAPE == arr_test1.shape)
        self.assertTrue(np.array_equal(testSharedArray.arr, arr_test1))
        self.assertEqual(type(testSharedArray.arr), type(arr_test1),
                         msg='Bad returned array type.')

        arr_test2 = deserialize_from_shm(testSharedArray.tmp_file)
        arr_test2 += 1
        self.assertTrue(np.array_equal(arr_test1, arr_test2))

        delete_shma(testSharedArray.tmp_file.name)
        clear_shma(False)


    @classmethod
    def setUpClass(cls):
        """
        Generate an 50x50 numpy array of 0 (ints) and set the temporary file
        for array serialization.
        """
        testSharedArray.arr = np.zeros(testSharedArray.SHMA_TEST_ARRAY_SHAPE)
        testSharedArray.tmp_file = tempfile.NamedTemporaryFile()


    @classmethod
    def tearDownClass(cls):
        """ Free the space used for the shared array. """
        testSharedArray.tmp_file.close()
