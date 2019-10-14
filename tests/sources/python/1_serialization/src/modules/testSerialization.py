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

from pycompss.util.serialization.serializer import deserialize_from_file, serialize_to_file

class testSerialization(unittest.TestCase):

    def testObjectArray(self):
        """ Tests de-/serialization of object np.arrays"""

        arr = np.array(np.random.random((50, 50)), dtype=object)

        tmp_file = tempfile.NamedTemporaryFile()

        serialize_to_file(((50,50), arr), tmp_file.name) # Serialise tuple instead of array
        dim, arr_test = deserialize_from_file(tmp_file.name)

        self.assertTrue((50, 50) == dim)
        self.assertTrue(np.array_equal(arr, arr_test))
