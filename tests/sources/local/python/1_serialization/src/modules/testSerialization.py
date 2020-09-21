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

        serialize_to_file(arr, tmp_file.name)
        deserialize_from_file(tmp_file.name)
