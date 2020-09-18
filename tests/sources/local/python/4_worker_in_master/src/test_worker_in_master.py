#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_basic_types import TestBasicTypes
from modules.test_files import TestFiles
from modules.test_objects import TestObjects
from modules.test_concurrent import TestConcurrency
from modules.test_binary import TestBinary
from modules.test_mpi import TestMPI
from modules.test_ompss import TestOmpSs

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBasicTypes)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestFiles))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestObjects))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestConcurrency))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestBinary))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMPI))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestOmpSs))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
