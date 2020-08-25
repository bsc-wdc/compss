#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Arguments Warnings
=====================================
"""

# Imports
import unittest

from modules.testStandard import testStandard
from modules.testBinary import testBinary
from modules.testOmpSs import testOmpSs
from modules.testMPI import testMPI
from modules.testDecaf import testDecaf



def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testStandard)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testBinary))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testOmpSs))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testMPI))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testDecaf))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
