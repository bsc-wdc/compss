#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Arguments Warnings
=====================================
"""

# Imports
import unittest

from modules.testSyntaxNumba import testSyntaxNumba
from modules.testSyntaxWConstraintsNumba import testSyntaxWConstraintsNumba
from modules.testSyntaxWImplementsNumba import testSyntaxWImplementsNumba
from modules.testPerformanceNumba import testPerformanceNumba

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testSyntaxNumba)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testSyntaxWConstraintsNumba))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testSyntaxWImplementsNumba))
    # Skipped performance test due to numba dependency on the hardware
    # suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testPerformanceNumba))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
