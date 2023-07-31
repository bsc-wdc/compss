#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from modules.testDynamicConstraintsFunctions import testDynamicConstraintsFunctions
from modules.testDynamicConstraintsMethods import testDynamicCostraintsMethods


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testDynamicConstraintsFunctions)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testDynamicCostraintsMethods))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
