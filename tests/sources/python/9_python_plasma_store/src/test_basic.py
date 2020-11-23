#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest
from modules.kmeans import main as kmeans_exec
from modules.testNumpyDataPlasma import testNumpyDataPlasma
from modules.testStandardDataPlasma import testStandardDataPlasma


def main_test():
    suite = unittest.TestLoader().loadTestsFromTestCase(testNumpyDataPlasma)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testStandardDataPlasma))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    # main_test()
    kmeans_exec(0, 1024, 2, 4, 8, 'uniform', 20, 1e-9, 50, False)
