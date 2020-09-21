#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testFunctions import testFunctions


# from modules.testLaunch import testLaunch


def main():
    suiteAdvance = unittest.TestLoader().loadTestsFromTestCase(testFunctions)
    # suiteAdvance.addTest(unittest.TestLoader().loadTestsFromTestCase(testLaunch))
    unittest.TextTestRunner(verbosity=2).run(suiteAdvance)


if __name__ == "__main__":
    main()
