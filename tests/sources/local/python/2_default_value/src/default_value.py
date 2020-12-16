#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from modules.testObjects import testDefaultObjectValue
from modules.testFiles import testDefaultFileValue


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testDefaultObjectValue)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testDefaultFileValue))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
