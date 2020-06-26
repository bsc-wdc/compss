#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Arguments Warnings
=====================================
"""

# Imports
import unittest

from modules.testGenerator import testGenerator
from modules.testTimeIt import testTimeIt
from modules.testMapReduce import testMapReduce

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testGenerator)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testTimeIt))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testMapReduce))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
