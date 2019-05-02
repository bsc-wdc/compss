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

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFiles)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestBasicTypes)
    # suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestFiles))
    # suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestObjects))
    # suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testFunction))  # if you want to add another test file
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
    