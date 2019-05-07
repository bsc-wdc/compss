#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_files import TestFiles
from modules.test_objects import TestObjects

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFiles)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestObjects))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
