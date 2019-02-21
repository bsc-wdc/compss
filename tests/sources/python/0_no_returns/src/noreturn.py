#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testNoReturn import testNoReturn
from modules.testNoReturnClasses import testNoReturnClasses


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testNoReturn)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testNoReturnClasses))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
