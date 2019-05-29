#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Arguments Warnings
=====================================
"""

# Imports
import unittest

from modules.testArgumentDeprecation import testArgumentDeprecation

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testArgumentDeprecation)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
