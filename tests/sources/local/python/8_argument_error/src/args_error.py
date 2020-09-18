#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Arguments Warnings
=====================================
"""

# Imports
import unittest

from modules.testArgumentError import testArgumentError

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testArgumentError)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
