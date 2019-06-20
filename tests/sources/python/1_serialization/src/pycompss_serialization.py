#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testSerialization import testSerialization

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testSerialization)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
