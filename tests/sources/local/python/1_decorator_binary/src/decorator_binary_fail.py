#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testBinaryFailExit1 import testBinaryFailExit1


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testBinaryFailExit1)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
