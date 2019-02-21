#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testCOMPSsDecorator import TestCOMPSsDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCOMPSsDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
