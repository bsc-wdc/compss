#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testMultinodeDecorator import TestMultinodeDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMultinodeDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
