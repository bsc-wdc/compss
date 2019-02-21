#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Inheritance Testbench
==============================
"""

# Imports
import unittest

from modules.testInheritance import testInheritance


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testInheritance)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
