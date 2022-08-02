#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testEviction import testEviction


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testEviction)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
