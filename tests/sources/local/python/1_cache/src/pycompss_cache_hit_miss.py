#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testHitMiss import testHitMiss


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testHitMiss)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
