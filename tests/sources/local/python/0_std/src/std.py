#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from modules.testSTD import testSTD


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testSTD)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
