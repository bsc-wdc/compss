#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testPrologEpilog import TestPrologEpilog


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPrologEpilog)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
