#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_dynamic_computing_nodes import TestDynamicComputingNodes


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDynamicComputingNodes)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
