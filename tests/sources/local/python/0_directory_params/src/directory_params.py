#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_dir_params import TestDirParams


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDirParams)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
