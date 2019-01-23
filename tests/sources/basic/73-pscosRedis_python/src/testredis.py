#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testRedis import TestRedis
from modules.testRedisApp import TestRedisApp


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRedis)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestRedisApp))
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
