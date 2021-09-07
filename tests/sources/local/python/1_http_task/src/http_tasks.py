#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_http_task import TestHttpTask


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestHttpTask)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
