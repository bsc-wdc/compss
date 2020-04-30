#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_cancel_binary_tasks import TestCancelBinaryTasks


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCancelBinaryTasks)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
