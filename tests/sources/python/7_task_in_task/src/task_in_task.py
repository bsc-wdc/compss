#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Arguments Warnings
=====================================
"""

# Imports
import unittest

from modules.testTaskInTask import testTaskInTask

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testTaskInTask)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
