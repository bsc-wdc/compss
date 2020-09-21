#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.test_resources_api import TestResourcesApi


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestResourcesApi)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
