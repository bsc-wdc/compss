#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import sys
import unittest

from modules.testNoReturn import testNoReturn
from modules.testNoReturnClasses import testNoReturnClasses


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testNoReturn)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testNoReturnClasses))
    if sys.version_info >= (3, 5):
        # Include Type-Hinting checks
        from modules.testNoReturnTH import testNoReturnTH
        from modules.testNoReturnClassesTH import testNoReturnClassesTH
        suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testNoReturnTH))
        suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testNoReturnClassesTH))
    elif sys.version_info >= (3, 0):
        raise Exception("ERROR: PYTHON >= 3.5 REQUIRED.")
    else:
        # Python 2 does not support type-hinting, so nothing else to do.
        pass

    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
