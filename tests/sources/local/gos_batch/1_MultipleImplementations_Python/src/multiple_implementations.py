#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from modules.testBinaryDecorator import testBinaryDecorator

from modules.testMpiDecorator import testMpiDecorator

from modules.testContainerDecorator import testContainerDecorator
def testBinary():
    print("---------------------------------------------------------------------")
    print("Beginning Testing Binary")
    suite = unittest.TestLoader().loadTestsFromTestCase(testBinaryDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)
    print("Ending Testing Binary")
    print("---------------------------------------------------------------------")

def testMPI():
    print("---------------------------------------------------------------------")
    print("Beginning Testing MPI")
    suite = unittest.TestLoader().loadTestsFromTestCase(testMpiDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)
    print("Ending Testing MPI")
    print("---------------------------------------------------------------------")


def testContainer():
    print("---------------------------------------------------------------------")
    print("Beginning Testing Container")
    suite = unittest.TestLoader().loadTestsFromTestCase(testContainerDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)
    print("Ending Testing Container")
    print("---------------------------------------------------------------------")


def main():

    #testContainer()

    testBinary()

    testMPI()




if __name__ == "__main__":
    main()
