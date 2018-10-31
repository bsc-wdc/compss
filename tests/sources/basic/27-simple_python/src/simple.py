#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.parameter import *
from pycompss.api.task import task
import unittest
import sys


@task(filePath=FILE_INOUT)
def increment(filePath):
    print("Init task user code")
    # Read value
    fis = open(filePath, 'r')
    value = fis.read()
    print("Received " + value)
    fis.close()

    # Write value
    fos = open(filePath, 'w')
    newValue = str(int(value) + 1)
    print("Computed " + newValue)
    fos.write(newValue)
    fos.close()


def usage():
    print("[ERROR] Bad number of parameters")
    print("    Usage: simple <counterValue>")


class KmeansTest(unittest.TestCase):
    def test_simple(self):
        from pycompss.api.api import compss_open


        initial_value = '1'
        fileName = "counter"

        # Write value
        fos = open(fileName, 'w')
        fos.write(initial_value)
        fos.close()
        print("Initial counter value is " + initial_value)

        # Execute increment
        increment(fileName)

        # Write new value
        fis = compss_open(fileName, 'r+')
        final_value = fis.read()
        fis.close()
        print("Final counter value is " + final_value)

        expected_final_value = '2'
        if final_value != expected_final_value:
            print("Simple increment test result is incorrect. "
                  "Expected: %s, got: %s" % (expected_final_value, final_value))
        self.assertEqual(final_value, expected_final_value)


if __name__ == '__main__':
    unittest.main()
