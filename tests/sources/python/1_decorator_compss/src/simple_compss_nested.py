#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.parameter import FILE_INOUT
from pycompss.api.task import task
from pycompss.api.api import compss_open


@task(file_path=FILE_INOUT)
def increment(file_path):
    print("Init task user code")
    # Read value
    fis = open(file_path, 'r')
    value = fis.read()
    print("Received " + value)
    fis.close()

    # Write value
    fos = open(file_path, 'w')
    new_value = str(int(value) + 1)
    print("Computed " + new_value)
    fos.write(new_value)
    fos.close()


def usage():
    print("[ERROR] Bad number of parameters")
    print("    Usage: simple <counterValue>")


def test_simple(initial_value):
    file_name = "counter"

    # Write value
    with open(file_name, 'w') as fos:
        fos.write(initial_value)
    print("Initial counter value is " + initial_value)

    # Execute increment
    increment(file_name)

    # Write new value
    with compss_open(file_name, 'r+') as fis:
        final_value = fis.read()
        print("Final counter value is " + final_value)


if __name__ == '__main__':
    import sys

    counter = sys.argv[1]
    test_simple(counter)
