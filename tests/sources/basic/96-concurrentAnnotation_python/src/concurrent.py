#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.parameter import *
from pycompss.api.task import task
from pycompss.api.api import compss_open
import unittest
import time


@task(file_path=FILE_CONCURRENT)
def write_one(file_path):
    print("Init task user code")
    # Read value
    with open(file_path, 'r') as fis:
        value = fis.read()

    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
    print("current counter value is " + str(new_value))

    time.sleep(2)

@task(file_path=FILE_IN)
def write_two(file_path):
    print("Init task user code")
    # Read value
    with open(file_path, 'r') as fis:
        value = fis.read()

    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)
    print("current counter value is " + str(new_value))

    time.sleep(2)

if __name__ == '__main__':
    from pycompss.api.api import compss_barrier_concurrent

    file_name = "/tmp/sharedDisk/file.txt"

    open(file_name, 'w').close()

    for i in range(15):
        # Execute increment
        write_one(file_name)

    write_two(file_name)
 #   compss_barrier_concurrent()


    # Read final value
    with compss_open(file_name, 'r') as fis:
        final_value = fis.read()
    total = final_value.count('1')
    total2 = final_value.count('2')
    print("Final counter value is " + str(total) + " ones and " + str(total2) + "twos")

