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


if __name__ == '__main__':
    from pycompss.api.api import compss_barrier_concurrent

    file_name = "/tmp/sharedDisk/file.txt"

    open(file_name, 'w').close()

    for i in range(15):
        # Execute increment
        write_one(file_name)

    compss_barrier_concurrent()


    # Read final value
    with open(file_name, 'r') as fis:
        final_value = fis.read()
    print("Final counter value is " + final_value)

