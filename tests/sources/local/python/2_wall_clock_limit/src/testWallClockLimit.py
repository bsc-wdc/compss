#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import time
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import FILE_INOUT, FILE_IN, FILE_OUT
from pycompss.api.task import task
from pycompss.api.exceptions import COMPSsException

NUM_TASKS = 6
STORAGE_PATH = "/tmp/sharedDisk/"


@task(file_path=FILE_IN, returns=1)
def long_task(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)
    time.sleep(20)
    return 1


def create_file(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()


def test_cancellation(file_name):
    res = []
    for i in range (50):
        res.append(long_task(file_name))
    compss_wait_on(res)


def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)

    print("[LOG] Test CANCEL APPLICATION TASKS")
    test_cancellation(file_name1)

if __name__ == '__main__':
    main()
