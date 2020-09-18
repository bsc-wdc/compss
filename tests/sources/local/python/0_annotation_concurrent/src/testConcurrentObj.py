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
from pycompss.api.parameter import FILE_INOUT, FILE_CONCURRENT
from pycompss.api.task import task
from pycompss.api.api import compss_open

from models import MyFile

NUM_TASKS = 4
TASK_SLEEP_TIME = 2
STORAGE_PATH = "/tmp/sharedDisk/"


@task(file_path=FILE_CONCURRENT)
def write_one(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
    time.sleep(TASK_SLEEP_TIME)


@task(file_path=FILE_INOUT)
def write_two(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)
    time.sleep(TASK_SLEEP_TIME)


def test_direction_concurrent(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

    # Launch NUM_TASKS CONCURRENT
    for i in range(NUM_TASKS):
        write_one(file_name)

    # Launch NUM_TASKS INOUT
    for i in range(NUM_TASKS):
        write_two(file_name)

    # Synchronize final value
    with compss_open(file_name, 'r') as fis:
        final_value = fis.read()
    total = final_value.count('1')
    total2 = final_value.count('2')
    print("Final counter value is " + str(total) + " ones and " + str(total2) + "twos")


def test_psco_concurrent(file_name):
    # Initialize file
    open(file_name, 'w').close()

    # Initialize PSCO object
    file_psco = MyFile(file_name)
    file_psco.makePersistent()

    # Launch NUM_TASKS CONCURRENT
    for i in range(NUM_TASKS):
        # Execute increment
        file_psco.write_three()

    # Synchronize
    file_psco = compss_wait_on(file_psco)
    total = file_psco.count_threes()
    print("Final counter value is " + str(total) + " three")
    file_psco.deletePersistent()

    # Re-create the file
    file2 = MyFile(file_name)
    open(file_name, 'a').close()

    # Launch NUM_TASKS INOUT
    for i in range(NUM_TASKS):
        # Execute increment
        file2.write_four()

    # Synchronize
    file2 = compss_wait_on(file2)
    total = file2.count_fours()
    print("Final counter value is " + str(total) + " fours")


def main():
    file_name1 = STORAGE_PATH + "file1.txt"
    file_name2 = STORAGE_PATH + "file2.txt"

    print ("[LOG] Test DIRECTION CONCURRENT")
    test_direction_concurrent(file_name1)

    print ("[LOG] Test PSCO CONCURRENT")
    test_psco_concurrent(file_name2)


if __name__ == '__main__':
    main()
