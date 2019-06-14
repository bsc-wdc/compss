#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import time
from pycompss.api.api import compss_barrier_group, TaskGroup
from pycompss.api.parameter import FILE_INOUT, FILE_OUT
from pycompss.api.task import task

NUM_TASKS = 3
NUM_GROUPS = 3
STORAGE_PATH = "/tmp/sharedDisk/"

@task(file_path=FILE_INOUT)
def write_one(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)


@task(file_path=FILE_INOUT)
def write_two(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)

def test_task_groups(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

    with TaskGroup('bigGroup'):
        # Launch NUM_TASKS reading tasks
        for i in range(NUM_GROUPS):
            with(TaskGroup('group'+str(i))):
                for j in range(NUM_TASKS):
                     write_one(file_name)

    for i in range(NUM_GROUPS):
        compss_barrier_group('group'+str(i))

    with TaskGroup('individualGroup'):
        # Launch NUM_TASKS reading tasks
        for i in range(NUM_TASKS):
            write_two(file_name)

    # # Wait for file
    # compss_wait_on_file(file_name);
    #
    # # Synchronize final value
    # import subprocess
    #
    # final_value = subprocess.check_output(["cat", file_name])
    # print("final value = " + str(final_value) + ".   (Expected 1)")

def main():
    file_name1 = STORAGE_PATH + "fileGROUPS.txt"

    print ("[LOG] Test GET FILE")
    test_task_groups(file_name1)


if __name__ == '__main__':
    main()