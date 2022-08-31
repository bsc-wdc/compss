#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import time
from pycompss.api.api import compss_barrier_group, compss_cancel_group, TaskGroup
from pycompss.api.parameter import FILE_INOUT, FILE_IN
from pycompss.api.task import task
from pycompss.api.exceptions import COMPSsException

NUM_TASKS = 3
NUM_GROUPS = 3
STORAGE_PATH = "/tmp/sharedDisk/"

@task(file_path=FILE_INOUT)
def write_one(file_path):
    # Write value
    time.sleep(10)
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)


@task(file_path=FILE_INOUT)
def write_two(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)


def create_file(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()


def test_task_groups(file_name):

    with TaskGroup('bigGroup', False):
        # Inside a big group, more groups are created
        for i in range(NUM_GROUPS):
            with(TaskGroup('group'+str(i), False)):
                for j in range(NUM_TASKS):
                     write_one(file_name)
    time.sleep(15)
    # Barrier for groups
    for i in range(NUM_GROUPS):
         compss_cancel_group('group'+str(i))

    # Creation of group
    with TaskGroup('individualGroup', True):
        for i in range(NUM_TASKS):
            write_two(file_name)

def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)
    time.sleep(15)
    print ("[LOG] Test TASK GROUPS implicit barrier")
    test_task_groups(file_name1)

if __name__ == '__main__':
    main()
