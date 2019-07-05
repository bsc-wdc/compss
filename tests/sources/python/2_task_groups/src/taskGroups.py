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
from pycompss.api.parameter import FILE_INOUT, FILE_IN
from pycompss.api.task import task

NUM_TASKS = 3
NUM_GROUPS = 3
STORAGE_PATH = "/tmp/sharedDisk/"
TASK_SLEEP_TIME_FAST = 1
TASK_SLEEP_TIME_SLOW = 4

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

@task(file_path=FILE_IN, time_out=2)
def wait_fast(file_path):
    # Task sleeps less than time out
    time.sleep(TASK_SLEEP_TIME_FAST)
    # Write value
    with open(file_path, 'r') as fis:
        contents = int(fis.readline())

@task(file_path=FILE_IN, time_out=2, on_failure='IGNORE')
def wait_slow(file_path):
    # Time out is less than sleeping time
    time.sleep(TASK_SLEEP_TIME_SLOW)
    # Write value
    with open(file_path, 'r') as fis:
        contents = int(fis.readline())

def test_task_groups(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

    print("[LOG] New blank file created")

    with TaskGroup('bigGroup', True):
        print("Inside task group")
        # Inside a big group, more groups are created
        for i in range(NUM_GROUPS):
            with(TaskGroup('group'+str(i), True)):
                for j in range(NUM_TASKS):
                     write_one(file_name)

    # # Barrier for groups
    # for i in range(NUM_GROUPS):
    #     compss_barrier_group('group'+str(i))

    # Creation of group
    with TaskGroup('individualGroup', True):
        for i in range(NUM_TASKS):
            write_two(file_name)


def test_time_out(file_name):
    wait_fast(file_name)
    wait_slow(file_name)


def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"

    print ("[LOG] Test TASK GROUPS")
    test_task_groups(file_name1)

    print("[LOG] Test TIME OUT")
    test_time_out(file_name1)

if __name__ == '__main__':
    main()