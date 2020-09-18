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
from pycompss.api.exceptions import COMPSsException

NUM_TASKS = 3
NUM_GROUPS = 3
STORAGE_PATH = "/tmp/sharedDisk/"
TASK_SLEEP_TIME_FAST = 1
TASK_SLEEP_TIME_SLOW = 4

@task(file_path=FILE_IN, time_out=2)
def wait_fast(file_path):
    # Task sleeps less than time out
    time.sleep(TASK_SLEEP_TIME_FAST)


@task(file_path=FILE_IN, time_out=2)
def wait_slow(file_path):
    # Time out is less than sleeping time
    time.sleep(TASK_SLEEP_TIME_SLOW)


def create_file(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()


def test_time_out(file_name):
    wait_fast(file_name)
    wait_slow(file_name)

def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)

    print("[LOG] Test TIME OUT")
    test_time_out(file_name1)


if __name__ == '__main__':
    main()