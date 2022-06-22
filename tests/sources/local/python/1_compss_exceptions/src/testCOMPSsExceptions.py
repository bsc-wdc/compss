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

@task(file_path=FILE_INOUT)
def write_three(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(3)
        fos.write(new_value)
    raise COMPSsException("Exception has been raised!!")

@task(returns=1)
def task_with_return(in_data):
    if in_data > 1:
        raise COMPSsException("Exception has been raised!!")
    else:
        return in_data + 1

def create_file(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

def test_exception_with_return():
    group_name = 'exceptionGroup0'
    try:
        with TaskGroup(group_name):
            i=1
            i = task_with_return(i)
            i = task_with_return(i)
            i = task_with_return(i)
    except COMPSsException:
        print("COMPSsException caught")

def test_exceptions(file_name):
    try:
        # Creation of group
        with TaskGroup('exceptionGroup1'):
            for i in range(NUM_TASKS):
                write_three(file_name)
    except COMPSsException:
        print("COMPSsException caught")
        write_two(file_name)
    write_one(file_name)


def test_exceptions_barrier(file_name):
    group_name = 'exceptionGroup2'
    # Creation of group
    with TaskGroup(group_name, False):
        for i in range(NUM_TASKS):
            write_three(file_name)
    try:
        # The barrier is not implicit and the exception is thrown
        compss_barrier_group(group_name)
    except COMPSsException:
        print("COMPSsException caught")
        write_two(file_name)
    write_one(file_name)


def test_exceptions_barrier_error(file_name):
    group_name = 'exceptionGroup3'
    # Creation of group
    with TaskGroup(group_name, False):
        for i in range(NUM_TASKS):
            write_three(file_name)

    # The barrier is not implicit and the exception is thrown
    compss_barrier_group(group_name)

def main():

    print("[LOG] Test EXCEPTIONS with RETURNS")
    test_exception_with_return()

    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)

    print("[LOG] Test EXCEPTIONS")
    test_exceptions(file_name1)

    print("[LOG] Test EXCEPTIONS and BARRIERS")
    test_exceptions_barrier(file_name1)

    print("[LOG] Test EXCEPTIONS and BARRIERS")
    test_exceptions_barrier_error(file_name1)

if __name__ == '__main__':
    main()
