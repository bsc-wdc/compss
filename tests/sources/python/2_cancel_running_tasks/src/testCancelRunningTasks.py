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
from pycompss.api.parameter import FILE_INOUT, FILE_IN, FILE_OUT
from pycompss.api.task import task
from pycompss.api.exceptions import COMPSsException

NUM_TASKS = 6
STORAGE_PATH = "/tmp/sharedDisk/"


@task(file_path=FILE_INOUT)
def throw_exception(file_path):
    time.sleep(10)
    raise COMPSsException("Exception raised from the third task")


@task(file_name=FILE_IN)
def long_task(file_name):
    time.sleep(30)
    print("Task not cancelled")


@task(file_name=FILE_IN)
def executed_task(file_name):
    time.sleep(1)


@task(file_path=FILE_IN)
def cancelled_task(file_path):
    time.sleep(4)
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
    # Read contents
    with open(file_path, 'r') as fis:
        contents = fis.read()
        print(contents)


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


def test_cancellation(file_name):
    try:
        # Creation of group
        with TaskGroup('failedGroup'):
            long_task(file_name)
            long_task(file_name)
            executed_task(file_name)
            throw_exception(file_name)
            cancelled_task(file_name)
            cancelled_task(file_name)
    except COMPSsException:
        print("COMPSsException caught")
        write_two(file_name)
    write_two(file_name)


def test_cancellation_no_implicit_barrier(file_name):
    # Creation of group
    with TaskGroup('failedGroup2', False):
        long_task(file_name)
        long_task(file_name)
        executed_task(file_name)
        throw_exception(file_name)
        cancelled_task(file_name)
        cancelled_task(file_name)
    try:
        # The barrier is not implicit and the exception is thrown
        compss_barrier_group('failedGroup2')
    except COMPSsException:
        print("COMPSsException caught")
        write_two(file_name)
    write_two(file_name)


def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)

    print("[LOG] Test CANCEL RUNNING TASKS")
    test_cancellation(file_name1)

    print("[LOG] Test CANCEL RUNNING TASKS WITHOUT IMPLICIT BARRIER")
    test_cancellation_no_implicit_barrier(file_name1)


if __name__ == '__main__':
    main()
