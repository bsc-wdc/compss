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
import sys


NUM_TASKS = 4
TASK_SLEEP_TIME = 2
STORAGE_PATH = "/tmp/sharedDisk/"

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    def __init__(self):
        pass


@task(file_path=FILE_INOUT, onFailure='FAIL')
def write_file(file_path):
    print('Start processing')
    with open(file_path) as f:
        read_value = f.read()
    print('Read value is : ' + read_value)
    write_value = int(read_value) + 1

    print ('Write value is : ' + str(write_value))

    # Write value
    with open(file_path, 'w') as fos:
        new_value = str(write_value)
        fos.write(new_value)

    if int(read_value) < 3:
        print("EXCEPTION")
        #raise TooSmallException('The computed number is too low')
        raise ServiceExit()
  #  time.sleep(TASK_SLEEP_TIME)


def test_on_failure_retry(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

    # Write value
    with open(file_name, 'w') as fos:
        new_value = str(1)
        fos.write(new_value)

    # Launch NUM_TASKS CONCURRENT
    for i in range(NUM_TASKS):
        try:
            write_file(file_name)
        except Exception as e:
            print(e.value)
            print('Exiting main program')
            sys.exit(0)

    # Synchronize final value
    with open(file_name) as f:
        final_value = f.read()
    print("File has been written " + str(final_value) + " times ")

def main():
    file_name1 = STORAGE_PATH + "file1.txt"

    print ("[LOG] Test RETRY")
    test_on_failure_retry(file_name1)


if __name__ == '__main__':
    main()
