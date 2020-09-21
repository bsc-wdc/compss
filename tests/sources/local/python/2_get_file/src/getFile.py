#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os

from pycompss.api.api import compss_wait_on_file
from pycompss.api.parameter import FILE_INOUT, FILE_IN
from pycompss.api.task import task

NUM_TASKS = 5
STORAGE_PATH = "/tmp/sharedDisk/"


@task(file_path=FILE_INOUT)
def write_in_file(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
        print("write inout")

@task(file_path=FILE_IN)
def read_from_file(file_path):
    # Write value
    with open(file_path) as fos:
        n = fos.read()
        print("read in")


def test_get_file(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

    # Launch NUM_TASKS reading tasks
    for i in range(NUM_TASKS*2):
        read_from_file(file_name)

    # Launch writing tasks
    for i in range(1):
        write_in_file(file_name)

    # Wait for file
    compss_wait_on_file(file_name);

    # Synchronize final value
    import subprocess

    final_value = subprocess.check_output(["cat", file_name])
    print("final value = " + str(final_value) + ".   (Expected 1)")

def main():
    file_name1 = STORAGE_PATH + "file.txt"

    print ("[LOG] Test GET FILE")
    test_get_file(file_name1)


if __name__ == '__main__':
    main()