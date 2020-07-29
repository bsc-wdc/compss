#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os

from pycompss.api.api import compss_wait_on, compss_open
from pycompss.api.parameter import FILE_INOUT
from pycompss.api.task import task
from pycompss.api.constraint import constraint

STORAGE_PATH = "/tmp/"
@constraint(app_software="App1")
@task(file_path=FILE_INOUT)
def increment1(j, file_path):
    with open(file_path, 'a') as fos:
        new_value = str(j + 1)
        fos.write(new_value)

@constraint(app_software="App2")
@task(file_path=FILE_INOUT)
def increment2(j, file_path):
    with open(file_path, 'a') as fos:
        new_value = str(j + 1)
        fos.write(new_value)

@task(file_path=FILE_INOUT, returns=1)
def sum(file_path):
    with compss_open(file_path, 'r') as fis:
        final_value = fis.read()
    l = final_value.split("")
    return sum([int(i) for i in l if type(i) == int or i.isdigit()])

def createNewFile(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()

def test_per_worker_score():
    file1 = STORAGE_PATH + "file1.txt"
    file2 = STORAGE_PATH + "file2.txt"
    createNewFile(file1)
    print("Created new file 1")
    createNewFile(file2)
    print("Created new file 2")
    range1=8
    for i in range(range1):
        increment1(i, file1)
        increment2(i, file2)
    file1 = compss_wait_on(file1)
    file2 = compss_wait_on(file2)
    for i in range(range1):
        increment1(i, file1)
        increment2(i, file2)
    with compss_open(file1, 'r') as fis:
        final_value1 = fis.read()
    with compss_open(file2, 'r') as fis:
        final_value2 = fis.read()

    #compss_wait_on(result1)
    #compss_wait_on(result2)
    print("Result 1 = " + str(final_value1))
    print("Result 2 = " + str(final_value2))

def main():
    print ("[LOG] Test SCHEDULING")
    test_per_worker_score()


if __name__ == '__main__':
    main()