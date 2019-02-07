#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from models import MyFile
from pycompss.api.api import compss_wait_on
import os
from pycompss.api.parameter import *
from pycompss.api.task import task
from pycompss.api.api import compss_open
import socket
import time

@task(file_path=FILE_CONCURRENT)
def write_one(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
    time.sleep(2)

@task(file_path=FILE_INOUT)
def write_two(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)
    time.sleep(2)

storage_path = '/tmp/sharedDisk/'

def testDirectionConcurrent(file_name):

    if not os.path.exists(storage_path):
        os.mkdir(storage_path)
    if os.path.exists(file_name):
        os.remove(file_name)
    open(file_name, 'w').close()

    for i in range(4):
        # Execute increment
        write_one(file_name)

    for i in range(3):
        write_two(file_name)

    # Read final value
    with compss_open(file_name, 'r') as fis:
        final_value = fis.read()
    total = final_value.count('1')
    total2 = final_value.count('2')
    print("Final counter value is " + str(total) + " ones and " + str(total2) + "twos")

def testPSCOConcurrent(file_name):

    open(file_name, 'w').close()

    file = MyFile(file_name)

    file.makePersistent()

    for i in range(4):
        # Execute increment
        file.writeThree()

    file = compss_wait_on(file)

    total = file.countThrees()
    print("Final counter value is " + str(total) + " three")
    file.deletePersistent()

    file2 = MyFile(file_name)
    open(file_name, 'a').close()

    for i in range(4):
        # Execute increment
        file2.writeFour()

    file2 = compss_wait_on(file2)

    total = file2.countFours()
    print("Final counter value is " + str(total) + " fours")

def main():
    file_name = "/tmp/sharedDisk/file.txt"
    print ("[LOG] Test DIRECTION CONCURRENT")
    testDirectionConcurrent(file_name)
    print ("[LOG] Test PSCO CONCURRENT")
    testPSCOConcurrent(file_name)

if __name__ == '__main__':
    main()
