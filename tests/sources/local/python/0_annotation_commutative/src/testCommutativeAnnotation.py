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
from pycompss.api.parameter import FILE_OUT, FILE_COMMUTATIVE, FILE_IN, FILE_INOUT
from pycompss.api.task import task
from pycompss.api.api import compss_open

from models import PersistentObject

NUM_TASKS = 7
TASK_SLEEP_TIME = 1
OTHER_TASK_SLEEP_TIME = 0.5
STORAGE_PATH = "/tmp/sharedDisk/"


@task(file_path=FILE_OUT)
def write_one(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
    time.sleep(TASK_SLEEP_TIME)


@task(file_path=FILE_OUT)
def write_two_slow(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)
    time.sleep(TASK_SLEEP_TIME*2)

@task(file_path1=FILE_IN, file_path2=FILE_IN, file_path3=FILE_COMMUTATIVE)
def write_commutative(file_path1, file_path2, file_path3):

    with open(file_path1, 'r') as fis:
        contents1 = int(fis.read())
    with open(file_path2, 'r') as fis:
         contents2 = int(fis.read())

     # Write value
    with open(file_path3, 'a') as fos:
         fos.write(str(contents1+contents2)+"\n")
    time.sleep(TASK_SLEEP_TIME)

    print("Writen value is " + str(contents1+contents2)+"\n")

@task(file_path=FILE_INOUT)
def check_results(file_path):
    with open(file_path, 'r') as fis:
        contents1 = int(fis.readline())
        contents2 = int(fis.readline())
        print("Contents1 value is " + str(contents1))
        print("Contents2 value is " + str(contents2))

        # Write value
    with open(file_path, 'w') as fos:
        fos.write(str(contents1 + contents2))
        print("Writen value is " + str(contents1 + contents2) )
    time.sleep(OTHER_TASK_SLEEP_TIME)

@task(file_path=FILE_IN)
def check_results2(file_path):
    with open(file_path, 'r') as fis:
        contents1 = int(fis.readline())
        print("Contents1 value is " + str(contents1))
    time.sleep(OTHER_TASK_SLEEP_TIME)
    return contents1

@task(file_path=FILE_COMMUTATIVE)
def addOne_commutative(file_path):
    with open(file_path, 'r') as fis:
        contents1 = int(fis.readline())

        # Write value
    with open(file_path, 'w') as fos:
        fos.write(str(contents1 + 1))
        print("Writen value is " + str(contents1+1) )
    time.sleep(OTHER_TASK_SLEEP_TIME)

@task(file_path1=FILE_IN, file_path2=FILE_COMMUTATIVE)
def accumulate_commutative(file_path1, file_path2):
    with open(file_path1, 'r') as fis:
        contents1 = int(fis.readline())
    with open(file_path2, 'r') as fis:
        contents2 = int(fis.readline())

    # Write value
    with open(file_path2, 'w') as fos:
        fos.write(str(contents1 + contents2))
        print("Writen value is " + str(contents1 + contents2))
    time.sleep(OTHER_TASK_SLEEP_TIME)



def test_direction_commutative(file_commons):

    files = ['']*NUM_TASKS;
    # Clean previous ocurrences of the files
    print("[LOG] Initializing files")
    for i in range(1,NUM_TASKS):
        files[i] = create_new_file(file_commons + str(i) + ".txt")
        print(str(files[i]))

    print ("[LOG] Write one")
    write_one(files[1])

    print ("[LOG] Write two slow")
    write_two_slow(files[2])
    print ("[LOG] Write two slow")
    write_two_slow(files[3])

    print ("[LOG] Write one")
    write_one(files[4])
    print ("[LOG] Write one")
    write_one(files[5])
    time.sleep(0.1)
    print ("[LOG] Write commutative")
    write_commutative(files[2], files[3], files[6])
    time.sleep(0.1)
    print ("[LOG] Write commutative")
    write_commutative(files[4], files[5], files[6])

    print ("[LOG] Check results")
    check_results(files[6])
    time.sleep(0.1)
    print("[LOG] AddOne commutative")
    addOne_commutative(files[6])
    time.sleep(0.1)
    print("[LOG] AddOne commutative")
    addOne_commutative(files[6])
    time.sleep(0.1)
    print("[LOG] AddOne commutative")
    addOne_commutative(files[6])
    time.sleep(0.1)
    print("[LOG] Accumulate commutative")
    accumulate_commutative(files[6], files[1])
    time.sleep(0.1)
    print("[LOG] Accumulate commutative")
    accumulate_commutative(files[6], files[1])
    time.sleep(0.1)
    print("[LOG] Accumulate commutative")
    accumulate_commutative(files[6], files[1])
    time.sleep(0.1)
    print("[LOG] AddOne commutative")
    addOne_commutative(files[1])
    time.sleep(0.1)
    print("[LOG] AddOne commutative")
    addOne_commutative(files[1])
    time.sleep(0.1)
    print("[LOG] AddOne commutative")
    addOne_commutative(files[1])

    print ("[LOG] Check results")
    results = check_results2(files[1])

    print(results)
    # Synchronize final value
    with compss_open(files[6], 'r') as fis:
        final_value = fis.read()

    print("Final counter value of text file 1 is " + final_value)
    # Synchronize final value
    with compss_open(files[1], 'r') as fis:
        final_value = fis.read()

    print("Final counter value of text file 2 is " + final_value)

def test_psco_commutative():
    # Initialize PSCO object
    file_psco = PersistentObject()

    file_psco.makePersistent()

    # Launch NUM_TASKS CONCURRENT
    for i in range(NUM_TASKS):
        # Execute increment
        time.sleep(0.1)
        file_psco.write_three()

    # Synchronize
    file_psco = compss_wait_on(file_psco)
    total_three = file_psco.get_count()
    print("Final counter value is " + str(total_three))

    # Launch NUM_TASKS INOUT
    for i in range(NUM_TASKS):
        # Execute increment
        file_psco.write_four()

    # Synchronize
    file_psco = compss_wait_on(file_psco)
    total = file_psco.get_count()
    print("Final counter value is " + str(total))
    file_psco.deletePersistent()

def create_new_file(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()
    return file_name

def main():
    file_commons = STORAGE_PATH + "fileCommutativeTest"

    print ("[LOG] Test DIRECTION COMMUTATIVE")
    test_direction_commutative(file_commons)

    print ("[LOG] Test PSCO CONCURRENT")
    test_psco_commutative()

if __name__ == '__main__':
    main()
