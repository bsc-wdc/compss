#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.exceptions import COMPSsException
from pycompss.api.api import compss_delete_file, compss_delete_object, compss_file_exists, compss_open, compss_wait_on, compss_wait_on_file, compss_open, TaskGroup, compss_snapshot
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint

import time
import random
import os
from os import path

import glob

CHECKPOINT_DIR = "/tmp/checkpointing/"


def wait_for_checkpoint(patterns, timeout=30, interval=0.2):
    # Wait until the checkpointer has persisted the given data versions,
    # instead of sleeping a fixed amount. No grace period is needed: the
    # runtime shutdown drains pending checkpoint copies, records and
    # superseded-version deletions before exiting. The timeout stays below
    # the execution script's 60s harness timeout so this diagnostic fires
    # first.
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(glob.glob(CHECKPOINT_DIR + p + "*") for p in patterns):
            return
        time.sleep(interval)
    raise Exception("Timed out waiting for checkpoint files: " + str(patterns))



@task(fileName=FILE_INOUT)
def increment(fileName):
    # Read value
    fis = open(fileName, 'r')
    value = fis.read()
    fis.close()

    # Write value
    fos = open(fileName, 'w')
    fos.write(str(int(value) + 1))
    fos.close()

def main():
    # Check and get parameters
    if len(sys.argv) != 2:
        exit(-1)

    exception = sys.argv[1]

    fileName = "fileName"
    fileName2 = "fileName2"
    initialValue = "1"
    # Write value
    fos = open(fileName, 'w')
    fos.write(initialValue)
    fos.close()
    print("Initial counter value is " + str(initialValue))

    fos2 = open(fileName2, 'w')
    fos2.write(initialValue)
    fos2.close()
    print("Initial counter value is " + str(initialValue))
    # Execute increment
    increment(fileName2)
    increment(fileName2)
    increment(fileName)
    increment(fileName)

    val = 0
    fis = compss_open(fileName, 'r')
    middleValue = fis.read()
    fis.close()
    val = val+2
    print("Value " + str(middleValue))

    if exception == "1":
        compss_snapshot()
        wait_for_checkpoint(["d1v3_", "d2v3_"])
        raise Exception("Error")

    increment(fileName)
    increment(fileName)
    increment(fileName)
    increment(fileName2)


    val = val+1
    print("Value " + str(val))

    compss_snapshot()
    wait_for_checkpoint(["d1v4_", "d2v6_"])

    fis = compss_open(fileName, 'r')
    finalValue = fis.read()
    fis.close()
    fis2 = compss_open(fileName2, 'r')
    finalValue2 = fis2.read()
    fis2.close()
    print("Final counter value is " + str(finalValue) + " and: " + str(finalValue2))


    # snapshot

    '''
    increment(fileName)

    if exception == "1":
        raise Exception("Error")
    increment(fileName)

    # Write new value
    # Falta que compss_open al acabar resti un al reader
    fis = compss_open(fileName, 'r')
    finalValue = fis.read()
    fis.close()
    print("Final counter value is " + str(finalValue))
    '''

if __name__ == '__main__':
    main()
