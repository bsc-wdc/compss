#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.exceptions import COMPSsException
from pycompss.api.api import compss_delete_file, compss_delete_object, compss_file_exists, compss_open, compss_wait_on, compss_wait_on_file, compss_open, TaskGroup
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



@task(count=INOUT)
def increment(count):
    count.add()


def main():
    if len(sys.argv) != 2:
        exit(-1)

    exception = sys.argv[1]

    class Num:
        def __init__(self, val):
            self.val = val

        def add(self):
            self.val += 1

        def getVal(self):
            return self.val
    fileName = Num(1)

    increment(fileName)
    increment(fileName)

    if exception == "1":
        wait_for_checkpoint(["d1v3_"])
        raise Exception("Error")

    increment(fileName)
    increment(fileName)

    fileName = compss_wait_on(fileName)
    print("Final counter value is " + str(fileName.getVal()))

if __name__ == '__main__':
    main()
