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
    increment(fileName)

    fileName = compss_wait_on(fileName)
    print("Middle counter value is " + str(fileName.getVal()))

    if exception == "1":
        time.sleep(15)
        raise Exception("Error")

    increment(fileName)
    increment(fileName)
    increment(fileName)
    increment(fileName)
    increment(fileName)

    fileName = compss_wait_on(fileName)
    print("Final counter value is " + str(fileName.getVal()))
    time.sleep(5)

if __name__ == '__main__':
    main()
