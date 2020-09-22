#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *


class obj(object):
    def __init__(self, value, tag):
        self.val = value
        self.tag = tag

    def __str__(self):
        return str(self.val) + " " + self.tag



@task(value=INOUT)
def increment(value):
    print("hello increment " + str(value))
    value.val = value.val + 1


@task()
def main():
    print("hello main")
    val = obj(0, "name")
    increment(val)
    val = compss_wait_on(val)
    print("Updated object "+str(val))

if __name__ == "__main__":
    main()