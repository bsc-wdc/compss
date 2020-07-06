#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.binary import binary
from pycompss.api.mpi import mpi
from pycompss.api.api import TaskGroup
from pycompss.api.exceptions import COMPSsException


# Tasks definition
@task()
def exception_task():
    import time
    time.sleep(5)  # s
    raise COMPSsException("Intended exception")


@binary(binary="${PATH_BINARY}/simple.sh")
@task(returns=1, on_failure="IGNORE")
def cancel_binary():
    pass


@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh")
@task(returns=1, on_failure="IGNORE")
def cancel_mpi():
    pass


# Test
class TestCancelBinaryTasks(unittest.TestCase):

    def test_binary(self):
        group_name = "binary"
        try:
            with TaskGroup(group_name):
                cancel_binary()
                exception_task()
        except COMPSsException as e:
            print("Captured compss exception" + str(e))

    def test_mpi(self):
        group_name = "mpi"
        try:
            with TaskGroup(group_name):
                cancel_mpi()
                exception_task()
        except COMPSsException as e:
            print("Captured compss exception" + str(e))
