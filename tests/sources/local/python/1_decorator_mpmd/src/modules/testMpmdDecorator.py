#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import os

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier, compss_wait_on
from pycompss.api.mpmd_mpi import mpmd_mpi


@mpmd_mpi(runner="mpirun",
          programs=[
               dict(binary=os.getcwd() + "/src/scripts/hello.sh", processes=2),
               dict(binary=os.getcwd() + "/src/scripts/hello.sh", processes=2)
          ])
@task()
def basic():
    pass


@mpmd_mpi(runner="mpirun",
          programs=[
               dict(binary="date", processes=2, params="-d {{first}}"),
               dict(binary="date", processes=2, params="-d {{second}}")
          ])
@task()
def params(first, second):
    pass


@mpmd_mpi(runner="mpirun",
          working_dir=".",
          programs=[
               dict(binary="sed", processes=2, params="{{exp}} {{in_file}}"),
               dict(binary="sed", params="{{exp}} {{in_file}}"),
          ])
@task(in_file=FILE_IN)
def file_in(exp, in_file):
    pass


class TestMpmdDecorator(unittest.TestCase):

    def testBasic(self):
        basic()
        compss_barrier()

    def testParams(self):
        params("next monday", "next friday")
        compss_barrier()

    def testFileManagementIN(self):
        infile = "src/infile"
        file_in("s/Hi/HELLO/g", infile)
        compss_barrier()

