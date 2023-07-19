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


@mpmd_mpi(runner="mpirun", processes_per_node=2,
          programs=[
               dict(binary="date", processes=2),
               dict(binary="date", processes=6)
          ])
@task()
def basic_2():
    pass


@mpmd_mpi(runner="mpirun",
          programs=[
               dict(binary="date", processes=2, args="-d {{first}}"),
               dict(binary="date", processes=2, args="-d {{second}}")
          ])
@task()
def test_args(first, second):
    pass


@mpmd_mpi(runner="mpirun",
          programs=[
            dict(binary="date", args="{{pref}} {{param_1}}"),
            dict(binary="date", args="{{pref}} {{param_2}}"),
          ])
@task()
def myDateDef(pref="-d", param_1="last wednesday", param_2="next wednesday"):
    pass


@mpmd_mpi(runner="mpirun",
          working_dir=".",
          programs=[
               dict(binary="sed", processes=2, args="{{exp}} {{in_file}}"),
               dict(binary="sed", args="{{exp}} {{in_file}}"),
          ])
@task(in_file=FILE_IN)
def file_in(exp, in_file):
    pass


@mpmd_mpi(runner="mpirun",
          working_dir=".",
          programs=[
               dict(binary="grep", args="{{keyword}} {{in_file}}"),
               dict(binary="grep", args="{{keyword}} {{in_file}}"),
          ])
@task(in_file=FILE_IN, result={Type: FILE_OUT_STDOUT})
def std_out(keyword, in_file, result):
    pass


@mpmd_mpi(runner="mpirun", fail_by_exit_value=False,
          programs=[
               dict(binary=os.getcwd() + "/src/scripts/exit_with_code.sh",
                    args="{{exit_code}}"),
               dict(binary=os.getcwd() + "/src/scripts/exit_with_code.sh",
                    args="{{exit_code}}")
          ])
@task(returns=int)
def exit_with_code(exit_code):
    pass


@mpmd_mpi(runner="mpirun",
          working_dir=os.getcwd() + '{{wd}}',
          programs=[
               dict(binary="pwd"),
               dict(binary="pwd"),
          ])
@task(result={Type: FILE_OUT_STDOUT})
def param_in_wd(wd, result):
    pass


class TestMpmdDecorator(unittest.TestCase):

    def testBasic(self):
        basic()
        compss_barrier()

    def testBasic2(self):
        basic_2()
        compss_barrier()

    def testParams(self):
        test_args("next monday", "next friday")
        compss_barrier()

    def testDefaultValue(self):
        myDateDef()
        compss_barrier()

    def testFileInParam(self):
        infile = "src/infile"
        file_in("s/Hi/HELLO/g", infile)
        compss_barrier()

    def testStdOutFile(self):
        infile = "src/infile"
        outfile = "src/outfile"
        std_out("Hi", infile, outfile)
        compss_barrier()

    def testFailedBinaryExitValue(self):
        ev = exit_with_code(19)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 19)  # own exit code for failed execution

    def testParamInWD(self):
        wd = '/test_param_in_wd'
        outfile = "param_wd_out"
        param_in_wd(wd, outfile)
        compss_barrier()

