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
from pycompss.api.api import compss_barrier as cb, compss_wait_on as cwo
from pycompss.api.binary import binary
from pycompss.api.mpi import mpi
from pycompss.api.mpmd_mpi import mpmd_mpi
from pycompss.api.prolog import prolog
from pycompss.api.parameter import *
from pycompss.api.epilog import epilog


@prolog(binary="echo", args="just a prolog")
@epilog(binary="echo", args="just an epilog")
@task()
def basic():
    return True


@prolog(binary="date", args="-d {{a}}")
@epilog(binary="date", args="-d {{b}}")
@mpmd_mpi(runner="mpirun",
          programs=[
               dict(binary="echo", processes=2, args="program 1"),
               dict(binary="echo", processes=2, args="program 2")
          ])
@task()
def args_mpmd(a, b):
    pass


@prolog(binary="date", args="-date", fail_by_exit_value=False)
@epilog(binary="date", args="-date", fail_by_exit_value=False)
@task()
def skip_failure():
    return True


@prolog(binary="date", args="-wrong", fail_by_exit_value=False)
@mpi(runner="mpirun", binary="echo", args="prolog failed successfully")
@task(returns=1)
def mpi_skip_failure():
    pass


@prolog(binary="date", args="-wrong", fail_by_exit_value=False)
@binary(binary="echo", args="prolog failed successfully",
        fail_by_exit_value=False)
@task(returns=1)
def mpi_skip_failure():
    pass


@prolog(binary="cat", args="{{p_file}}")
@epilog(binary="cat", args="{{e_file}}")
@task(p_file=FILE_IN, e_file=FILE_IN)
def file_in(p_file, e_file):
    return 1


@epilog(binary=os.getcwd() + "/src/misc/hello.sh",
        args="{{text}} {{file_out}}")
@task(returns=1, file_out=FILE_OUT)
def std_out(ret_value, text, file_out):
    return ret_value


@prolog(binary="echo", args="{{a}}_{{b}}")
@epilog(binary="echo", args="{{c}}_{{d}}")
@task(returns=4)
def task_1(a, b, c, d):
    return a, b, c, d


@prolog(binary="echo", args="prolog_{{b}}")
@epilog(binary="echo", args="epilog_{{d}}")
@mpi(binary="echo", runner="mpirun", args="mpi_{{a}}")
@task(returns=1)
def task_2(a, b, c, d):
    pass


class TestPrologEpilog(unittest.TestCase):

    def testFBEV(self):
        ev = cwo(skip_failure())
        self.assertTrue(ev, "ERROR: Prolog / Epilog failure shouldn't have "
                            "stopped the task execution")

    def testStdOutFile(self):
        text = "some text for epilog"
        outfile = "src/misc/outfile"
        ret_val = cwo(std_out(10, text, outfile))
        self.assertEqual(10, ret_val, "ERROR: testStdOutFile return value "
                                      "is NOT correct ")

    def testFileInParam(self):
        p_file = "src/misc/p_file"
        e_file = "src/misc/e_file"
        ret = cwo(file_in(p_file, e_file))
        self.assertEqual(ret, 1, "ERROR: testFileInParam ret value NOT correct")

    def testParams(self):
        args_mpmd("next monday", "next friday")
        cb()

    def testBasic(self):
        ret = basic()
        self.assertTrue(ret)

    def testMpiSkipFailure(self):
        cwo(mpi_skip_failure())
        cb()

    def testOutParam(self):
        t_1 = task_1("AAA", "BBB", "CCC", "DDD")
        t_2 = cwo(task_2(*t_1))
        self.assertEqual(t_2, 0, "ERROR: testOutParam exit value not 0.")
