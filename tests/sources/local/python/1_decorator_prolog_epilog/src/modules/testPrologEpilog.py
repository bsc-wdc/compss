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
from pycompss.api.epilog import epilog


@prolog(binary="echo", params="just a prolog")
@epilog(binary="echo", params="just an epilog")
@task()
def basic():
    return True


@prolog(binary="date", params="-d {{a}}")
@epilog(binary="date", params="-d {{b}}")
@mpmd_mpi(runner="mpirun",
          programs=[
               dict(binary="echo", processes=2, params="program 1"),
               dict(binary="echo", processes=2, params="program 2")
          ])
@task()
def params_mpmd(a, b):
    pass


@prolog(binary="date", params="-date", fail_by_exit_value=False)
@epilog(binary="date", params="-date", fail_by_exit_value=False)
@task()
def skip_failure():
    return True


@prolog(binary="date", params="-wrong", fail_by_exit_value=False)
@mpi(runner="mpirun", binary="echo", params="prolog failed successfully")
@task(returns=1)
def mpi_skip_failure():
    pass


@prolog(binary="date", params="-wrong", fail_by_exit_value=False)
@binary(binary="echo", params="prolog failed successfully")
@task(returns=1)
def mpi_skip_failure():
    pass


class TestPrologEpilog(unittest.TestCase):

    def testFBEV(self):
        ev = cwo(skip_failure())
        self.assertTrue(ev, "ERROR: Prolog / Epilog failure shouldn't have "
                            "stopped the task execution")

    def testParams(self):
        params_mpmd("next monday", "next friday")
        cb()

    def testBasic(self):
        ret = basic()
        self.assertTrue(ret)

    def testSomeMpi(self):
        cwo(mpi_skip_failure())
        cb()
