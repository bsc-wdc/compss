#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.mpi import mpi
from pycompss.api.api import compss_barrier


# Tasks definition
@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh")
@task(returns=1)
def default_cn():
    pass


@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh", computing_nodes=2)
@task(returns=1)
def static_int_cn():
    pass


@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh", computing_nodes="2")
@task(returns=1)
def static_str_cn():
    pass


@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh", computing_nodes="$CN")
@task(returns=1)
def dynamic_env_cn():
    pass


@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh", computing_nodes="${CN}")
@task(returns=1)
def dynamic_env_cn():
    pass


@mpi(runner="mpirun", binary="${PATH_BINARY}/simple.sh", computing_nodes="cn")
@task(returns=1)
def dynamic_gv_cn():
    global cn
    pass


# Test
class TestDynamicComputingNodes(unittest.TestCase):

    def test_default(self):
        default_cn()

        compss_barrier()
        print ("CORRECTNESS IS CHECKED IN THE RESULT SCRIPT")

    def test_static(self):
        static_int_cn()
        static_str_cn()

        compss_barrier()
        print ("CORRECTNESS IS CHECKED IN THE RESULT SCRIPT")

    def test_environment(self):
        dynamic_env_cn()
        dynamic_env_cn()

        compss_barrier()
        print ("CORRECTNESS IS CHECKED IN THE RESULT SCRIPT")

    def test_dynamic(self):
        global cn

        cn = 2
        dynamic_gv_cn()

        cn = 4
        dynamic_gv_cn()

        compss_barrier()
        print ("CORRECTNESS IS CHECKED IN THE RESULT SCRIPT")
