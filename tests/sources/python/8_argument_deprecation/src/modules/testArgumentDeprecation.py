#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *

from pycompss.api.binary import binary
from pycompss.api.compss import compss
from pycompss.api.constraint import constraint
from pycompss.api.decaf import decaf
from pycompss.api.implement import implement
from pycompss.api.mpi import mpi
from pycompss.api.multinode import multinode
from pycompss.api.ompss import ompss
# from pycompss.api.opencl import opencl

# All of the following tasks include a deprecated argument which is
# still supported.
# However, they must raise a WARNING message through stderr which is
# checked in the result script.
# The correct way is in lower case and underscores (snake).

# Deprecated working dir argument
@binary(binary="date", workingDir="/tmp")  # the correct way is working_dir
@task()
def binary_task(dprefix, param):
    pass

# Deprecated app name, worker in master and computing nodes arguments
@compss(runcompss="${RUNCOMPSS}", flags="-d", appName="${APP_DIR}/src/simple_compss_nested.py", workerInMaster="false", computingNodes="2")
@constraint(computing_units="2")
@task(returns=int)
def compss_task(value):
    pass

# Deprecated working dir, df script, df executor and df lib arguments
@decaf(workingDir=".", runner="mpirun", dfScript="${APP_DIR}/src/test_decaf.py", dfExecutor="test.sh", dfLib="lib")
@task(param=FILE_OUT)
def my_decaf_task(param):
    pass

@task(returns=int)
def slow_task(value):
    return value * value * value

# Deprecated sorce class argument
@implement(sourceClass="modules.testArgumentDeprecation", method="slow_task")
@constraint(computing_units="1")
@task(returns=list)
def better_task(value):
    return value ** 3

# Deprecated working dir argument
@mpi(binary="date", workingDir="/tmp", runner="mpirun")
@task()
def mpi_task(dprefix, param):
    pass

# Deprecated computing nodes argument
@constraint(computing_units="2")
@multinode(computingNodes="2")
@task(returns=1)
def multi_node_task():
    return 0

# Deprecated working dir argument
@ompss(binary="date", workingDir="/tmp")
@task()
def ompss_task(dprefix, param):
    pass

# # Deprecated working dir argument
# @opencl(kernel="date", workingDir="/tmp")
# @task()
# def opencl_task(dprefix, param):
#     pass


class testArgumentDeprecation(unittest.TestCase):

    def testBinaryArgDepr(self):
        binary_task("-d", "next friday")
        compss_barrier()

    def testCompssArgDepr(self):
        ev = compss_task(1)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

    # TODO: currently, if the argument is not recognized -> ERROR
    @unittest.skip("The runtime throws an error if unrecognized constraing")
    def testConstraintArgDepr(self):
        pass

    def testDecafArgDepr(self):
        my_decaf_task("outFileAll")
        compss_barrier()

    def testImplementArgDepr(self):
        v = 20
        o = slow_task(v)
        o = compss_wait_on(o)
        self.assertEqual(o, v * v * v)

    def testMpiArgDepr(self):
        mpi_task("-d", "next friday")
        compss_barrier()

    def testMultinodeArgDepr(self):
        ev = multi_node_task()
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

    def testOmpssArgDepr(self):
        ompss_task("-d", "next monday")
        compss_barrier()

    @unittest.skip("OpenCL unsupported")
    def testOpenclArgDepr(self):
        opencl_task("-d", "next monday")
        compss_barrier()

    @unittest.skip("TODO: do the check with @parallel")
    def testParallelArgDepr(self):
        # TODO: check with @parallel
        pass
