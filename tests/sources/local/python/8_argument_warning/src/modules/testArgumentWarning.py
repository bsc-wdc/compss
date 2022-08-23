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

# All of the following tasks include an unexpected argument
# So they must raise a WARNING message through stderr which is
# checked in the result script.

# Wrong working dir argument (must be working_dir or workingDir)
@binary(binary="date", WorkingDir="/tmp")  # the correct way is working_dir
@task()
def binary_task(dprefix, param):
    pass

# Unexpected argument included "bad_arg"
@compss(runcompss="${RUNCOMPSS}", flags="-d", app_name="${APP_DIR}/src/simple_compss_nested.py", worker_in_master="false", computing_nodes="2", bad_arg=1234)
@constraint(computing_units="2")
@task(returns=int)
def compss_task(value):
    pass

# Wrong runner argument (must be runner instead of Runner)
@decaf(working_dir=".", Runner="mpirun", df_script="${APP_DIR}/src/test_decaf.py", df_executor="test.sh", df_lib="lib")
@task(param=FILE_OUT)
def my_decaf_task(param):
    pass

@task(returns=int)
def slow_task(value):
    return value * value * value

# Unexpected argument included "bad_arg"
@implement(source_class="modules.testArgumentWarning", method="slow_task", bad_arg="unexpected")
@constraint(computing_units="1")
@task(returns=list)
def better_task(value):
    return value ** 3

# Wrong working dir argument (must be working_dir or workingDir)
@mpi(binary="date", WorkingDir="/tmp", runner="mpirun")
@task()
def mpi_task(dprefix, param):
    pass

# Unexpected argument included "bad_arg"
@constraint(computing_units="2")
@multinode(computing_nodes="2", bad_arg=1234)
@task(returns=1)
def multi_node_task():
    return 0

# Wrong working dir argument (must be working_dir or workingDir)
@ompss(binary="date", WorkingDir="/tmp")
@task()
def ompss_task(dprefix, param):
    pass

# # Wrong working dir argument (must be working_dir or workingDir)
# @opencl(kernel="date", WorkingDir="/tmp")
# @task()
# def opencl_task(dprefix, param):
#     pass

# Wrong returns argument (must be returns)
@task(Returns=1)
def task1_task(i):
    pass

# a is not defined in the function as parameter
@task(returns=1, a=INOUT)
def task2_task(i):
    return 1

# pirority is wrong (must be priority)
@task(returns=1, pirority=True)
def task3_task(i):
    return 1

# The user uses kwargs: MUST NOT THROW WARNING
@task(returns=1, i=IN)
def task_kwarg(i, **kwargs):
    return 1

# The user uses args: MUST NOT THROW WARNING
@task(returns=1, i=IN)
def task_args(i, *args):
    return 1

class testArgumentWarning(unittest.TestCase):

    def testBinaryArgWarn(self):
        binary_task("-d", "next friday")
        compss_barrier()

    def testCompssArgWarn(self):
        ev = compss_task(1)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

    # TODO: currently, if the argument is not recognized -> ERROR
    @unittest.skip("The runtime throws an error if unrecognized constraint")
    def testConstraintArgWarn(self):
        pass

    def testDecafArgWarn(self):
        my_decaf_task("outFileAll")
        compss_barrier()

    def testImplementArgWarn(self):
        v = 20
        o = slow_task(v)
        o = compss_wait_on(o)
        self.assertEqual(o, v * v * v)

    def testMpiArgWarn(self):
        mpi_task("-d", "next friday")
        compss_barrier()

    def testMultinodeArgWarn(self):
        ev = multi_node_task()
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

    def testOmpssArgWarn(self):
        ompss_task("-d", "next monday")
        compss_barrier()

    @unittest.skip("OpenCL unsupported")
    def testOpenclArgWarn(self):
        opencl_task("-d", "next monday")
        compss_barrier()

    @unittest.skip("TODO: do the check with @parallel")
    def testParallelArgWarn(self):
        # TODO: check with @parallel
        pass

    def testTaskArgWarn(self):
        task1_task(1)
        o = task2_task(2)
        p = task3_task(3)
        compss_barrier()
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, p)

    def testTaskKwargWarn(self):
        kw = {'i': 1, 'j':2}
        o = task_kwarg(**kw)
        o = compss_wait_on(o)
        self.assertEqual(o, 1)

    def testTaskArgsWarn(self):
        args = [1, 2]
        o = task_args(*args)
        o = compss_wait_on(o)
        self.assertEqual(o, 1)
