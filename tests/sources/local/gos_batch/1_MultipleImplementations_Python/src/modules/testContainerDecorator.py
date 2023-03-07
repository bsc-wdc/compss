#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# For better print formatting
from __future__ import print_function

# PyCOMPSs imports
from pycompss.api.task import task
from pycompss.api.binary import binary
from pycompss.api.container import container
from pycompss.api.software import software
from pycompss.api.api import compss_barrier, compss_wait_on
from pycompss.api.parameter import *

# Imports
import unittest
import os


#
# Binary Tasks definition
#
@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls")
@task()
def task_binary_empty():
    pass


@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls")
@task(returns=1)
def task_binary_ev():
    pass


@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls",
        working_dir="${TEST_WORKING_DIR}")
@task()
def task_binary_wd():
    pass


@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls",
        working_dir="${TEST_WORKING_DIR}")
@task(stdout={Type: FILE_OUT, StdIOStream: STDOUT}, stderr={Type: FILE_OUT, StdIOStream: STDERR})
def task_binary_std(stdout, stderr):
    pass


#
# Python Tasks definition
#
@container(engine="DOCKER",
           image="compss/compss")
@task()
def task_python_empty():
    print("Hello from Task Python EMPTY")


@container(engine="DOCKER",
           image="compss/compss")
@task(num=IN, in_str=IN, fin=FILE_IN)
def task_python_args(num, in_str, fin):
    print("Hello from Task Python ARGS")
    print("- Arg 1: num -- " + str(num))
    print("- Arg 1: str -- " + str(in_str))
    print("- Arg 1: fin -- " + str(fin))
    with open(fin, 'r') as f:
        print(f.read())


@container(engine="DOCKER",
           image="compss/compss")
@task(returns=1)
def task_python_return_int():
    print("Hello from Task Python RETURN")
    return 3


@container(engine="DOCKER",
           image="compss/compss")
@task(returns=1, num=IN, in_str=IN, fin=FILE_IN)
def task_python_return_str(num, in_str, fin):
    print("Hello from Task Python RETURN")
    print("- Arg 1: num -- " + str(num))
    print("- Arg 1: str -- " + str(in_str))
    print("- Arg 1: fin -- " + str(fin))
    return "Hello"


@container(engine="DOCKER",
           image="compss/compss")
@task(finout=FILE_INOUT)
def task_python_inout(finout):
    print("Hello from Task Python ARGS")

    # Read
    print("- Arg 1: num -- " + str(finout))
    with open(finout, 'r') as f:
        print(f.read())

    # Write
    with open(finout, 'a') as f:
        f.write("Hello from task!\n")


@software(config_file=os.getcwd() + "/src/config/container_basic.json")
@task()
def task_container_basic():
    pass


@software(config_file=os.getcwd() + "/src/config/container_pycompss.json")
@task(returns=1)
def task_python_return_int():
    print("Hello from Task Python RETURN")
    return 3


# Tests

class testContainerDecorator(unittest.TestCase):

    def test_software_container(self):
        task_container_basic()
        compss_barrier()

    def test_software_pycompss(self):
        ret_int = task_python_return_int()
        ret_int = compss_wait_on(ret_int)
        self.assertEquals(ret_int, 3)

    def test_binary_container(self):
        # Imports
        from pycompss.api.api import compss_barrier
        from pycompss.api.api import compss_wait_on
        from pycompss.api.api import compss_open

        # Test empty binary execution
        task_binary_empty()

        compss_barrier()

        # Test exit value
        ev = task_binary_ev()

        ev = compss_wait_on(ev)
        self.assertEquals(ev, 0)

        # Test working dir
        # WARN: Check WD in result script
        task_binary_wd()

        compss_barrier()

        # Test stdout and stderr
        stdout = "binary_output.txt"
        stderr = "binary_error.txt"
        task_binary_std(stdout, stderr)

        print("STDOUT:")
        with compss_open(stdout, 'r') as f:
            content = f.read()
            print(content)
            self.assertTrue(len(content) != 0)
        print("STDERR:")
        with compss_open(stderr, 'r') as f:
            print(f.read())

    def test_python_container(self):
        # Imports
        from pycompss.api.api import compss_barrier
        from pycompss.api.api import compss_wait_on
        from pycompss.api.api import compss_open

        # Test empty Python execution
        task_python_empty()

        compss_barrier()

        # Test IN arguments
        # WARN: Check task output in result script
        num = 1234
        in_str = "Hello World!"
        fin = "in.file"
        with open(fin, 'w') as f:
            f.write("Hello from main!\n")

        task_python_args(num, in_str, fin)

        compss_barrier()

        # Test returns
        ret_int = task_python_return_int()
        ret_int = compss_wait_on(ret_int)
        self.assertEquals(ret_int, 3)

        ret_str = task_python_return_str(num, in_str, fin)
        ret_str = compss_wait_on(ret_str)
        self.assertEquals(ret_str, "Hello")

        # Test INOUT
        finout = "inout.file"
        with open(finout, 'w') as f:
            f.write("Hello from main!\n")

        task_python_inout(finout)

        print("FINOUT:")
        with compss_open(finout, 'r') as f:
            content = f.read()
            print(content)
            self.assertTrue("Hello from task!" in content)
