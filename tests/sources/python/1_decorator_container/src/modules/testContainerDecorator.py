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
from pycompss.api.parameter import *

# Imports
import unittest


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
def task_python_return():
    print("Hello from Task Python RETURN")
    return 3


# Tests

class testContainerDecorator(unittest.TestCase):

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

        # Test empty Python execution
        task_python_empty()

        compss_barrier()

        # Test IN arguments
        # WARN: Check task output in result script
        num = 1234
        in_str = "Hello World!"
        fin = "in.file"
        with open(fin, 'w') as f:
            f.write("Hello World!")

        task_python_args(num, in_str, fin)

        compss_barrier()

        # Test returns
        # TODO: Enable test
        # ret = task_python_return()

        # ret = compss_wait_on(ret)
        # print("Return: " + str(ret))
