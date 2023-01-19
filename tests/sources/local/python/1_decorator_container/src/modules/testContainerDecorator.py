#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# For better print formatting
from __future__ import print_function

from pycompss.api.mpi import mpi
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

from modules.tasks import *

# Tests

class testContainerDecorator(unittest.TestCase):

    def test_software_container(self):
        task_container_basic()
        compss_barrier()

    def test_mpi_container(self):
        ret_int = task_container_mpi(1)
        ret_int = compss_wait_on(ret_int)
        self.assertEquals(ret_int, 0)

    def test_software_pycompss(self):
        ret_int = task_python_return_int_soft()
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

        task_binary_options()

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
