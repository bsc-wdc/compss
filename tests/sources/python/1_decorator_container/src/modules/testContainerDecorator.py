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
from pycompss.api.api import compss_open

# Imports
import unittest


# Tasks definitions
@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls",
        working_dir="${TEST_WORKING_DIR}")
@task(result={Type: FILE_OUT, StdIOStream: STDOUT})
def func1(result):
    pass

@container(engine="DOCKER",
           image="ubuntu")
@task(result={Type: FILE_OUT, StdIOStream: STDOUT})
def func2(result):
    pass

@binary(binary="ls",
        working_dir="${TEST_WORKING_DIR}")
@task(result={Type: FILE_OUT, StdIOStream: STDOUT})
def func3(result):
    pass

# Tests

class testContainerDecorator(unittest.TestCase):

    def testContainer(self):
        docker_out = "docker_output.txt"
        func1(docker_out)
        func2(docker_out)
        func3(docker_out)

