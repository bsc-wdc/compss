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
@task(file_path=FILE_INOUT)
def increment(file_path):
    # Read value
    fis = open(file_path, 'r')
    value = fis.read()
    fis.close()

    # Write value
    fos = open(file_path, 'w')
    fos.write(str(int(value) + 1))
    fos.close()


@container(engine="DOCKER",
           container="centos",
           binary="ls", working_dir="/home/compss/")
@task(result={Type: FILE_OUT, StdIOStream: STDOUT})
def docker_func(result):
    pass


@container(engine="SINGULARITY",
           container="/home/compss/singularity/examples/ubuntu_latest.sif",
           binary="ls", working_dir="/home/compss/")
@task(result={Type: FILE_OUT, StdIOStream: STDOUT})
def singularity_func(result):
    pass


@binary(binary="ls", working_dir="${TEST_WORKING_DIR}")
@task(result={Type: FILE_OUT, StdIOStream: STDOUT})
def exec_ls(result):
    pass


# Tests

class testContainerDecorator(unittest.TestCase):

    def testContainer(self):
        initial_value = 1
        file_name = "counter"
        infile = "infile.txt"

        # Write value
        fos = open(file_name, 'w')
        fos.write(initial_value)
        fos.close()
        print("Initial counter value is " + str(initial_value))

        # Execute increment
        increment(file_name)

        # Write new value
        fis = compss_open(file_name, 'r+')
        final_value = fis.read()
        fis.close()
        print("Final counter value is " + str(final_value))

        # Execute BINARY_FUNC
        docker_func(infile)
        # Write new value
        fis_ls = compss_open(infile, 'r+')
        final_value = fis_ls.read()
        fis_ls.close()
        print(final_value)

        # Execute LS
        # singularity_func(infile)

        # Write new value
        fis_ls = compss_open(infile, 'r+')
        final_value = fis_ls.read()
        fis_ls.close()
        print(final_value)

        exec_ls(infile)

        # Write new value
        fis_ls = compss_open(infile, 'r+')
        final_value = fis_ls.read()
        fis_ls.close()
        print(final_value)
