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
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier, compss_open, compss_wait_on
from pycompss.api.mpi import mpi
from pycompss.api.constraint import constraint


@mpi(binary="date", working_dir="/tmp", runner="mpirun")
@task()
def myDate(dprefix, param):
    pass


@constraint(computing_units="2")
@mpi(binary="date", working_dir="/tmp", runner="mpirun", computing_nodes=2)
@task()
def myDateConstrained(dprefix, param):
    pass


@constraint(computing_units="$CUS")
@mpi(binary="date", working_dir="/tmp", runner="mpirun", computing_nodes="$CUS")
@task()
def myDateConstrainedWithEnvVar(dprefix, param):
    pass


@mpi(binary="sed", working_dir=".", runner="mpirun", computing_nodes="4")
@task(file=FILE_IN)
def mySedIN(expression, file):
    pass


@mpi(binary="date", working_dir=".", runner="mpirun", computing_nodes=1)
@task(returns=int)
def myReturn():
    pass


@mpi(binary="./private.sh", working_dir=os.getcwd() + '/src/scripts/', runner="mpirun", computing_nodes=1)
@task(returns=int)
def failedBinary(code):
    pass


@mpi(binary="sed", working_dir=".", runner="mpirun")
@task(file=FILE_INOUT)
def mySedINOUT(flag, expression, file):
    pass


@mpi(binary="grep", working_dir=".", runner="mpirun")
# @task(infile=Parameter(TYPE.FILE, DIRECTION.IN, STREAM.STDIN), result=Parameter(TYPE.FILE, DIRECTION.OUT, STREAM.STDOUT))
# @task(infile={Type:FILE_IN, Stream:STDIN}, result={Type:FILE_OUT, Stream:STDOUT})
@task(infile={Type: FILE_IN_STDIN}, result={Type: FILE_OUT_STDOUT})
def myGrepper(keyword, infile, result):
    pass


@mpi(binary="ls", runner="mpirun", computing_nodes=2)
@task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Type: IN, Prefix: "--sort="})
def myLs(flag, hide, sort):
    pass


@mpi(binary="ls", runner="mpirun", computing_nodes=2)
@task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Prefix: "--sort="})
def myLsWithoutType(flag, hide, sort):
    pass

@mpi(binary="./checkNames.sh", working_dir=os.getcwd() + '/src/scripts/', runner="mpirun", computing_nodes=1)
@task(f=FILE_IN, fp={Type: FILE_IN, Prefix: "--prefix="}, fout={Type: FILE_OUT}, returns=int)
def checkFileNames(f, fp, name, fout):
    pass


class testMpiDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        compss_barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        compss_barrier()

    def testFunctionalUsageWithEnvVarConstraint(self):
        myDateConstrainedWithEnvVar("-d", "next tuesday")
        compss_barrier()

    def testFileManagementIN(self):
        infile = "src/infile"
        mySedIN('s/Hi/HELLO/g', infile)
        compss_barrier()

    def testReturn(self):
        ev = myReturn()
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

    def testFailedBinaryExitValue(self):
        ev = failedBinary(123)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 123)

    @unittest.skip("UNSUPPORTED WITH GAT")
    def testFileManagementINOUT(self):
        inoutfile = "src/inoutfile"
        mySedINOUT('-i', 's/Hi/HELLO/g', inoutfile)
        with compss_open(inoutfile, "r") as finout_r:
            content_r = finout_r.read()
        # Check if there are no Hi words, and instead there is HELLO
        if 'Hi' in content_r:
            self.fail("INOUT File failed.")

    def testFileManagement(self):
        infile = "src/infile"
        outfile = "src/grepoutfile"
        myGrepper("Hi", infile, outfile)
        compss_barrier()

    def testFilesAndPrefix(self):
        flag = '-l'
        infile = "src/infile"
        sort = "size"
        myLs(flag, infile, sort)
        compss_barrier()

    def testFilesAndPrefixWithoutType(self):
        flag = '-l'
        infile = "src/inoutfile"
        sort = "time"
        myLsWithoutType(flag, infile, sort)
        compss_barrier()

    def testCheckFileNames(self):
        f = "src/infile"
        fp = "src/infile"
        name = "infile"
        fout = "checkFileNamesResult.txt"
        exit_value = checkFileNames(f, fp, name, fout)
        exit_value = compss_wait_on(exit_value)
        with compss_open(fout) as result:
            data = result.read()
        print("CheckFileNamesResult: " + str(data))
        self.assertEqual(exit_value, 0, "At least one file name is NOT as expected: {}, {}, {}".format(f, fp, name))
