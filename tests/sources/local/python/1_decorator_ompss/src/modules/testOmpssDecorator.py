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
from pycompss.api.ompss import ompss
from pycompss.api.constraint import constraint


@ompss(binary="date", working_dir="/tmp")
@task()
def myDate(dprefix, param):
    pass


@constraint(computing_units="2")
@ompss(binary="date", working_dir="/tmp")
@task()
def myDateConstrained(dprefix, param):
    pass


@constraint(computing_units="$CUS")
@ompss(binary="date", working_dir="/tmp")
@task()
def myDateConstrainedWithEnvVar(dprefix, param):
    pass


@ompss(binary="sed", working_dir=".")
@task(file=FILE_IN)
def mySedIN(expression, file):
    pass


@ompss(binary="date", working_dir=".")
@task(returns=int)
def myReturn():
    pass


@ompss(binary="./private.sh", working_dir=os.getcwd() + '/src/scripts/',
       fail_by_exit_value=False)
@task(returns=int)
def failedBinary(code):
    pass


@ompss(binary="sed", working_dir=".")
@task(file=FILE_INOUT)
def mySedINOUT(flag, expression, file):
    pass


@ompss(binary="grep", working_dir=".")
# @task(infile=Parameter(TYPE.FILE, DIRECTION.IN, IOSTREAM.STDIN), result=Parameter(TYPE.FILE, DIRECTION.OUT, IOSTREAM.STDOUT))
# @task(infile={Type:FILE_IN, StdIOStream:STDIN}, result={Type:FILE_OUT, StdIOStream:STDOUT})
@task(infile={Type: FILE_IN_STDIN}, result={Type: FILE_OUT_STDOUT})
def myGrepper(keyword, infile, result):
    pass


@ompss(binary="ls")
@task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Type: IN, Prefix: "--sort="})
def myLs(flag, hide, sort):
    pass


@ompss(binary="ls")
@task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Prefix: "--sort="})
def myLsWithoutType(flag, hide, sort):
    pass


@ompss(binary="./checkNames.sh", working_dir=os.getcwd() + '/src/scripts/')
@task(f=FILE_IN, fp={Type: FILE_IN, Prefix: "--prefix="}, fout={Type: FILE_OUT}, returns=int)
def checkFileNames(f, fp, name, fout):
    pass


@ompss(binary="./checkString.sh", working_dir=os.getcwd() + '/src/scripts/')
@task(returns=int)
def checkStringParam(string_param):
    pass


class testOmpssDecorator(unittest.TestCase):

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
        self.assertEqual(ev, 123)  # own exit code for failed execution

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

    def testStringParams(self):
        string_param = "This is a string."
        exit_value1 = checkStringParam(string_param)
        exit_value2 = checkStringParam(string_param)
        exit_value1 = compss_wait_on(exit_value1)
        exit_value2 = compss_wait_on(exit_value2)
        self.assertEqual(exit_value1, 0)
        self.assertEqual(exit_value2, 0)
