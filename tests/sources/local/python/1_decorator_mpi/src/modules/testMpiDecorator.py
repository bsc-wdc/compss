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
@mpi(binary="date", working_dir="/tmp", runner="mpirun", processes=2, scale_by_cu=True)
@task()
def myDateConstrained(dprefix, param):
    pass


@mpi(binary="date", working_dir="/tmp", runner="mpirun",)
@task()
def myDateDef(dprefix="-d", param="next wednesday"):
    pass


@mpi(binary="date", args="-d {{arg}}", working_dir="/tmp", runner="mpirun",)
@task()
def myDateDef_2(arg):
    pass


@constraint(computing_units="$CUS")
@mpi(binary="date", working_dir="/tmp", runner="mpirun", processes="$CUS", scale_by_cu=True)
@task()
def myDateConstrainedWithEnvVar(dprefix, param):
    pass


@mpi(binary="sed", working_dir=".", runner="mpirun", processes="4")
@task(file=FILE_IN)
def mySedIN(expression, file):
    pass


@mpi(binary="date", working_dir=".", runner="mpirun", processes=1)
@task(returns=int)
def myReturn():
    pass


@mpi(binary="./private.sh", working_dir=os.getcwd() + '/src/scripts/', runner="mpirun",
     processes=1, fail_by_exit_value=False)
@task(returns=int)
def failedBinary(code):
    pass


@mpi(binary="sed", working_dir=".", runner="mpirun")
@task(file=FILE_INOUT)
def mySedINOUT(flag, expression, file):
    pass


@mpi(binary="grep", working_dir=".", runner="mpirun")
# @task(infile=Parameter(TYPE.FILE, DIRECTION.IN, IOSTREAM.STDIN), result=Parameter(TYPE.FILE, DIRECTION.OUT, IOSTREAM.STDOUT))
# @task(infile={Type:FILE_IN, StdIOStream:STDIN}, result={Type:FILE_OUT, StdIOStream:STDOUT})
@task(infile={Type: FILE_IN_STDIN}, result={Type: FILE_OUT_STDOUT})
def myGrepper(keyword, infile, result):
    pass


@mpi(binary="ls", runner="mpirun", processes=2)
@task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Type: IN, Prefix: "--sort="})
def myLs(flag, hide, sort):
    pass


@mpi(binary="ls", runner="mpirun", processes=2)
@task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Prefix: "--sort="})
def myLsWithoutType(flag, hide, sort):
    pass


@mpi(binary="./checkNames.sh", working_dir=os.getcwd() + '/src/scripts/', runner="mpirun",
     processes=1, fail_by_exit_value=False)
@task(f=FILE_IN, fp={Type: FILE_IN, Prefix: "--prefix="}, fout={Type: FILE_OUT}, returns=int)
def checkFileNames(f, fp, name, fout):
    pass


@mpi(binary="./checkString.sh", working_dir=os.getcwd() + '/src/scripts/', runner="mpirun",
     processes=1, fail_by_exit_value=False)
@task(returns=int)
def checkStringParam(string_param):
    pass


@mpi(runner="mpirun", binary="pwd", working_dir=os.getcwd() + '{{wd}}')
@task(result={Type: FILE_OUT_STDOUT})
def myWd(wd, result):
    pass


@mpi(runner="mpirun", binary="touch", args="{{fayl}}", working_dir="/tmp")
@task(returns=1, fayl=FILE_OUT)
def create_file_in_wd(fayl):
    pass


@mpi(runner="mpirun", binary="touch", args="holalaa", working_dir="/tmp")
@task(returns=1, fayl=FILE_IN)
def file_in_wd(fayl):
    pass


class testMpiDecorator(unittest.TestCase):

    def testFileInWorkingDir(self):
        new_file = "/tmp/hola.txt"
        res = compss_wait_on(create_file_in_wd(new_file))
        self.assertEqual(res, 0, "Failed to create a new file in working dir")
        ret = compss_wait_on(file_in_wd(new_file))
        compss_barrier()
        self.assertTrue(os.path.isfile(new_file), "FILE_IN from working dir "
                                                  "has been removed.")

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        compss_barrier()

    def testDefaultValue(self):
        myDateDef()
        compss_barrier()

    def testDefaultValue_2(self):
        myDateDef_2("last wednesday")
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

    def testStringParams(self):
        string_param = "This is a string."
        exit_value1 = checkStringParam(string_param)
        exit_value2 = checkStringParam(string_param)
        exit_value1 = compss_wait_on(exit_value1)
        exit_value2 = compss_wait_on(exit_value2)
        self.assertEqual(exit_value1, 0)
        self.assertEqual(exit_value2, 0)

    def testParamInWD(self):
        wd = '/test_param_in_wd'
        outfile = "param_wd_out"
        myWd(wd, outfile)
        compss_barrier()
