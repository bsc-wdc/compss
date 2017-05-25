import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import barrier, compss_open
from pycompss.api.binary import binary
from pycompss.api.constraint import constraint


@binary(binary="date", workingDir="/tmp")
@task()
def myDate(dprefix, param):
    pass

@constraint(computingUnits="2")
@binary(binary="date", workingDir="/tmp")
@task()
def myDateConstrained(dprefix, param):
    pass

@binary(binary="sed", workingDir=".")
@task(file=FILE_IN)
def mySedIN(expression, file):
    pass

# skipped
@binary(binary="sed", workingDir=".")
@task(file=FILE_INOUT)
def mySedINOUT(flag, expression, file):
    pass

# skipped
@binary(binary="grep", workingDir=".")
@task(infile=FILE_IN, result=FILE_OUT)
def myGrepper(keyword, infile, redirect, result):
    pass


class testBinaryDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        barrier()

    def testFileManagementIN(self):
        infile = "src/infile"
        mySedIN('s/Hi/HELLO/g', infile)
        barrier()

    @unittest.skip("The redirection has to be done through streams -> ignoring")
    def testFileManagementINOUT(self):
        inoutfile = "src/inoutfile"
        mySedINOUT('-i', 's/Hi/HELLO/g', inoutfile)
        with compss_open(inoutfile, "r") as finout_r:
            content_r = finout_r.read()
        # Check if there are no Hi words, and instead there is HELLO
        print "XXXXXXXXXXXX"
        print content_r
        print "XXXXXXXXXXXX"

    @unittest.skip("Redirection not supported yet -> ignoring")
    def testFileManagement(self):
        infile = "infile"
        outfile = "outfile"
        myGrepper("Hi", infile, ">>", outfile)
        barrier()
        with compss_open(outfile, "r") as fout_r:
            content_r = fout_r.read()
        # Check if there are only lines containint "Hi"
        print "XXXXXXXXXXXX"
        print content_r
        print "XXXXXXXXXXXX"
