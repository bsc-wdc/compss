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

@binary(binary="sed", workingDir=".")
@task(file=FILE_INOUT_STDIN)
def mySedINOUT(flag, expression, file):
    pass

@binary(binary="grep", workingDir=".")
#@task(infile=Parameter(TYPE.FILE, DIRECTION.IN, STREAM.STDIN), result=Parameter(TYPE.FILE, DIRECTION.OUT, STREAM.STDOUT))
#@task(infile={Type:FILE_IN, Stream:STDIN}, result={Type:FILE_OUT, Stream:STDOUT})
@task(infile={Type:FILE_IN_STDIN}, result={Type:FILE_OUT_STDOUT})
def myGrepper(keyword, infile, result):
    pass

@binary(binary="ls")
@task(hide={Type:FILE_IN, Prefix:"--hide="}, sort={Type:IN, Prefix:"--sort="})
def myLs(flag, hide, sort):
    pass

@binary(binary="ls")
@task(hide={Type:FILE_IN, Prefix:"--hide="}, sort={Prefix:"--sort="})
def myLsWithoutType(flag, hide, sort):
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

    # Fails when retrieving the file... doesn't exist?
    @unittest.skip("Fails to retrieve the inout file.")
    def testFileManagementINOUT(self):
        inoutfile = "src/inoutfile"
        mySedINOUT('-i', 's/Hi/HELLO/g', inoutfile)
        with compss_open(inoutfile, "r") as finout_r:
            content_r = finout_r.read()
        # Check if there are no Hi words, and instead there is HELLO
        print "XXXXXXXXXXXX"
        print content_r
        print "XXXXXXXXXXXX"

    def testFileManagement(self):
        infile = "src/infile"
        outfile = "src/grepoutfile"
        myGrepper("Hi", infile, outfile)
        barrier()

    def testFilesAndPrefix(self):
        flag = '-l'
        infile = "src/infile"
        sort = "size"
        myLs(flag ,infile, sort)
        barrier()

    def testFilesAndPrefixWithoutType(self):
        flag = '-l'
        infile = "src/inoutfile"
        sort = "time"
        myLsWithoutType(flag ,infile, sort)
        barrier()
