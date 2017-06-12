import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import barrier
from pycompss.api.decaf import decaf
from pycompss.api.constraint import constraint

@decaf(dfScript="$PWD/decaf/test-auto.py")
@task(param=FILE_OUT)
def myDecaf(dprefix, param):
    pass

@decaf(workingDir=".", runner="mpirun", dfScript="$PWD/decaf/test.py", dfExecutor="test.sh", dfLib="lib")
@task(param=FILE_OUT)
def myDecafAll(dprefix, param):
    pass

@constraint(computingUnits="2")
@decaf(runner="mpirun", computingNodes=2, dfScript="$PWD/decaf/test-2.py", dfExecutor="test-2.sh", dfLib="lib")
@task(param=FILE_OUT)
def myDecafConstrained(dprefix, param):
    pass

class testDecafDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDecaf("--file", "outFile")
        barrier()

    def testFunctionalUsageAll(self):
        myDecafAll("--file", "outFileAll")
        barrier()

    def testFunctionalUsageWithConstraint(self):
        myDecafConstrained("--file", "outFileConstrained")
        barrier()
