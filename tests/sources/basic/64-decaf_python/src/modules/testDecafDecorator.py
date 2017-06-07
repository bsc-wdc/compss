import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import barrier
from pycompss.api.decaf import decaf
from pycompss.api.constraint import constraint

@decaf(binary="date", workingDir="/tmp", runner="mpirun", dfScript="myscript")
@task()
def myDate(dprefix, param):
    pass

@decaf(binary="date", workingDir="/tmp", runner="mpirun", dfScript="myscript", dfExecutor="executor", dfLib="lib")
@task()
def myDate(dprefix, param):
    pass

@constraint(computingUnits="2")
@decaf(binary="date", workingDir="/tmp", runner="mpirun", computingNodes=2, dfScript="myscript", dfExecutor="executor", dfLib="lib")
@task()
def myDateConstrained(dprefix, param):
    pass

# TODO: ADD SUPPORT FOR STREAMS !!!

class testDecafDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        barrier()
