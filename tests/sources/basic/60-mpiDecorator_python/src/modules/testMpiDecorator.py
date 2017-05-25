import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import barrier
from pycompss.api.mpi import mpi
from pycompss.api.constraint import constraint

@mpi(binary="date", workingDir="/tmp", runner="mpirun")
@task()
def myDate(dprefix, param):
    pass

@constraint(computingUnits="2")
@mpi(binary="date", workingDir="/tmp", runner="mpirun", computingNodes=2)
@task()
def myDateConstrained(dprefix, param):
    pass


class testMpiDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        barrier()
