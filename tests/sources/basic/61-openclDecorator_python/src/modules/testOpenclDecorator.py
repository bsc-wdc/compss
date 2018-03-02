import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier
from pycompss.api.opencl import opencl
from pycompss.api.constraint import constraint

@opencl(kernel="date", workingDir="/tmp")
@task()
def myDate(dprefix, param):
    pass

@constraint(computingUnits="2")
@opencl(kernel="date", workingDir="/tmp")
@task()
def myDateConstrained(dprefix, param):
    pass


class testOpenclDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate("-d", "next friday")
        compss_barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        compss_barrier()
