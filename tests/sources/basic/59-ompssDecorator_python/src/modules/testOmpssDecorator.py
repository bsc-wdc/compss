import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import barrier, compss_open
from pycompss.api.ompss import ompss
from pycompss.api.constraint import constraint


@ompss(binary="date", workingDir="/tmp")
@task()
def myDate():
    pass

@constraint(computingUnits="2")
@ompss(binary="date", workingDir="/tmp")
@task()
def myDateConstrained(dprefix, param):
    pass


class testOmpssDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        myDate()
        barrier()

    def testFunctionalUsageWithConstraint(self):
        myDateConstrained("-d", "next monday")
        barrier()
