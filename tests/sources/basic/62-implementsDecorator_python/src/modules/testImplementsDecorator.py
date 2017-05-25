import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on
from pycompss.api.implement import implement
from pycompss.api.constraint import constraint

@implement(source_class="testImplementsDecorator", method="addtwovectors")
@constraint(computingUnits="2")
@task(returns=list)
def myfunctionWithNumpy2Cores(list1, list2):
    import numpy as np
    x = np.array(list1)
    y = np.array(list2)
    z = x + y
    return z.tolist()

@implement(source_class="testImplementsDecorator", method="addtwovectors")
@task(returns=list)
def myfunctionWithNumpy(list1, list2):
    import numpy as np
    x = np.array(list1)
    y = np.array(list2)
    z = x + y
    return z.tolist()

@task(returns=list)
def addtwovectors(list1, list2):
    assert(len(list1) == len(list2))
    for i in range(len(list1)):
        list1[i] += list2[i]
    return list1


class testImplementsDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        a = [1 for i in range(10)]
        b = [2 for i in range(10)]
        c = addtwovectors(a, b)
        c = compss_wait_on(c)

        assert len(c) == 10, "[ERROR] Return length error."
        for i in c:
            assert i==3, "[ERROR] Wrong value"
