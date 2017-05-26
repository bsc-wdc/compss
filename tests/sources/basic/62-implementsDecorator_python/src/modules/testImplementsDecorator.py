import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on
from pycompss.api.implement import implement
from pycompss.api.constraint import constraint

@implement(source_class="modules.testImplementsDecorator", method="addtwovectors")
@constraint(AppSoftware="NUMPY")
@task(returns=list)
def myfunctionWithNumpy(list1, list2):
    print "myfunctionWithNumpy"
    assert(len(list1) == len(list2))
    import numpy as np
    x = np.array(list1)
    y = np.array(list2)
    z = x + y
    return z.tolist()

# TODO: FUTURE WORK TO PROVIDE SUPPORT FOR THIS TYPE OF TASKS
# THE SCHEDULER WILL HAVE TO BE ABLE TO CHECK WHICH ONE IS FASTER
# AND DECIDE WHETHER TO CHOOSE THE IMPLEMENTATION OR THE REAL ON
# EACH RESOURCE.
'''
@implement(source_class="modules.testImplementsDecorator", method="addtwovectors")
@task(returns=list)
def myfunctionImplementation(list1, list2):
    import numpy as np
    x = np.array(list1)
    y = np.array(list2)
    z = x + y
    return z.tolist()
'''

@constraint(AppSoftware="NonNumpy")
@task(returns=list)
def addtwovectors(list1, list2):
    print "addtwovectors"
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
