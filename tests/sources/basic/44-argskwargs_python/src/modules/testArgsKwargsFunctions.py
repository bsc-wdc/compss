import unittest
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *

@task(returns=int)
def argTask(*args):
    print "ARG: ", args
    return sum(args)
  
@task(returns=int)
def varargTask(v, w, *args):
    print "V: ", v
    print "W: ", w
    print "ARG: ", args
    return (v * w) + sum(args)

@task(returns=int)
def kwargTask(**kwargs):
    print "KARG: ", kwargs
    return len(kwargs)
  
@task(returns=int)
def varkwargTask(v, w , **kwargs):
    print "V: ", v
    print "W: ", w
    print "KARG: ", kwargs
    return (v * w) + len(kwargs)
  
@task(returns=int)
def argkwargTask(*args, **kwargs):
    print "ARG: ", args
    print "KARG: ", kwargs
    return sum(args) + len(kwargs)

@task(returns=int)
def varargkwargTask(v, w , *args, **kwargs):
    print "V: ", v
    print "W: ", w
    print "ARG: ", args
    print "KARG: ", kwargs
    return (v * w) + sum(args) + len(kwargs)

@task(returns=int)
def varargdefaultkwargTask(v, w, s = 2, *args, **kwargs):
    print "V: ", v
    print "W: ", w
    print "S: ", s
    print "ARGS: ", args
    print "KWARG: ", kwargs
    return (v * w) + sum(args) + len(kwargs) + s

class testArgsKwargsFunctions(unittest.TestCase):

    '''
    FUNCTION WITH *ARGS
    '''

    # we have arguments
    def testArgTask1(self):
        pending = argTask(1, 2)
        result = compss_wait_on(pending)
        self.assertEqual(result, 3)

    def testArgTask2(self):
        pending = argTask(1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    # args is empty
    def testArgTask3(self):
        pending = argTask()
        result = compss_wait_on(pending)
        self.assertEqual(result, 0)

    # args is not empty but args are an unpacked tuple
    def testArgTask4(self):
        my_tup = (1,2,3,4)
        pending = argTask(*my_tup)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    '''
        FUNCTION WITH ARGS + *ARGS
    '''

    def testVarArgTask1(self):
        pending = varargTask(10, 20, 1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 210)

    def testVarArgTask2(self):
        pending = varargTask(4, 50, 5, 4, 3, 2, 1)
        result = compss_wait_on(pending)
        self.assertEqual(result, 215)

    def testVarArgTask3(self):
        pending = varargTask(4, 50)
        result = compss_wait_on(pending)
        self.assertEqual(result, 200)

    '''
        FUNCTION WITH **KWARGS
    '''

    def testKwargTask1(self):
        pending = kwargTask(hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 1)

    def testKwargTask2(self):
        pending = kwargTask(this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 2)

    def testKwargTask3(self):
        pending = kwargTask()
        result = compss_wait_on(pending)
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS + **KWARGS
    '''

    def testVarKwargTask1(self):
        pending = varkwargTask(1, 2, hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 3)

    def testVarArgKwargTask2(self):
        pending = varkwargTask(2, 3, this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 8)

    def testVarArgKwargTask3(self):
        pending = varkwargTask(2, 3)
        result = compss_wait_on(pending)
        self.assertEqual(result, 6)

    '''
        FUNCTION WITH *ARGS + **KWARGS
    '''

    def testArgKwargTask1(self):
        pending = argkwargTask(1, 2, hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 4)

    def testArgKwargTask2(self):
        pending = argkwargTask(1, 2, 3, 4, this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 12)

    def testArgKwargTask3(self):
        pending = argkwargTask(1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    def testArgKwargTask4(self):
        pending = argkwargTask()
        result = compss_wait_on(pending)
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgKwargTask1(self):
        pending = varargkwargTask(1, 2, 3, 4, hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    def testVarArgKwargTask2(self):
        pending = varargkwargTask(1, 2, 3, 4, 5, 6, this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 22)

    '''
        FUNCTION WITH ARGS, DEFAULTED ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgDefaultKwargTask1(self):
        pending = varargdefaultkwargTask(1, 1)
        result = compss_wait_on(pending)
        self.assertEqual(result, 3)

    def testVarArgDefaultKwargTask2(self):
        pending = varargdefaultkwargTask(1, 2, 3)
        result = compss_wait_on(pending)
        self.assertEqual(result, 5)

    def testVarArgDefaultKwargTask3(self):
        pending = varargdefaultkwargTask(1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 9)

    def testVarArgDefaultKwargTask4(self):
        pending = varargdefaultkwargTask(1, 2, 3, 4, five=5)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)
