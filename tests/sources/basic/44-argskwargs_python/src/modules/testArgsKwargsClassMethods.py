import unittest
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *

class testArgsKwargsClassMethods(unittest.TestCase):

    @classmethod
    @task(returns=int)
    def argTask(cls, *args):
        print "ARG: ", args
        return sum(args)

    @classmethod
    @task(returns=int)
    def varargTask(cls, v, w, *args):
        print "V: ", v
        print "W: ", w
        print "ARG: ", args
        return (v * w) + sum(args)

    @classmethod
    @task(returns=int)
    def kwargTask(cls, **kwargs):
        print "KARG: ", kwargs
        return len(kwargs)
    
    @classmethod  
    @task(returns=int)
    def varkwargTask(cls, v, w , **kwargs):
        print "V: ", v
        print "W: ", w
        print "KARG: ", kwargs
        return (v * w) + len(kwargs)
    
    @classmethod
    @task(returns=int)
    def argkwargTask(cls, *args, **kwargs):
        print "ARG: ", args
        print "KARG: ", kwargs
        return sum(args) + len(kwargs)

    @classmethod
    @task(returns=int)
    def varargkwargTask(cls, v, w , *args, **kwargs):
        print "V: ", v
        print "W: ", w
        print "ARG: ", args
        print "KARG: ", kwargs
        return (v * w) + sum(args) + len(kwargs)

    @classmethod
    @task(returns=int)
    def varargdefaultkwargTask(cls, v, w, s = 2, *args, **kwargs):
        print "V: ", v
        print "W: ", w
        print "S: ", s
        print "ARGS: ", args
        print "KWARG: ", kwargs
        return (v * w) + sum(args) + len(kwargs) + s

    '''
    FUNCTION WITH *ARGS
    '''

    # we have arguments
    def testArgTask1(self):
        pending = self.argTask(1, 2)
        result = compss_wait_on(pending)
        self.assertEqual(result, 3)

    def testArgTask2(self):
        pending = self.argTask(1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    # args is empty
    def testArgTask3(self):
        pending = self.argTask()
        result = compss_wait_on(pending)
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS + *ARGS
    '''

    def testVarArgTask1(self):
        pending = self.varargTask(10, 20, 1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 210)

    def testVarArgTask2(self):
        pending = self.varargTask(4, 50, 5, 4, 3, 2, 1)
        result = compss_wait_on(pending)
        self.assertEqual(result, 215)

    def testVarArgTask3(self):
        pending = self.varargTask(4, 50)
        result = compss_wait_on(pending)
        self.assertEqual(result, 200)

    '''
        FUNCTION WITH **KWARGS
    '''

    def testKwargTask1(self):
        pending = self.kwargTask(hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 1)

    def testKwargTask2(self):
        pending = self.kwargTask(this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 2)

    def testKwargTask3(self):
        pending = self.kwargTask()
        result = compss_wait_on(pending)
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS + **KWARGS
    '''

    def testVarKwargTask1(self):
        pending = self.varkwargTask(1, 2, hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 3)

    def testVarArgKwargTask2(self):
        pending = self.varkwargTask(2, 3, this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 8)

    def testVarArgKwargTask3(self):
        pending = self.varkwargTask(2, 3)
        result = compss_wait_on(pending)
        self.assertEqual(result, 6)

    '''
        FUNCTION WITH *ARGS + **KWARGS
    '''

    def testArgKwargTask1(self):
        pending = self.argkwargTask(1, 2, hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 4)

    def testArgKwargTask2(self):
        pending = self.argkwargTask(1, 2, 3, 4, this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 12)

    def testArgKwargTask3(self):
        pending = self.argkwargTask(1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    def testArgKwargTask4(self):
        pending = self.argkwargTask()
        result = compss_wait_on(pending)
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgKwargTask1(self):
        pending = self.varargkwargTask(1, 2, 3, 4, hello='world')
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)

    def testVarArgKwargTask2(self):
        pending = self.varargkwargTask(1, 2, 3, 4, 5, 6, this='is', a='test')
        result = compss_wait_on(pending)
        self.assertEqual(result, 22)

    '''
        FUNCTION WITH ARGS, DEFAULTED ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgDefaultKwargTask1(self):
        pending = self.varargdefaultkwargTask(1, 1)
        result = compss_wait_on(pending)
        self.assertEqual(result, 3)

    def testVarArgDefaultKwargTask2(self):
        pending = self.varargdefaultkwargTask(1, 2, 3)
        result = compss_wait_on(pending)
        self.assertEqual(result, 5)

    def testVarArgDefaultKwargTask3(self):
        pending = self.varargdefaultkwargTask(1, 2, 3, 4)
        result = compss_wait_on(pending)
        self.assertEqual(result, 9)

    def testVarArgDefaultKwargTask4(self):
        pending = self.varargdefaultkwargTask(1, 2, 3, 4, five=5)
        result = compss_wait_on(pending)
        self.assertEqual(result, 10)
