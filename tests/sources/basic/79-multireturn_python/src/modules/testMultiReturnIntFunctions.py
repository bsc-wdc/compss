#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.api import compss_wait_on
from pycompss.api.task import task


@task(returns=2)
def argTask(*args):
    print("ARG: ", args)
    secondReturn = 12345
    if len(args) != 0:
        secondReturn = args[0]
    return sum(args), secondReturn


@task(returns=2)
def varargTask(v, w, *args):
    print("V: ", v)
    print("W: ", w)
    print("ARG: ", args)
    return (v * w) + sum(args), [v, w]


@task(returns=2)
def kwargTask(**kwargs):
    print("KARG: ", kwargs)
    return len(kwargs), sorted(list(kwargs.keys()))


@task(returns=2)
def varkwargTask(v, w, **kwargs):
    print("V: ", v)
    print("W: ", w)
    print("KARG: ", kwargs)
    return (v * w) + len(kwargs), sorted(list(kwargs.values()))


@task(returns=3)
def argkwargTask(*args, **kwargs):
    print("ARG: ", args)
    print("KARG: ", kwargs)
    return sum(args) + len(kwargs), args, kwargs


@task(returns=3)
def varargkwargTask(v, w, *args, **kwargs):
    print("V: ", v)
    print("W: ", w)
    print("ARG: ", args)
    print("KARG: ", kwargs)
    return (v * w) + sum(args) + len(kwargs), args, kwargs


@task(returns=4)
def varargdefaultkwargTask(v, w, s=2, *args, **kwargs):
    print("V: ", v)
    print("W: ", w)
    print("S: ", s)
    print("ARGS: ", args)
    print("KWARG: ", kwargs)
    return (v * w) + sum(args) + len(kwargs) + s, s, args, kwargs


@task(returns=2)
def taskUnrollDict(a, b, **kwargs):
    print("a: ", a)
    print("b: ", b)
    print("kwargs: ", kwargs)
    return a + b, kwargs


@task(returns=2)
def taskUnrollDictWithDefaults(a=1, b=2, **kwargs):
    print("a: ", a)
    print("b: ", b)
    print("kwargs: ", kwargs)
    return a + b, kwargs


class testMultiReturnIntFunctions(unittest.TestCase):
    '''
    FUNCTION WITH *ARGS
    '''

    # we have arguments
    def testArgTask1(self):
        pending1, pending2 = argTask(1, 2)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (3, 1))

    def testArgTask2(self):
        pending1, pending2 = argTask(1, 2, 3, 4)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (10, 1))

    # args is empty
    def testArgTask3(self):
        pending1, pending2 = argTask()
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (0, 12345))

    # args is not empty but args are an unpacked tuple
    def testArgTask4(self):
        my_tup = (1, 2, 3, 4)
        pending1, pending2 = argTask(*my_tup)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (10, 1))

    '''
    FUNCTION WITH ARGS + *ARGS
    '''

    def testVarArgTask1(self):
        pending1, pending2 = varargTask(10, 20, 1, 2, 3, 4)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (210, [10, 20]))

    def testVarArgTask2(self):
        pending1, pending2 = varargTask(4, 50, 5, 4, 3, 2, 1)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (215, [4, 50]))

    def testVarArgTask3(self):
        pending1, pending2 = varargTask(4, 50)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (200, [4, 50]))

    '''
    FUNCTION WITH **KWARGS
    '''

    def testKwargTask1(self):
        pending1, pending2 = kwargTask(hello='world')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (1, ['hello']))

    def testKwargTask2(self):
        pending1, pending2 = kwargTask(this='is', a='test')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (2, sorted(['this', 'a'])))

    def testKwargTask3(self):
        pending1, pending2 = kwargTask()
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (0, []))

    '''
    FUNCTION WITH ARGS + **KWARGS
    '''

    def testVarKwargTask1(self):
        pending1, pending2 = varkwargTask(1, 2, hello='world')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (3, ['world']))

    def testVarArgKwargTask2(self):
        pending1, pending2 = varkwargTask(2, 3, this='is', a='test')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (8, sorted(['is', 'test'])))

    def testVarArgKwargTask3(self):
        pending1, pending2 = varkwargTask(2, 3)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (6, []))

    '''
    FUNCTION WITH *ARGS + **KWARGS
    '''

    def testArgKwargTask1(self):
        pending1, pending2, pending3 = argkwargTask(1, 2, hello='world')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (4, (1, 2), {'hello': 'world'}))

    def testArgKwargTask2(self):
        pending1, pending2, pending3 = argkwargTask(1, 2, 3, 4, this='is', a='test')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (12, (1, 2, 3, 4), {'this': 'is', 'a': 'test'}))

    def testArgKwargTask3(self):
        pending1, pending2, pending3 = argkwargTask(1, 2, 3, 4)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (10, (1, 2, 3, 4), {}))

    def testArgKwargTask4(self):
        pending1, pending2, pending3 = argkwargTask()
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (0, (), {}))

    '''
    FUNCTION WITH ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgKwargTask1(self):
        pending1, pending2, pending3 = varargkwargTask(1, 2, 3, 4, hello='world')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (10, (3, 4), {'hello': 'world'}))

    def testVarArgKwargTask2(self):
        pending1, pending2, pending3 = varargkwargTask(1, 2, 3, 4, 5, 6, this='is', a='test')
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (22, (3, 4, 5, 6), {'this': 'is', 'a': 'test'}))

    '''
    FUNCTION WITH ARGS, DEFAULTED ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgDefaultKwargTask1(self):
        pending1, pending2, pending3, pending4 = varargdefaultkwargTask(1, 1)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        result4 = compss_wait_on(pending4)
        self.assertEqual((result1, result2, result3, result4), (3, 2, (), {}))

    def testVarArgDefaultKwargTask2(self):
        pending1, pending2, pending3, pending4 = varargdefaultkwargTask(1, 2, 3)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        result4 = compss_wait_on(pending4)
        self.assertEqual((result1, result2, result3, result4), (5, 3, (), {}))

    def testVarArgDefaultKwargTask3(self):
        pending1, pending2, pending3, pending4 = varargdefaultkwargTask(1, 2, 3, 4)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        result4 = compss_wait_on(pending4)
        self.assertEqual((result1, result2, result3, result4), (9, 3, (4,), {}))

    def testVarArgDefaultKwargTask4(self):
        pending1, pending2, pending3, pending4 = varargdefaultkwargTask(1, 2, 3, 4, five=5)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        result4 = compss_wait_on(pending4)
        self.assertEqual((result1, result2, result3, result4), (10, 3, (4,), {'five': 5}))

    '''
    FUNCTION WITH **KWARGS AND DICT UNROLLING
    '''

    def testKwargsDictUnrolling(self):
        z = {'a': 10, 'b': 20, 'c': 30}
        pending1, pending2 = taskUnrollDict(**z)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (30, {'c': 30}))

    def testKwargsDictUnrollingControl(self):
        pending1, pending2 = taskUnrollDict(10, 20, c=30)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (30, {'c': 30}))

    def testKwargsDictUnrollingDefaults(self):
        z = {'a': 10, 'b': 20, 'c': 30}
        pending1, pending2 = taskUnrollDictWithDefaults(**z)
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (30, {'c': 30}))

    def testKwargsDictUnrollingDefaultsControl(self):
        pending1, pending2 = taskUnrollDictWithDefaults()
        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (3, {}))
