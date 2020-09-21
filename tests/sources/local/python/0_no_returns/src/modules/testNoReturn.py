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


@task()
def noReturnNoReturns(v):
    print("V: ", v)


@task()
def PrimitiveReturnNoReturns(v):
    print("V: ", v)
    return v + 1


@task()
def ObjectReturnNoReturns(v):
    print("V: ", v)
    v[0] += 1
    return v


@task()
def MultiPrimitiveReturnNoReturns(v):
    print("V: ", v)
    return v + 1, v + 2


@task()
def MultiObjectReturnNoReturns(v, w):
    print("V: ", v)
    print("W: ", w)
    v[0] += 1
    w.append(v[0])
    return v, w


@task()
def MultiReturnNoReturns(v):
    print("V: ", v)
    v[0] += 1
    return v, v[1] + 2


class testNoReturn(unittest.TestCase):
    '''
    TASKS
    '''

    def testNoReturn(self):
        o = noReturnNoReturns(1)
        o = compss_wait_on(o)
        self.assertEqual(o, None)

    def testPrimitiveReturn(self):
        o = PrimitiveReturnNoReturns(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 2)

    def testObjectReturn(self):
        v = [2]
        o = ObjectReturnNoReturns(v)
        o = compss_wait_on(o)
        self.assertEqual(o, [3])

    def testMultiPrimitiveReturn(self):
        o, p = MultiPrimitiveReturnNoReturns(1)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, 2)
        self.assertEqual(p, 3)

    def testMultiPrimitiveReturn2(self):
        o = MultiPrimitiveReturnNoReturns(1)
        o = compss_wait_on(o)
        self.assertEqual(o, [2, 3])

    def testMultiObjectReturn(self):
        v = [2, 3, 4]
        w = [4, 5, 6]
        o, p = MultiObjectReturnNoReturns(v, w)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, [3, 3, 4])
        self.assertEqual(p, [4, 5, 6, 3])

    def testMultiObjectReturn2(self):
        v = [2, 3, 4]
        w = [4, 5, 6]
        o = MultiObjectReturnNoReturns(v, w)
        o = compss_wait_on(o)
        self.assertEqual(o, [[3, 3, 4], [4, 5, 6, 3]])

    def testObjectReturn(self):
        v = [2, 3, 5, 7]
        o, p = MultiReturnNoReturns(v)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, [3, 3, 5, 7])
        self.assertEqual(p, 5)
