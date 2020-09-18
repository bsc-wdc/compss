#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench - Test No Return Type-Hinting within Class methods
=====================================================================
"""

# Imports
import unittest

from pycompss.api.api import compss_wait_on
from .myclassTH import myClass


class testNoReturnClassesTH(unittest.TestCase):
    '''
    TASKS DEFINED WITHIN CLASSES
    '''

    def testNoReturn(self):
        obj = myClass()
        o = obj.noReturnNoReturns(1)
        o = compss_wait_on(o)
        self.assertEqual(o, None)

    def testPrimitiveReturn(self):
        obj = myClass()
        o = obj.PrimitiveReturnNoReturns(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 2)

    def testObjectReturn(self):
        obj = myClass()
        v = [2]
        o = obj.ObjectReturnNoReturns(v)
        o = compss_wait_on(o)
        self.assertEqual(o, [3])

    def testMultiPrimitiveReturn(self):
        obj = myClass()
        o, p = obj.MultiPrimitiveReturnNoReturns(1)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, 2)
        self.assertEqual(p, 3)

    def testMultiPrimitiveReturn2(self):
        obj = myClass()
        o = obj.MultiPrimitiveReturnNoReturns(1)
        o = compss_wait_on(o)
        self.assertEqual(o, [2, 3])

    def testMultiObjectReturn(self):
        obj = myClass()
        v = [2, 3, 4]
        w = [4, 5, 6]
        o, p = obj.MultiObjectReturnNoReturns(v, w)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, [3, 3, 4])
        self.assertEqual(p, [4, 5, 6, 3])

    def testMultiObjectReturn2(self):
        obj = myClass()
        v = [2, 3, 4]
        w = [4, 5, 6]
        o = obj.MultiObjectReturnNoReturns(v, w)
        o = compss_wait_on(o)
        self.assertEqual(o, [[3, 3, 4], [4, 5, 6, 3]])

    def testObjectReturn(self):
        obj = myClass()
        v = [2, 3, 5, 7]
        o, p = obj.MultiReturnNoReturns(v)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, [3, 3, 5, 7])
        self.assertEqual(p, 5)

    @unittest.skip('WIP')
    def testObjectReturnClassMethod(self):
        obj = myClass()
        v = [2, 3, 5, 7]
        o, p = obj.MultiReturnNoReturnsClassMethod(v)
        o = compss_wait_on(o)
        p = compss_wait_on(p)
        self.assertEqual(o, [3, 3, 5, 7])
        self.assertEqual(p, 5)
