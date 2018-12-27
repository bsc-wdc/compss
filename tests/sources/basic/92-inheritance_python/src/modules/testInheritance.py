#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Inheritance Testbench
==============================
"""

# Imports
import unittest

from pycompss.api.api import compss_wait_on
# from .myclass import myClass
from .myclass import inheritedClass
from .myclass import inheritedClassWithOverride
from .myclass import inheritedClassExtended
from .myclass import inheritedClassMultilevelOverridedExtended


class testInheritance(unittest.TestCase):

    def testSimpleInheritance_non_modifier(self):
        obj = inheritedClass()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235)

    def testSimpleInheritance_modifier(self):
        obj = inheritedClass()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235)

    def testInheritance_with_overriding_non_modifier(self):
        obj = inheritedClassWithOverride()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235 * 2)

    def testInheritance_with_overriding_modifier(self):
        obj = inheritedClassWithOverride()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235 * 2)

    def testInheritance_extended_non_modifier(self):
        obj = inheritedClassExtended()
        o = obj.multiplier_non_modifier(4)
        o = compss_wait_on(o)
        self.assertEqual(o, 1234 * 4)

    def testInheritance_extended_modifier(self):
        obj = inheritedClassExtended()
        obj.multiplier_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1234 * 4)

    @unittest.skip("not supported yet - bug")
    def testInheritance_multilevel_non_modifier(self):
        obj = inheritedClassMultilevelOverridedExtended()
        o = obj.increment_non_modifier(4)
        o = compss_wait_on(o)
        p = obj.divider_non_modifier(4)
        p = compss_wait_on(p)
        self.assertEqual(o, 2 * (1234 + 4))
        self.assertEqual(p, 1234 / 4)

    @unittest.skip("not supported yet - bug")
    def testInheritance_multilevel_modifier(self):
        obj = inheritedClassMultilevelOverridedExtended()
        obj.increment_modifier(4)
        obj.divider_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 2 * (1234 + 4) / 4)
