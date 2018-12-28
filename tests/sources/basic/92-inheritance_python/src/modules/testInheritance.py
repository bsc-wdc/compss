#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Inheritance Testbench
==============================
"""

# Imports
import unittest

from pycompss.api.api import compss_wait_on

class testInheritance(unittest.TestCase):

    '''
    INHERITANCE WITHIN THE SAME FILE
    '''

    def test_simple_inheritance_non_modifier(self):
        from .myclass import inheritedClass
        obj = inheritedClass()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235)

    def test_simple_inheritance_modifier(self):
        from .myclass import inheritedClass
        obj = inheritedClass()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235)

    def test_inheritance_overriding_task_non_modifier(self):
        from .myclass import inheritedClassWithOverride
        obj = inheritedClassWithOverride()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235 * 2)

    def test_inheritance_overriding_task_modifier(self):
        from .myclass import inheritedClassWithOverride
        obj = inheritedClassWithOverride()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235 * 2)

    def test_inheritance_extending_task_non_modifier(self):
        from .myclass import inheritedClassExtended
        obj = inheritedClassExtended()
        o = obj.multiplier_non_modifier(4)
        o = compss_wait_on(o)
        self.assertEqual(o, 1234 * 4)

    def test_inheritance_extending_task_modifier(self):
        from .myclass import inheritedClassExtended
        obj = inheritedClassExtended()
        obj.multiplier_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1234 * 4)

    def test_inheritance_multilevel_non_modifier(self):
        from .myclass import inheritedClassMultilevelOverridedExtended
        obj = inheritedClassMultilevelOverridedExtended()
        o = obj.increment_non_modifier(4)
        o = compss_wait_on(o)
        p = obj.divider_non_modifier(4)
        p = compss_wait_on(p)
        self.assertEqual(o, 2 * (1234 + 4))
        self.assertEqual(p, 1234 / 4)

    def test_inheritance_multilevel_modifier(self):
        from .myclass import inheritedClassMultilevelOverridedExtended
        obj = inheritedClassMultilevelOverridedExtended()
        obj.increment_modifier(4)
        obj.divider_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 2 * (1234 + 4) / 4)

    '''
    INHERITANCE FROM A DIFFERENT FILE WITH SAME CLASS NAME
    '''

    def test_simple_inheritance_other_module_same_classname_non_modifier(self):
        from .otherclasses_same_name import inheritedClass
        obj = inheritedClass()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235)

    def test_simple_inheritance_other_module_same_classname_modifier(self):
        from .otherclasses_same_name import inheritedClass
        obj = inheritedClass()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235)

    def test_inheritance_other_module_same_classname_overriding_task_non_modifier(self):
        from .otherclasses_same_name import inheritedClassWithOverride
        obj = inheritedClassWithOverride()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235 * 2)

    def test_inheritance_other_module_same_classname_overriding_task_modifier(self):
        from .otherclasses_same_name import inheritedClassWithOverride
        obj = inheritedClassWithOverride()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235 * 2)

    def test_inheritance_other_module_same_classname_extending_task_non_modifier(self):
        from .otherclasses_same_name import inheritedClassExtended
        obj = inheritedClassExtended()
        o = obj.multiplier_non_modifier(4)
        o = compss_wait_on(o)
        self.assertEqual(o, 1234 * 4)

    def test_inheritance_other_module_same_classname_extending_task_modifier(self):
        from .otherclasses_same_name import inheritedClassExtended
        obj = inheritedClassExtended()
        obj.multiplier_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1234 * 4)

    def test_inheritance_other_module_same_classname_multilevel_non_modifier(self):
        from .otherclasses_same_name import inheritedClassMultilevelOverridedExtended
        obj = inheritedClassMultilevelOverridedExtended()
        o = obj.increment_non_modifier(4)
        o = compss_wait_on(o)
        p = obj.divider_non_modifier(4)
        p = compss_wait_on(p)
        self.assertEqual(o, 2 * (1234 + 4))
        self.assertEqual(p, 1234 / 4)

    def test_inheritance_other_module_same_classname_multilevel_modifier(self):
        from .otherclasses_same_name import inheritedClassMultilevelOverridedExtended
        obj = inheritedClassMultilevelOverridedExtended()
        obj.increment_modifier(4)
        obj.divider_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 2 * (1234 + 4) / 4)

    '''
    INHERITANCE FROM A DIFFERENT FILE WITH DIFFERENT CLASS NAME
    '''

    def test_simple_inheritance_other_module_different_classname_non_modifier(self):
        from .otherclasses_diff_name import inheritedClass2
        obj = inheritedClass2()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235)

    def test_simple_inheritance_other_module_different_classname_modifier(self):
        from .otherclasses_diff_name import inheritedClass2
        obj = inheritedClass2()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235)

    def test_inheritance_other_module_different_classname_overriding_task_non_modifier(self):
        from .otherclasses_diff_name import inheritedClassWithOverride2
        obj = inheritedClassWithOverride2()
        o = obj.increment_non_modifier(1)
        o = compss_wait_on(o)
        self.assertEqual(o, 1235 * 2)

    def test_inheritance_other_module_different_classname_overriding_task_modifier(self):
        from .otherclasses_diff_name import inheritedClassWithOverride2
        obj = inheritedClassWithOverride2()
        obj.increment_modifier(1)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1235 * 2)

    def test_inheritance_other_module_different_classname_extending_task_non_modifier(self):
        from .otherclasses_diff_name import inheritedClassExtended2
        obj = inheritedClassExtended2()
        o = obj.multiplier_non_modifier(4)
        o = compss_wait_on(o)
        self.assertEqual(o, 1234 * 4)

    def test_inheritance_other_module_different_classname_extending_task_modifier(self):
        from .otherclasses_diff_name import inheritedClassExtended2
        obj = inheritedClassExtended2()
        obj.multiplier_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 1234 * 4)

    def test_inheritance_other_module_different_classname_multilevel_non_modifier(self):
        from .otherclasses_diff_name import inheritedClassMultilevelOverridedExtended2
        obj = inheritedClassMultilevelOverridedExtended2()
        o = obj.increment_non_modifier(4)
        o = compss_wait_on(o)
        p = obj.divider_non_modifier(4)
        p = compss_wait_on(p)
        self.assertEqual(o, 2 * (1234 + 4))
        self.assertEqual(p, 1234 / 4)

    def test_inheritance_other_module_different_classname_multilevel_modifier(self):
        from .otherclasses_diff_name import inheritedClassMultilevelOverridedExtended2
        obj = inheritedClassMultilevelOverridedExtended2()
        obj.increment_modifier(4)
        obj.divider_modifier(4)
        obj = compss_wait_on(obj)
        self.assertEqual(obj.get_value(), 2 * (1234 + 4) / 4)
