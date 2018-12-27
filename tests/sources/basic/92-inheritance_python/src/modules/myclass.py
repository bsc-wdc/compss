#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task


class myClass(object):

    def __init__(self):
        self.value = 1234

    @task(returns = int)
    def increment_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return self.value + v

    @task()
    def increment_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = self.value + v

    def get_value(self):
        return self.value


class inheritedClass(myClass):
    pass

class inheritedClassWithOverride(myClass):
    @task(returns = int)
    def increment_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return 2 * (self.value + v)

    @task()
    def increment_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = 2 * (self.value + v)

class inheritedClassExtended(myClass):
    @task(returns = int)
    def multiplier_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return self.value * v

    @task()
    def multiplier_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = self.value * v

class inheritedClassMultilevelOverridedExtended(inheritedClassWithOverride):
    @task(returns = int)
    def divider_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return self.value / v

    @task()
    def divider_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = self.value / v
