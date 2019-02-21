#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task


class MyClass(object):

    def __init__(self, a):
        self.a = a

    @task()
    def modify(self, x):
        self.a = self.a * x

    @task()
    def modify2(self, x):
        self.a = self.a - x
