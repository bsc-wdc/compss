#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench object
========================
"""

# Imports
from storage.api import StorageObject

class MyClass(StorageObject):
    def __init__(self):
        super(MyClass, self).__init__()
        self.test = "holala"

    def get_test(self):
        return self.test

    def set_test(self, test):
        self.test = test
