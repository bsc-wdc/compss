#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench object
========================
"""


class MyClass(object):
    def __init__(self):
        self.test = "holala"

    def get_test(self):
        return self.test

    def set_test(self, test):
        self.test = test

