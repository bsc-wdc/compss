#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench object
========================
"""


class wrapper(object):
    def __init__(self, content):
        self.content = content

    def __repr__(self):
        return "Wrapper(" + str(self.content) + ")"
