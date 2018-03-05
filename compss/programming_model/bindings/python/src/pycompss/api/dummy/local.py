#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Dummy API - local
==========================
    Local decorator dummy.
"""


def local(input_function):
    def wrapped_function(*args, **kwargs):
        return input_function(*args, **kwargs)
    return wrapped_function
