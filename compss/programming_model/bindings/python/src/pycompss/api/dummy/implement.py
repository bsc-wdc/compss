#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Dummy API - Implement
==============================
    This file contains the dummy class implement used as decorator.
"""


class implement(object):
    """
    Dummy implement class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
