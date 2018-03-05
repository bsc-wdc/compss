#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Dummy API - OmpSs
==========================
    This file contains the dummy class OmpSs used as decorator.
"""


class ompss(object):
    """
    Dummy constraint class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
