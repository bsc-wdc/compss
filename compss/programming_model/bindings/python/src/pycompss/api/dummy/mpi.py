#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Dummy API - MPI
========================
    This file contains the dummy class MPI used as decorator.
"""


class mpi(object):
    """
    Dummy MPI class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
