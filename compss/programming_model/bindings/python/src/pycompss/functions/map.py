#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Functions: Map
=======================
    This file defines the common map functions.
"""
import sys

def map(*args):
    """
    Apply function to every item of iterable and return a list of the results.
    If additional iterable arguments are passed, function must take that many
    arguments and is applied to the items from all iterables in parallel.
    If one iterable is shorter than another it is assumed to be extended with
    None items. If function is None, the identity function is assumed; if
    there are multiple arguments, map() returns a list consisting of tuples
    containing the corresponding items from all iterables (a kind of transpose
    operation). The iterable arguments may be a sequence or any iterable
    object. The result is always a list.
    :param function: function to apply to data
    :param data: List of items to be reduced
    :return: list result
    """
    try:
        if sys.version_info >= (3, 0):
            import builtins
            return list(builtins.map(*args))
        else:
            import __builtin__
            return __builtin__.map(*args)
    except Exception as e:
        raise e
