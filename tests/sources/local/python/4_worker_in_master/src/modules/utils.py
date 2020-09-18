#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Utilities
========================
"""

# IMPORTS
import sys

def verify_line(obtained, expected):
    """
    Compares two strings. If they don't match, the method raises an exception.
    """
    if obtained:
        if expected is None or not obtained == expected:
            sys.stderr.write("Expecting:\n" + expected + "\n and obtained\n" + obtained)
            raise Exception("Unexpected file content.")
    else:
        if expected:
            sys.stderr.write("Expecting:\n" + expected + "\n and obtained\n" + obtained)
            raise Exception("Unexpected file content.")
