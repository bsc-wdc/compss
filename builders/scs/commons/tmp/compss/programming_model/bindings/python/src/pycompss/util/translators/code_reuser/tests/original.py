#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


def unmodified_header():
    print("This code should remain the same")


def test_func():
    print("Hello World")


def unmodified_footer():
    print("This code should remain the same")


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    unittest.main()
