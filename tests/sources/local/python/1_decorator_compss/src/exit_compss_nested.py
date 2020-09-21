#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Exit
=======================
"""

def test_exit(error_code):
    exit(error_code)  # Intentionally exit code


if __name__ == '__main__':
    import sys
    error_code = int(sys.argv[1])
    test_exit(error_code)
