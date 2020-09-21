#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from myclass import MyClass


def main():
    from pycompss.api.api import compss_wait_on
    o = MyClass(10)
    o.modify(2)
    o = compss_wait_on(o)
    print("o.a: " + str(o.a))
    if 20 == o.a:
        print("- Result value: OK")
    else:
        print("- Result value: ERROR")


if __name__ == '__main__':
    main()
