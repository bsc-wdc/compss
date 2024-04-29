#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task


@task(returns=int)
def increment(v):
    import bad  # Do not remove
    return v + 1


def main():
    from pycompss.api.api import compss_wait_on
    value = 0
    value = increment(value)
    result = compss_wait_on(value)

    if result == None:
        print("- Result value: OK")
    else:
        print("- Result value: ERROR")
        print("- This error is a root error. Please fix error at test 19.")


if __name__ == '__main__':
    main()
