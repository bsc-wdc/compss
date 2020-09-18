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
    return v + 1


def main():
    from pycompss.api.api import compss_wait_on
    raise Exception('GENERAL EXCEPTION RAISED - HAPPENED BEFORE SUBMITTING TASKS AT MASTER.')
    values = [0, 100, 200, 300]
    for i in range(4):
        for j in range(10):
            values[i] = increment(values[i])
    result = compss_wait_on(values)

    if result[0] == 10 and result[1] == 110 and result[2] == 210 and result[3] == 310:
        print("- Result value: OK")
    else:
        print("- Result value: ERROR")
        print("- This error is a root error. Please fix error at test 19.")


if __name__ == '__main__':
    main()
