#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.constraint import constraint


@constraint(computing_units="1")
@task(returns=1)
def increment(v):
    res = []
    for i in v:
        res.append(i + 1)
    return res


def main():
    from pycompss.api.api import compss_wait_on
    value = [1, 2, 3]
    for i in range(5):
        partialResult = increment(value)
    result = compss_wait_on(partialResult)
    print(result)
    if result[0] == 2 and result[1] == 3 and result[2] == 4:
        print("- Result value: OK")
    else:
        print("- Result value: ERROR")


if __name__ == '__main__':
    main()
