#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.implement import implement


@implement(source_class="module_7_implements", method="addtwovectors")
@constraint(computing_units="4")
@task(returns=list)
def myfunctionWithNumpy(list1, list2):
    print("myfunctionWithNumpy")
    assert (len(list1) == len(list2))
    import numpy as np
    x = np.array(list1)
    y = np.array(list2)
    z = x + y
    return z.tolist()


@constraint(computing_units="1")
@task(returns=list)
def addtwovectors(list1, list2):
    print("addtwovectors")
    assert (len(list1) == len(list2))
    for i in range(len(list1)):
        list1[i] += list2[i]
    return list1


def main():
    from pycompss.api.api import compss_wait_on
    a = [1 for i in range(10)]
    b = [2 for i in range(10)]
    c = addtwovectors(a, b)
    c = compss_wait_on(c)

    assert len(c) == 10, "[ERROR] Return length error."
    for i in c:
        assert i == 3, "[ERROR] Wrong value"
    print("Finished OK")


if __name__ == '__main__':
    main()
