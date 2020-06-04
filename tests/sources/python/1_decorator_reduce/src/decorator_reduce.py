#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import os

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier, compss_open, compss_wait_on
from pycompss.api.reduce import reduce
from pycompss.api.constraint import constraint

NUM_TASKS = 5


@reduce(chunk_size="2")
@task(returns=1, col=COLLECTION_IN)
def myreduction(col):
    r = 0
    for i in col:
        print("[LOG] Adding: " + str(i))
        r += i
    print("[LOG] Accum: " + str(r))
    return r

@task(returns=1)
def increment(v):
    e = v + 1
    print("[LOG] Returned : " + str(e))
    return e

def dependentParamsReduce():
    a = [x for x in range(1,NUM_TASKS+1)]
    result = []
    for element in a:
        print("[Element] " + str(element))
        # Prior task
        result.append(increment(element))
        print("[Result] " + str(result))
    # Reduction task
    final = myreduction(result)
    final = compss_wait_on(final)
    print("[LOG] Result dependent Parms: " + str(final))
    return final

def nonDependentParamsReduce():
    a = [x for x in range(1,NUM_TASKS+1)]
    result = []
    for element in a:
        print("[Element] " + str(element))
        # Creation of collection element
        result.append(element)
    final = myreduction(result)
    final = compss_wait_on(final)
    print("[LOG] Result non dependent Params: " + str(final))
    return final 

def dependentAndFreeParamsReduce():
    a = [x for x in range(1,NUM_TASKS-1)]
    result = []
    for element in a:
        print("[Element] " + str(element))
        
        # Dependent params
        result.append(increment(element))
        # Non dependent params
        result.append(element)
    final = myreduction(result)
    final = compss_wait_on(final)
    print("[LOG] Result mix of dependent and non dependent Params: " + str(final))
    return final

def postBarrierReduce():
    a = [x for x in range(1,NUM_TASKS+1)]
    result = []
    for element in a:
        print("[Element] " + str(element))
        result.append(increment(element))
    compss_barrier()
    final = myreduction(result)
    final = compss_wait_on(final)
    print("[LOG] Result Post-Barrier: " + str(final))
    return final

def main():
    print("[LOG] Test REDUCE")

    

    print("[LOG] Reduce of parameters dependent on other tasks")
    dependentParamsReduce()

    compss_barrier()

    print("[LOG] Reduce of non-dependent elements")
    nonDependentParamsReduce()

    compss_barrier()

    print("[LOG] Reduce from parameters from a barrier")
    postBarrierReduce()

    compss_barrier()

    print("[LOG] Reduce of mix of dependent and non-dependent elements")
    dependentAndFreeParamsReduce()

    compss_barrier()
    

   


if __name__ == '__main__':
    main()
