#!/usr/bin/python

# -*- coding: utf-8 -*-

from DummyObject import DummyObject
import time

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.parameter import *
import sys
import time


@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_int_coll(coll):
    print("printIntCollResult:" + str(coll))

@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_obj_coll(coll):
    print("printObjCollResult:" + str(coll))

@task(returns={Type: COLLECTION_OUT, Depth: 2})
def create_int_collection():
    print("executing create_int_collection")
    return [1,2,3,[41,42,43]]

@task(returns={Type: COLLECTION_OUT, Depth: 2})
def create_object_collection():
    print("executing create_object_collection")
    return [DummyObject(1),DummyObject(2),DummyObject(3),[DummyObject(41),DummyObject(42),DummyObject(43)]]


@task()
def main():
    time.sleep(6)
    print("executing main")
    time.sleep(6)
    ### MASTER -> WORKER ###
    print("createObjCollectionResult:" + str(compss_wait_on(create_object_collection())))
    print("createIntCollectionResult:" + str(compss_wait_on(create_int_collection())))
    print_int_coll([1,2,3,[41,42,43]])
    print_obj_coll([DummyObject(1),DummyObject(2),DummyObject(3),[DummyObject(41),DummyObject(42),DummyObject(43)]])


if __name__ == "__main__":
    main()