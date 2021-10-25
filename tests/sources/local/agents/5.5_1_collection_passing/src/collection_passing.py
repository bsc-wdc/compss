#!/usr/bin/python

# -*- coding: utf-8 -*-



"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.constraint import constraint
from pycompss.api.parameter import *
import sys
import time
from DummyObject import DummyObject
import time


@constraint(operating_system_type="agent_3")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_int_coll(coll):
    print("printIntCollResult:" + str(coll))

@constraint(operating_system_type="agent_2")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_obj_coll(coll):
    print("printObjCollResult:" + str(coll))

@constraint(operating_system_type="agent_3")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_worker_int_coll(coll):
    print("printWorkerIntCollResult:" + str(coll))

@constraint(operating_system_type="agent_2")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_worker_obj_coll(coll):
    print("printWorkerObjCollResult:" + str(coll))

@constraint(operating_system_type="agent_2")
@task(returns={Type: COLLECTION_OUT, Depth: 2})
def create_int_collection():
    return [1,2,3,[41,42,43]]

@constraint(operating_system_type="agent_3")
@task(returns={Type: COLLECTION_OUT, Depth: 2})
def create_object_collection():
    return [DummyObject(1),DummyObject(2),DummyObject(3),[DummyObject(41),DummyObject(42),DummyObject(43)]]

@constraint(operating_system_type="agent_2")
@task(returns={Type: COLLECTION_OUT, Depth: 2})
def create_int_collection_worker():
    return [1,2,3,[41,42,43]]

@constraint(operating_system_type="agent_3")
@task(returns={Type: COLLECTION_OUT, Depth: 2})
def create_object_collection_worker():
    return [DummyObject(1),DummyObject(2),DummyObject(3),[DummyObject(41),DummyObject(42),DummyObject(43)]]

@constraint(operating_system_type="agent_2")
@task(coll={Type: COLLECTION_INOUT, Depth: 2})
def modify_obj_collection(coll):
    coll[2].n = 11

@constraint(operating_system_type="agent_3")
@task(coll={Type: COLLECTION_INOUT, Depth: 2})
def modify_dummy_obj(coll):
    coll[1].n = 9

@constraint(operating_system_type="agent_2")
@task()
def print_elem(elem):
    print("printingElem: " + elem)

@constraint(operating_system_type="agent_3")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_mod_int_coll(coll):
    print("printModifiedIntCollResult:" + str(coll))

@constraint(operating_system_type="agent_2")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_mod_obj_coll(coll):
    print("printModifiedObjCollResult:" + str(coll))

@constraint(operating_system_type="agent_2")
@task(c={Type: COLLECTION_OUT, Depth: 1})
def gen_coll_out(c):
    c[0]=DummyObject(11)
    c[1]=DummyObject(12)

@constraint(operating_system_type="agent_3")
@task(coll={Type: COLLECTION_IN, Depth: 2})
def print_coll_out(coll):
    print("printCollOut:" + str(coll))


@task()
def main():


    ## MASTER -> WORKER ###
    print_int_coll([1,2,3,[41,42,43]]) #agent_3
    print_obj_coll([DummyObject(1),DummyObject(2),DummyObject(3),[DummyObject(41),DummyObject(42),DummyObject(43)]]) #agent_2

    ## WORKER -> WORKER ###
    print_worker_int_coll(compss_wait_on(create_int_collection_worker())) #agent_2 -->  #agent_3
    print_worker_obj_coll(compss_wait_on(create_object_collection_worker())) #agent_3 -->  #agent_2

    ## WORKER -> MASTER ###
    objCollection = create_object_collection() #agent_3
    intCollection = create_int_collection() #agent_2
    objCollection = compss_wait_on(objCollection)
    intCollection = compss_wait_on(intCollection)
    print("createObjCollectionResult:" + str(objCollection))
    print("createIntCollectionResult:" + str(intCollection))

    ### COLLECTION DEPENDENCY ###
    modify_dummy_obj(objCollection) #agent_3
    objCollection = compss_wait_on(objCollection)
    print(" modifiedObjColl:" + str(objCollection))

    print_mod_obj_coll(objCollection) #agent_2

    ### COLLECTION_OUT ###
    coll= [DummyObject(),DummyObject()]
    gen_coll_out(coll)
    coll = compss_wait_on(coll) #agent_2
    print_coll_out(coll) #agent_3

    coll = ["1","2","3","4","5","6"]
    for elem in coll:
        print_elem(elem) #agent_2
    

if __name__ == "__main__":
    main()