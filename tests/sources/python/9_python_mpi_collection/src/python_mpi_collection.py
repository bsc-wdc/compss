#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.mpi import mpi
from pycompss.api.constraint import constraint
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *


@constraint(computing_units="2")
@mpi(runner="mpirun", processes="2", scale_by_cu=True)
@task(returns=4)
def init(seed):
    from mpi4py import MPI

    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank

    print("Launched MPI task init with {0} MPI processes".format(size))

    return rank+seed


@constraint(computing_units="2")
@mpi(runner="mpirun", processes="2", scale_by_cu=True)
@task(input_data=COLLECTION_IN, returns=4)
def scale(input_data, i):
    from mpi4py import MPI

    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    a = input_data[rank]*i
    print("Launched MPI scale with {0} MPI processes".format(size))
    
    return a

@constraint(computing_units="2")
@mpi(runner="mpirun", processes="2", scale_by_cu=True)
@task(input_data=COLLECTION_IN, returns=4)
def increment(input_data):
    from mpi4py import MPI

    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    a = input_data[rank]+1
    print("Launched MPI process with {0} MPI processes".format(size))

    return a


@constraint(computing_units="2")
@mpi(runner="mpirun", processes="2", scale_by_cu=True)
@task(input_data={Type:COLLECTION_IN, Depth:2}, returns=4)
def merge(input_data):
    from mpi4py import MPI
    data_size = len(input_data)
    
    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    batch = int(data_size/size)
    a=0
    for data in input_data[rank]:
        a=a+data
    print("Launched MPI merge with {0} MPI processes".format(size))
    
    return a

def main():

    input_data = init(0)
    partial_res=[]
    for i in [1,10,20,30]:
        p_data = scale(input_data, i)
        for j in range(2): 
            p_data = increment(p_data)
        partial_res.append(p_data)
    results= merge(partial_res)
    results = compss_wait_on(results)
    print("Results: " + str(results))
    if results[0] != 14 or results[1] != 68 or results[2] != 128 or results[3] != 188 :
        raise Exception ("Error results " + str(results) + " != [14, 68, 128, 188]")
    print ("Finished without error")


if __name__ == '__main__':
    main()
