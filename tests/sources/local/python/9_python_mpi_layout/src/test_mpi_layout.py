import unittest
import os

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier, compss_wait_on, compss_open
from pycompss.api.mpi import mpi
from pycompss.api.constraint import constraint

import time
import pickle

@task(returns=int)
def genData():
    return 1

#@mpi(runner="mpirun", data_layout={block_count: 2}, processes=1)
#@mpi(runner="mpirun", processes=1, scale_by_cu=True) #>2.7
#@mpi(runner="mpirun", computing_nodes=1) 2.6
#@mpi(runner="mpirun", data_layout={block_count: 4}, processes=1, scale_by_cu=True)
#@mpi(runner="mpirun", processes=1, scale_by_cu=True)
@constraint(computing_units=4)
@mpi(runner="mpirun", processes=1, data_layout={block_count: 4, block_length: 1, stride: 1}, scale_by_cu=True)
@task(data=COLLECTION_IN, data2=COLLECTION_IN, returns=4)
def layout_test_with_normal(scalar, data, data2):
    from mpi4py import MPI
    rank = MPI.COMM_WORLD.rank
    print("Data is " + str(data))
    return (data2[rank] * data*rank) + scalar
@constraint(computing_units=4)
@mpi(runner="mpirun", processes=1, data_layout={block_count: 4, block_length: 1, stride: 1}, data2_layout={block_count: 4, block_length: 1, stride: 1}, scale_by_cu=True)
@task(data=COLLECTION_IN, data2=COLLECTION_IN, returns=4)
def two_layouts_test(scalar, data, data2):
    from mpi4py import MPI
    rank = MPI.COMM_WORLD.rank
    print("Data is " + str(data))
    return (data2 * data*rank) + scalar

@constraint(computing_units=4)
@mpi(runner="mpirun", processes=1, data_layout={block_count: 4, block_length: 2, stride: 1}, scale_by_cu=True)
@task(data=COLLECTION_IN, returns=4)
def layout_test_with_all(data):
    from mpi4py import MPI
    rank = MPI.COMM_WORLD.rank
    return data[0]+data[1]+rank

@constraint(computing_units=4)
@mpi(runner="mpirun", processes=1, data_layout={block_count: 4, block_length: 2, stride: 2}, scale_by_cu=True)
@task(data={Type:COLLECTION_IN, Depth: 2}, returns=4)
def layout_test_nested_lists_with_all(data):
    from mpi4py import MPI
    rank = MPI.COMM_WORLD.rank
    print("Data is:" + str(data))
    return (data[0][0]+data[0][1]+data[1][0]+data[1][1])*rank

@constraint(computing_units=4)
@mpi(runner="mpirun", processes=1, data_layout={block_count: 8, block_length: 1, stride: 1}, scale_by_cu=True)
@task(data=COLLECTION_IN, returns=4)
def layout_test_nested_lists(data):
    from mpi4py import MPI
    rank = MPI.COMM_WORLD.rank
    if len(data) == 2:
        return (data[0][0]+data[0][1]+data[1][0]+data[1][1])*rank
    else:
        return data

@constraint(computing_units=4)
@mpi(runner="mpirun", processes=1, data_layout={block_count: 2, block_length: 1, stride: 1}, scale_by_cu=True)
@task(data=COLLECTION_IN, returns=4)
def layout_test_nested_lists_smaller(data):
    from mpi4py import MPI
    rank = MPI.COMM_WORLD.rank
    if len(data) == 2:
        return (data[0]+data[1])*rank
    else:
        return -len(data)

if __name__ == "__main__":
   print("Test single element form previous task")
   data = [] 
   for i in range(4):
       data.append(genData())
   data2 = [10,10,10,10] 
   ret = layout_test_with_normal(2, data, data2)
   ret = compss_wait_on(ret)
   if ret[0]==2 and ret[1]==12 and ret[2]==22 and ret[3]==32:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [2,12,22,32]")
   
   print("Test same with 2 layouts")
   ret = two_layouts_test(2, data, data2)
   ret = compss_wait_on(ret)
   if ret[0]==2 and ret[1]==12 and ret[2]==22 and ret[3]==32:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [2,12,22,32]")
   
   print("Test single element form integer collection")
   data3 = [1,2,3,4]
   ret = layout_test_with_normal(2, data3, data2)
   ret = compss_wait_on(ret)
   if ret[0]==2 and ret[1]==22 and ret[2]==62 and ret[3]==122:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [2,22,62,122]")
   
   print("Test block with len and stride form integer collection")
   data3 = [1,2,3,4,5]
   ret = layout_test_with_all(data3)
   ret = compss_wait_on(ret)
   if ret[0]==3 and ret[1]==6 and ret[2]==9 and ret[3]==12:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [3,6,9,12]") 
   
   print("Test block with nested collection")
   data4 = [[1,1],[1,2],[1,3],[1,4],[1,5],[1,6],[1,7],[1,8]]
   ret = layout_test_nested_lists(data4)
   ret = compss_wait_on(ret)
   if ret[0]==0 and ret[1]==10 and ret[2]==24 and ret[3]==42:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [0,10,24,42]")
   
   print("Test block with len and stride with nested collection")
   ret = layout_test_nested_lists_with_all(data4)
   ret = compss_wait_on(ret)
   if ret[0]==0 and ret[1]==9 and ret[2]==26 and ret[3]==51:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [0,9,26,51]")
   
   print("Test block len and stride with nested collection")
   ret = layout_test_nested_lists_smaller(data4)
   ret = compss_wait_on(ret)
   if ret[0]==0 and ret[1]==3 and ret[2]==0 and ret[3]==0:
       print("Test correct.")
   else:
       raise Exception("Incorrect values " + str(ret) + ". Expecting [0,3,0,0]")
   
