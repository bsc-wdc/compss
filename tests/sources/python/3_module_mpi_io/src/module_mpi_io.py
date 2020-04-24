#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import tempfile
import shutil 
import time
import os

from pycompss.api.task import task
from pycompss.api.IO import IO
from pycompss.api.mpi import mpi
from pycompss.api.constraint import constraint


@task(returns=str)
def task1(tmp_dir):
    data = "task1 identifier"
    f = open(os.path.join(tmp_dir, "task1_data"), "w")
    f.write(data)
    f.close()
    
    return data

@task()
def task2(data, tmp_dir):
    #give time to mpi_io_task to check how many files in tmp_dir
    time.sleep(5)
    
    f = open(os.path.join(tmp_dir, "task2_data"), "w")
    f.write(data)
    f.close()

@constraint(computing_units="4")
@mpi(runner="mpirun", processes="1", scale_by_cu=True)
@IO()
@task(returns=int)
def mpi_io_task(data, tmp_dir):
    from mpi4py import MPI

    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    
    print("Launched MPI communicator with {0} MPI processes".format(size))
    
    dir_list = os.listdir("/"+tmp_dir)
    
    return len(dir_list)

def main():
    from pycompss.api.api import compss_barrier, compss_wait_on
    
    tmp_dir = tempfile.mkdtemp(dir="/tmp/")
    
    data = task1(tmp_dir)
    task2(data, tmp_dir)
    dir_len = mpi_io_task(data, tmp_dir)
    dir_len = compss_wait_on(dir_len)
    
    compss_barrier()
    shutil.rmtree(tmp_dir)

    assert len(dir_len) == 4, "MPI IO task launched with incorrect size of MPI communicator. Actual Size: {0} \nExpected Size: {1} ".format(len(dir_len), 4)

    for ret in dir_len:   
        #if ret = 2, this means that io_task() did not overlap with task2()
        assert ret == 1, "MPI IO task did not overlap with the compute task!"

if __name__ == '__main__':
    main()
