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


@task(returns=str)
def task1(tmp_dir):
    data = "task1 identifier"
    f = open(os.path.join(tmp_dir, "task1_data"), "w")
    f.write(data)
    f.close()
    
    return data

@task()
def task2(data, tmp_dir):
    #give time to io_task to check how many files in tmp_dir
    time.sleep(5)
    
    f = open(os.path.join(tmp_dir, "task2_data"), "w")
    f.write(data)
    f.close()

@IO()
@task(returns=int)
def io_task(data, tmp_dir):
    dir_list = os.listdir("/"+tmp_dir)
    return len(dir_list)

def main():
    from pycompss.api.api import compss_barrier, compss_wait_on
    
    tmp_dir = tempfile.mkdtemp(dir="/tmp/")
    
    data = task1(tmp_dir)
    task2(data, tmp_dir)
    dir_len = io_task(data, tmp_dir)
    dir_len = compss_wait_on(dir_len)
    
    compss_barrier()
    shutil.rmtree(tmp_dir)
   
    #if dir_len = 2, this means that io_task() did not overlap with task2()
    assert dir_len == 1, "IO task did not overlap with the compute task!"

if __name__ == '__main__':
    main()
