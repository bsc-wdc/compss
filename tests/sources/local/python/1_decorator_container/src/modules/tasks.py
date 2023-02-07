
# For better print formatting
from __future__ import print_function

from pycompss.api.mpi import mpi
# PyCOMPSs imports
from pycompss.api.task import task
from pycompss.api.binary import binary
from pycompss.api.container import container
from pycompss.api.software import software
from pycompss.api.api import compss_barrier, compss_wait_on
from pycompss.api.parameter import *

# Imports
import unittest
import os



#
# Binary Tasks definition
#
@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls", args="-l")
@task()
def task_binary_empty():
    pass


@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls")
@task(returns=1)
def task_binary_ev():
    pass


@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls",
        working_dir="${TEST_WORKING_DIR}")
@task()
def task_binary_wd():
    pass


@container(engine="DOCKER",
           image="ubuntu")
@binary(binary="ls",
        working_dir="${TEST_WORKING_DIR}")
@task(stdout={Type: FILE_OUT, StdIOStream: STDOUT}, stderr={Type: FILE_OUT, StdIOStream: STDERR})
def task_binary_std(stdout, stderr):
    pass

@container(engine="DOCKER",
           image="ubuntu", options="-e HOLA=hola")
@binary(binary="env")
@task()
def task_binary_options():
    pass

#
# Python Tasks definition
#
@container(engine="DOCKER",
           image="compss/compss",
           options="-e HOLA=hola")
@task()
def task_python_empty():
    print("Hello from Task Python EMPTY")


@container(engine="DOCKER",
           image="compss/compss")
@task(num=IN, in_str=IN, fin=FILE_IN)
def task_python_args(num, in_str, fin):
    print("Hello from Task Python ARGS")
    print("- Arg 1: num -- " + str(num))
    print("- Arg 1: str -- " + str(in_str))
    print("- Arg 1: fin -- " + str(fin))
    with open(fin, 'r') as f:
        print(f.read())


@container(engine="DOCKER",
           image="compss/compss")
@task(returns=1)
def task_python_return_int():
    print("Hello from Task Python RETURN")
    return 3


@container(engine="DOCKER",
           image="compss/compss")
@task(returns=1, num=IN, in_str=IN, fin=FILE_IN)
def task_python_return_str(num, in_str, fin):
    print("Hello from Task Python RETURN")
    print("- Arg 1: num -- " + str(num))
    print("- Arg 1: str -- " + str(in_str))
    print("- Arg 1: fin -- " + str(fin))
    return "Hello"


@container(engine="DOCKER",
           image="compss/compss")
@task(finout=FILE_INOUT)
def task_python_inout(finout):
    print("Hello from Task Python ARGS")

    # Read
    print("- Arg 1: num -- " + str(finout))
    with open(finout, 'r') as f:
        print(f.read())

    # Write
    with open(finout, 'a') as f:
        f.write("Hello from task!\n")


@software(config_file=os.getcwd() + "/src/config/container_basic.json")
@task()
def task_container_basic():
    pass


@software(config_file=os.getcwd() + "/src/config/container_pycompss.json")
def task_python_return_int_soft():
    print("Hello from Task Python RETURN")
    return 3


@container(engine="docker", image="ubuntu")
@mpi(binary="echo", runner="mpirun", args="testing+container+with+mpi")
@task(returns=1)
def task_container_mpi(a):
    pass
