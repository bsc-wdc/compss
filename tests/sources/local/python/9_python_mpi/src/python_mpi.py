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

@constraint(computing_units="2")
@mpi(runner="mpirun", processes="1", scale_by_cu=True )
@task(returns=list)
def return_rank(seed):
    from mpi4py import MPI

    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    
    print("Launched MPI communicator with {0} MPI processes".format(size))
    
    return rank+seed

@constraint(computing_units="2")
@mpi(runner="mpirun", processes="2", processes_per_node="2")
@task(returns=list)
def return_rank_omp(seed):
    from mpi4py import MPI
    import os
    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    omp = int(os.environ['OMP_NUM_THREADS'])
    
    print("Launched MPI communicator with {0} MPI processes and {1} OpenMP processes".format(size, omp))

    return rank+seed+omp


def main():
    from pycompss.api.api import compss_barrier

    rank_list = return_rank(10)
    rank_list = compss_wait_on(rank_list)
    
    expected_ranks = [10, 11]
    
    assert rank_list == expected_ranks, "Incorrect returns. Actual Result: {0} \nExpected Result: {1} ".format(rank_list, expected_ranks)
    rank_list_2 = return_rank_omp(10)
    rank_list_2 = compss_wait_on(rank_list_2)
    expected_ranks_2 = [12, 13]
    assert rank_list_2 == expected_ranks_2, "Incorrect returns. Actual Result: {0} \nExpected Result: {1} ".format(rank_list_2, expected_ranks_2)
    print("Finished")


if __name__ == '__main__':
    main()
