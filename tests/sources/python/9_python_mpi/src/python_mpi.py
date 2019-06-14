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
@mpi(runner="mpirun", computing_nodes="1")
@task(returns=list)
def return_rank(seed):
    from mpi4py import MPI

    size = MPI.COMM_WORLD.size
    rank = MPI.COMM_WORLD.rank
    
    print("Launched MPI communicator with {0} MPI processes".format(size))
    
    return rank+seed


def main():
    from pycompss.api.api import compss_barrier

    rank_list = return_rank(10)
    rank_list = compss_wait_on(rank_list)
    
    expected_ranks = [10, 11]
    
    assert rank_list == expected_ranks, "Incorrect returns. Actual Result: {0} \nExpected Result: {1} ".format(rank_list, expected_ranks)

    print("Finished")


if __name__ == '__main__':
    main()
