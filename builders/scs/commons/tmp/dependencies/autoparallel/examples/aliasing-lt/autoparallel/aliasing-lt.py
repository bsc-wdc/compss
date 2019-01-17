#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from pycompss.api.parallel import parallel
from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on

import numpy as np


############################################
# MATRIX GENERATION
############################################

def initialize_variables(n_size):
    mat = create_matrix(n_size)

    return mat


def create_matrix(n_size):
    mat = []
    for i in range(n_size):
        mb = create_entry(i, n_size)
        mat.append(mb)
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=1)
def create_entry(i, size):
    return np.float64(np.float64(i) / np.float64(size))


############################################
# MAIN FUNCTION
############################################

# @parallel()
@parallel(taskify_loop_level=1)
def test_main(mat, n_size):
    # Debug
    if __debug__:
        mat = compss_wait_on(mat)
        print("Matrix:")
        print(mat)

    # Compute
    index = 4
    for i in range(2, n_size):
        mat[i] = compute_mat(n_size)
        display(mat[index])

    # Debug result
    if __debug__:
        mat = compss_wait_on(mat)

        print("New Matrix:")
        print(mat)


############################################
# MATHEMATICAL FUNCTIONS
############################################

def compute_mat(n_size):
    # import time
    # start = time.time()

    return n_size

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


def display(elem):
    # Display value inside task
    print("GOT: " + str(elem))


############################################
# MAIN
############################################

if __name__ == "__main__":
    # Import libraries
    import time

    # Parse arguments
    import sys

    args = sys.argv[1:]
    NSIZE = int(args[0])

    # Log arguments if required
    if __debug__:
        print("Running test application with:")
        print(" - NSIZE = " + str(NSIZE))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    MAT = initialize_variables(NSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    comp_start_time = time.time()
    test_main(MAT, NSIZE)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = comp_start_time - start_time
    comp_time = end_time - comp_start_time

    print("RESULTS -----------------")
    print("VERSION AUTOPARALLEL")
    print("NSIZE " + str(NSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("FDTD_TIME " + str(comp_time))
    print("-------------------------")
