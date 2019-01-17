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

def initialize_variables(n_size, m_size):
    mat = create_matrix(n_size, m_size)

    return mat


def create_matrix(n_size, m_size):
    mat = []
    for i in range(n_size):
        row = []
        for j in range(m_size):
            mb = create_entry(i + j, n_size + m_size)
            row.append(mb)
        mat.append(row)
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=1)
def create_entry(index, n_size):
    return np.float64(np.float64(index) / np.float64(n_size))


############################################
# MAIN FUNCTION
############################################

# @parallel()
@parallel(pluto_extra_flags=["--tile"], taskify_loop_level=2)
def ep(mat, n_size, m_size, coef1, coef2):
    # Debug
    if __debug__:
        mat = compss_wait_on(mat)
        print("Matrix:")
        print(mat)

    # Compute expected result
    if __debug__:
        import copy
        mat_seq = copy.deepcopy(mat)
        mat_expected = seq_ep(mat_seq, n_size, m_size, coef1, coef2)

    # EP
    for i in range(n_size):
        for j in range(m_size):
            mat[i][j] = compute(mat[i][j], coef1, coef2)

    # Debug result
    if __debug__:
        mat = compss_wait_on(mat)

        print("New Matrix:")
        print(mat)

    # Check result
    if __debug__:
        check_result(mat, mat_expected)


def compute(elem, coef1, coef2):
    return coef1 * elem + coef2


############################################
# RESULT CHECK FUNCTIONS
############################################

def seq_ep(mat, n_size, m_size, coef1, coef2):
    for i in range(n_size):
        for j in range(m_size):
            mat[i][j] = compute(mat[i][j], coef1, coef2)

    return mat


def check_result(result, result_expected):
    is_ok = np.allclose(result, result_expected)
    print("Result check status: " + str(is_ok))

    if not is_ok:
        raise Exception("Result does not match expected result")


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
    MSIZE = int(args[1])
    COEF1 = np.float64(0.5)
    COEF2 = np.float64(0.7)

    # Log arguments if required
    if __debug__:
        print("Running ep application with:")
        print(" - NSIZE = " + str(NSIZE))
        print(" - MSIZE = " + str(MSIZE))
        print(" - COEF1 = " + str(COEF1))
        print(" - COEF2 = " + str(COEF2))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    MAT = initialize_variables(NSIZE, MSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    ep_start_time = time.time()
    ep(MAT, NSIZE, MSIZE, COEF1, COEF2)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = ep_start_time - start_time
    fdtd_time = end_time - ep_start_time

    print("RESULTS -----------------")
    print("VERSION AUTOPARALLEL")
    print("NSIZE " + str(NSIZE))
    print("MSIZE " + str(MSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("EP_TIME " + str(fdtd_time))
    print("-------------------------")
