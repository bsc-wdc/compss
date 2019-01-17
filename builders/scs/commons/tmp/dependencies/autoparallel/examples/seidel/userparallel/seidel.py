#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on

import numpy as np


############################################
# MATRIX GENERATION
############################################

def initialize_variables(n_size):
    a = create_matrix(n_size)

    return a


def create_matrix(n_size):
    mat = []
    for i in range(n_size):
        mat.append([])
        for j in range(n_size):
            mb = create_entry(i, j, n_size)
            mat[i].append(mb)

    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=1)
def create_entry(i, j, n_size):
    return np.float64(np.float64(i * (j + 2) + 2) / np.float64(n_size))


############################################
# MAIN FUNCTION
############################################

def seidel(a, n_size, t_size):
    # Debug
    if __debug__:
        a = compss_wait_on(a)

        print("Matrix A:")
        print(a)

    # Compute expected result
    if __debug__:
        import copy
        a_seq = copy.deepcopy(a)
        a_expected = seq_seidel(a_seq, n_size, t_size)

    # Seidel
    for _ in range(t_size):
        for i in range(1, n_size - 1):
            for j in range(1, n_size - 1):
                a[i][j] = compute_distance(a[i - 1][j - 1], a[i - 1][j], a[i - 1][j + 1], a[i][j - 1], a[i][j],
                                           a[i][j + 1], a[i + 1][j - 1], a[i + 1][j], a[i + 1][j + 1])

    # Debug result
    if __debug__:
        a = compss_wait_on(a)

        print("New Matrix A:")
        print(a)

    # Check result
    if __debug__:
        check_result(a, a_expected)


############################################
# MATHEMATICAL FUNCTIONS
############################################

@task(returns=1)
def compute_distance(a_tl, a_tc, a_tr, a_cl, a_cc, a_cr, a_bl, a_bc, a_br):
    # import time
    # start = time.time()

    return np.float64((np.float64(a_tl + a_tc + a_tr + a_cl + a_cc + a_cr + a_bl + a_bc + a_br)) / np.float64(9))

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


############################################
# RESULT CHECK FUNCTIONS
############################################

def seq_seidel(a, n_size, t_size):
    for _ in range(t_size):
        for i in range(1, n_size - 1):
            for j in range(1, n_size - 1):
                a[i][j] = np.float64(np.float64(
                    a[i - 1][j - 1] + a[i - 1][j] + a[i - 1][j + 1] + a[i][j - 1] + a[i][j] + a[i][j + 1] + a[i + 1][
                        j - 1] + a[i + 1][j] + a[i + 1][j + 1]) / np.float64(9))

    return a


def check_result(a, a_expected):
    is_ok = np.allclose(a, a_expected)
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
    TSIZE = int(args[1])

    # Log arguments if required
    if __debug__:
        print("Running seidel application with:")
        print(" - NSIZE = " + str(NSIZE))
        print(" - TSIZE = " + str(TSIZE))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    A = initialize_variables(NSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    seidel_start_time = time.time()
    seidel(A, NSIZE, TSIZE)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = seidel_start_time - start_time
    seidel_time = end_time - seidel_start_time

    print("RESULTS -----------------")
    print("VERSION USERPARALLEL")
    print("NSIZE " + str(NSIZE))
    print("TSIZE " + str(TSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("SEIDEL_TIME " + str(seidel_time))
    print("-------------------------")
