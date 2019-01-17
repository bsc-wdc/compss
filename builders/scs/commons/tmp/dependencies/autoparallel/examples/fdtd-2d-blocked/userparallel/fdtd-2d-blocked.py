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

def initialize_variables(n_size, b_size):
    ex = create_matrix(n_size, b_size)
    ey = create_matrix(n_size, b_size)
    hz = create_matrix(n_size, b_size)

    return ex, ey, hz


def create_matrix(n_size, b_size):
    mat = []
    for _ in range(n_size):
        mb = create_block(b_size)
        mat.append(mb)
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=1)
def create_block(b_size):
    import os
    np.random.seed(ord(os.urandom(1)))
    block = np.array(np.random.random(b_size), dtype=np.float64, copy=False)
    return block


############################################
# MAIN FUNCTION
############################################

def fdtd_blocked(ex, ey, hz, n_size, b_size, t_size, coef1, coef2):
    # Debug
    if __debug__:
        ex = compss_wait_on(ex)
        ey = compss_wait_on(ey)
        hz = compss_wait_on(hz)
        print("Matrix Ex:")
        print(ex)
        print("Matrix Ey:")
        print(ey)
        print("Matrix Hz:")
        print(hz)

    # Compute expected result
    if __debug__:
        import copy
        ex_seq = copy.deepcopy(ex)
        ey_seq = copy.deepcopy(ey)
        hz_seq = copy.deepcopy(hz)
        hz_expected = seq_fdtd_2d(ex_seq, ey_seq, hz_seq, n_size, b_size, t_size, coef1, coef2)

    # FDTD
    for t in range(t_size):
        ey[0] = copy_reference(t, b_size)
        for i in range(1, n_size):
            ey[i] = compute_e(ey[i], coef1, hz[i], hz[i - 1], 0, b_size)
        for i in range(n_size):
            ex[i] = compute_e(ex[i], coef1, hz[i], hz[i], 1, b_size)
        for i in range(n_size - 1):
            hz[i] = compute_h(hz[i], coef2, ex[i], ey[i + 1], ey[i], b_size)

    # Debug result
    if __debug__:
        hz = compss_wait_on(hz)

        print("New Matrix Hz:")
        print(hz)

    # Check result
    if __debug__:
        check_result(hz, hz_expected)


############################################
# MATHEMATICAL FUNCTIONS
############################################

@task(returns=1)
def compute_e(e, coef1, h2, h1, init, b_size):
    # import time
    # start = time.time()

    for j in range(init, b_size):
        e[j] -= coef1 * (h2[j] - h1[j - init])

    return e

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


@task(returns=1)
def compute_h(h, coef2, ex, ey2, ey1, b_size):
    # import time
    # start = time.time()

    for j in range(b_size - 1):
        h[j] -= coef2 * (ex[j + 1] - ex[j] + ey2[j] - ey1[j])

    return h

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


@task(returns=1)
def copy_reference(elem, b_size):
    block = np.full(b_size, elem, dtype=np.float64)
    return block


############################################
# RESULT CHECK FUNCTIONS
############################################

def seq_fdtd_2d(ex, ey, hz, n_size, b_size, t_size, coef1, coef2):
    for t in range(t_size):
        for j in range(b_size):
            ey[0][j] = t
        for i in range(1, n_size):
            for j in range(b_size):
                ey[i][j] -= coef1 * (hz[i][j] - hz[i - 1][j])
        for i in range(n_size):
            for j in range(1, b_size):
                ex[i][j] -= coef1 * (hz[i][j] - hz[i][j - 1])
        for i in range(n_size - 1):
            for j in range(b_size - 1):
                hz[i][j] -= coef2 * (ex[i][j + 1] - ex[i][j] + ey[i + 1][j] - ey[i][j])

    return hz


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
    BSIZE = int(args[1])
    TSIZE = int(args[2])
    COEF1 = np.float64(0.5)
    COEF2 = np.float64(0.7)

    # Log arguments if required
    if __debug__:
        print("Running fdtd-2d application with:")
        print(" - NSIZE = " + str(NSIZE))
        print(" - BSIZE = " + str(BSIZE))
        print(" - TSIZE = " + str(TSIZE))
        print(" - COEF1 = " + str(COEF1))
        print(" - COEF2 = " + str(COEF2))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    EX, EY, HZ = initialize_variables(NSIZE, BSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    fdtd_start_time = time.time()
    fdtd_blocked(EX, EY, HZ, NSIZE, BSIZE, TSIZE, COEF1, COEF2)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = fdtd_start_time - start_time
    fdtd_time = end_time - fdtd_start_time

    print("RESULTS -----------------")
    print("VERSION USERPARALLEL")
    print("NSIZE " + str(NSIZE))
    print("BSIZE " + str(BSIZE))
    print("TSIZE " + str(TSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("FDTD_TIME " + str(fdtd_time))
    print("-------------------------")
