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

def initialize_variables(nx_size, ny_size):
    ex = create_matrix(nx_size, ny_size, nx_size)
    ey = create_matrix(nx_size, ny_size, ny_size)
    hz = create_matrix(nx_size, ny_size, nx_size)

    return ex, ey, hz


def create_matrix(nx_size, ny_size, ref_size):
    mat = []
    for i in range(nx_size):
        mat.append([])
        for j in range(ny_size):
            mb = create_entry(i, j, ref_size)
            mat[i].append(mb)
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=1)
def create_entry(i, j, size):
    return np.float64(np.float64(i * (j + 1)) / np.float64(size))


############################################
# MAIN FUNCTION
############################################

# @parallel()
@parallel(pluto_extra_flags=["--tile"], taskify_loop_level=3)
def fdtd_2d(ex, ey, hz, nx_size, ny_size, t_size, coef1, coef2):
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
        hz_expected = seq_fdtd_2d(ex_seq, ey_seq, hz_seq, nx_size, ny_size, t_size, coef1, coef2)

    for t in range(t_size):
        for j in range(ny_size):
            ey[0][j] = copy_reference(t)
        for i in range(1, nx_size):
            for j in range(ny_size):
                # ey[i][j] -= coef1 * (hz[i][j] - hz[i - 1][j])
                ey[i][j] = compute_e(ey[i][j], coef1, hz[i][j], hz[i - 1][j])
        for i in range(nx_size):
            for j in range(1, ny_size):
                # ex[i][j] -= coef1 * (hz[i][j] - hz[i][j - 1])
                ex[i][j] = compute_e(ex[i][j], coef1, hz[i][j], hz[i][j - 1])
        for i in range(nx_size - 1):
            for j in range(ny_size - 1):
                # hz[i][j] -= coef2 * (ex[i][j + 1] - ex[i][j] + ey[i + 1][j] - ey[i][j])
                hz[i][j] = compute_h(hz[i][j], coef2, ex[i][j + 1], ex[i][j], ey[i + 1][j], ey[i][j])

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

def compute_e(e, coef1, h2, h1):
    # import time
    # start = time.time()

    return e - coef1 * (h2 - h1)

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


def compute_h(h, coef2, ex2, ex1, ey2, ey1):
    # import time
    # start = time.time()

    return h - coef2 * (ex2 - ex1 + ey2 - ey1)

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


def copy_reference(elem):
    return elem


############################################
# RESULT CHECK FUNCTIONS
############################################

def seq_fdtd_2d(ex, ey, hz, nx_size, ny_size, t_size, coef1, coef2):
    for t in range(t_size):
        for j in range(ny_size):
            ey[0][j] = t
        for i in range(1, nx_size):
            for j in range(ny_size):
                ey[i][j] -= coef1 * (hz[i][j] - hz[i - 1][j])
        for i in range(nx_size):
            for j in range(1, ny_size):
                ex[i][j] -= coef1 * (hz[i][j] - hz[i][j - 1])
        for i in range(nx_size - 1):
            for j in range(ny_size - 1):
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
    NXSIZE = int(args[0])
    NYSIZE = int(args[1])
    TSIZE = int(args[2])
    COEF1 = np.float64(0.5)
    COEF2 = np.float64(0.7)

    # Log arguments if required
    if __debug__:
        print("Running fdtd-2d application with:")
        print(" - NXSIZE = " + str(NXSIZE))
        print(" - NYSIZE = " + str(NYSIZE))
        print(" - TSIZE = " + str(TSIZE))
        print(" - COEF1 = " + str(COEF1))
        print(" - COEF2 = " + str(COEF2))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    EX, EY, HZ = initialize_variables(NXSIZE, NYSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    fdtd_start_time = time.time()
    fdtd_2d(EX, EY, HZ, NXSIZE, NYSIZE, TSIZE, COEF1, COEF2)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = fdtd_start_time - start_time
    fdtd_time = end_time - fdtd_start_time

    print("RESULTS -----------------")
    print("VERSION AUTOPARALLEL")
    print("NXSIZE " + str(NXSIZE))
    print("NYSIZE " + str(NYSIZE))
    print("TSIZE " + str(TSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("FDTD_TIME " + str(fdtd_time))
    print("-------------------------")
