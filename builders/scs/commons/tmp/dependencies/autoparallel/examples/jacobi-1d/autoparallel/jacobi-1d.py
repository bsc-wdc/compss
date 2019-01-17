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
    a = create_matrix(n_size, 2)
    b = create_matrix(n_size, 3)

    return a, b


def create_matrix(n_size, offset):
    mat = []
    for i in range(n_size):
        mb = create_entry(i, n_size, offset)
        mat.append(mb)

    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=1)
def create_entry(index, n_size, offset):
    return np.float64(np.float64(index + offset) / np.float64(n_size))


############################################
# MAIN FUNCTION
############################################

# @parallel(pluto_extra_flags=["--tile"], taskify_loop_level=2)
@parallel()
def jacobi_1d(a, b, n_size, t_size, coef):
    # Debug
    if __debug__:
        a = compss_wait_on(a)
        b = compss_wait_on(b)
        print("Matrix A:")
        print(a)
        print("Matrix B:")
        print(b)

    # Compute expected result
    if __debug__:
        import copy
        a_seq = copy.deepcopy(a)
        b_seq = copy.deepcopy(b)
        a_expected, b_expected = seq_jacobi_1d(a_seq, b_seq, n_size, t_size, coef)

    # Jacobi
    for _ in range(t_size):
        for i in range(1, n_size - 1):
            b[i] = compute(coef, a[i - 1], a[i], a[i + 1])
        for i in range(1, n_size - 1):
            a[i] = compute(coef, b[i - 1], b[i], b[i + 1])

    # Debug result
    if __debug__:
        a = compss_wait_on(a)
        b = compss_wait_on(b)

        print("New Matrix A:")
        print(a)
        print("New Matrix B:")
        print(b)

    # Check result
    if __debug__:
        check_result(a, b, a_expected, b_expected)


############################################
# MATHEMATICAL FUNCTIONS
############################################

def compute(coef, left, center, right):
    # import time
    # start = time.time()

    return coef * (left + center + right)

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


############################################
# RESULT CHECK FUNCTIONS
############################################

def seq_jacobi_1d(a, b, n_size, t_size, coef):
    for _ in range(t_size):
        for i in range(1, n_size - 1):
            b[i] = coef * (a[i - 1] + a[i] + a[i + 1])
        for i in range(1, n_size - 1):
            a[i] = coef * (b[i - 1] + b[i] + b[i + 1])

    return a, b


def check_result(a, b, a_expected, b_expected):
    is_a_ok = np.allclose(a, a_expected)
    is_b_ok = np.allclose(b, b_expected)
    is_ok = is_a_ok and is_b_ok
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
    COEF = np.float64(np.float64(1) / np.float64(3))

    # Log arguments if required
    if __debug__:
        print("Running jacobi-1d application with:")
        print(" - NSIZE = " + str(NSIZE))
        print(" - TSIZE = " + str(TSIZE))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    A, B = initialize_variables(NSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    jacobi_start_time = time.time()
    jacobi_1d(A, B, NSIZE, TSIZE, COEF)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = jacobi_start_time - start_time
    jacobi_time = end_time - jacobi_start_time

    print("RESULTS -----------------")
    print("VERSION AUTOPARALLEL")
    print("NSIZE " + str(NSIZE))
    print("TSIZE " + str(TSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("JACOBI_TIME " + str(jacobi_time))
    print("-------------------------")
