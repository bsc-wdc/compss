#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *

import numpy as np


############################################
# MATRIX GENERATION
############################################

def initialize_variables(m_size, b_size):
    a = create_matrix(m_size, b_size, True)
    b = create_matrix(m_size, b_size, True)
    c = create_matrix(m_size, b_size, False)

    return a, b, c


def create_matrix(m_size, b_size, is_random):
    mat = []
    for i in range(m_size):
        mat.append([])
        for _ in range(m_size):
            mb = create_block(b_size, is_random)
            mat[i].append(mb)
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=list)
def create_block(b_size, is_random):
    if is_random:
        import os
        np.random.seed(ord(os.urandom(1)))
        block = np.array(np.random.random((b_size, b_size)), dtype=np.float64, copy=False)
    else:
        block = np.array(np.zeros((b_size, b_size)), dtype=np.float64, copy=False)
    mb = np.matrix(block, dtype=np.float64, copy=False)
    return mb


############################################
# MAIN FUNCTION
############################################

def matmul(a, b, c, m_size):
    # Debug
    if __debug__:
        a = compss_wait_on(a)
        b = compss_wait_on(b)
        c = compss_wait_on(c)

        print("Matrix A:")
        print(a)
        print("Matrix B:")
        print(b)
        print("Matrix C:")
        print(c)

    # Compute expected result
    if __debug__:
        input_a = join_matrix(a)
        input_b = join_matrix(b)
        res_expected = np.dot(input_a, input_b)

    # Matrix multiplication
    for i in range(m_size):
        for j in range(m_size):
            for k in range(m_size):
                multiply(a[i][k], b[k][j], c[i][j])

    # Debug result
    if __debug__:
        c = join_matrix(compss_wait_on(c))

        print("New Matrix C:")
        print(c)

    # Check result
    if __debug__:
        check_result(c, res_expected)


############################################
# MATHEMATICAL FUNCTIONS
############################################

@task(c=INOUT)
def multiply(a, b, c):
    # import time
    # start = time.time()

    c += np.dot(a, b)

    # end = time.time()
    # tm = end - start
    # print "TIME: " + str(tm*1000) + " ms"


############################################
# BLOCK HANDLING FUNCTIONS
############################################

def join_matrix(a):
    joint_matrix = np.matrix([[]])
    for i in range(0, len(a)):
        current_row = a[i][0]
        for j in range(1, len(a[i])):
            current_row = np.bmat([[current_row, a[i][j]]])
        if i == 0:
            joint_matrix = current_row
        else:
            joint_matrix = np.bmat([[joint_matrix], [current_row]])

    return np.matrix(joint_matrix)


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
    MSIZE = int(args[0])
    BSIZE = int(args[1])

    # Log arguments if required
    if __debug__:
        print("Running matmul blocked application with:")
        print(" - MSIZE = " + str(MSIZE))
        print(" - BSIZE = " + str(BSIZE))

    # Initialize matrices
    if __debug__:
        print("Initializing matrices")
    start_time = time.time()
    A, B, C = initialize_variables(MSIZE, BSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    mult_start_time = time.time()
    matmul(A, B, C, MSIZE)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = mult_start_time - start_time
    mult_time = end_time - mult_start_time

    print("RESULTS -----------------")
    print("VERSION USERPARALLEL")
    print("MSIZE " + str(MSIZE))
    print("BSIZE " + str(BSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("MULT_TIME " + str(mult_time))
    print("-------------------------")
