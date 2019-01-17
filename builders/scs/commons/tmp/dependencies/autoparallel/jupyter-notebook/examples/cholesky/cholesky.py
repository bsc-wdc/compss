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

def generate_matrix(m_size, b_size):
    mat = []
    for i in range(m_size):
        mat.append([])
        for _ in range(m_size):
            mat[i].append([])

    for i in range(m_size):
        mat[i][i] = create_block(b_size, True)
        for j in range(i + 1, m_size):
            mat[i][j] = create_block(b_size, False)

    # Make it symmetric
    for i in range(m_size):
        mat[i][i] = compss_wait_on(mat[i][i])
        for j in range(i + 1, m_size):
            mat[i][j] = compss_wait_on(mat[i][j])  # To break aliasing between future objects
            mat[j][i] = mat[i][j]

    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=list)
def create_block(b_size, is_diag):
    import os
    np.random.seed(ord(os.urandom(1)))
    block = np.array(np.random.random((b_size, b_size)), dtype=np.float64, copy=False)

    mb = np.matrix(block, dtype=np.float64, copy=False)
    mb = mb + np.transpose(mb)
    if is_diag:
        mb = mb + 2 * b_size * np.eye(b_size)
    return mb


############################################
# MAIN FUNCTION
############################################

@parallel()
def cholesky_blocked(a, m_size, b_size):
    # Debug
    if __debug__:
        a = compss_wait_on(a)
        print("Matrix A:")
        print(a)

    # Compute expected result
    if __debug__:
        from numpy.linalg import cholesky as cholesky_numpy
        res_expected = cholesky_numpy(join_matrix(a))

    # Cholesky decomposition
    for k in range(m_size):
        # Diagonal block factorization
        a[k][k] = potrf(a[k][k])

        # Triangular systems
        for i in range(k + 1, m_size):
            a[i][k] = solve_triangular(a[k][k], a[i][k])
            a[k][i] = np.zeros((b_size, b_size))

        # Update trailing matrix
        for i in range(k + 1, m_size):
            for j in range(i, m_size):
                a[j][i] = gemm(-1.0, a[j][k], a[i][k], a[j][i], 1.0)
                # Only for A=B
                # a[j][i] = syrk(a[j][k], a[j][i])

    # Debug result
    if __debug__:
        a = compss_wait_on(a)
        res = join_matrix(a)

        print("New Matrix A:")
        print(res)

    # Check result
    if __debug__:
        check_result(res, res_expected)


############################################
# MATHEMATICAL FUNCTIONS
############################################

def potrf(a):
    from scipy.linalg.lapack import dpotrf
    a = dpotrf(a, lower=True)[0]
    return a


def solve_triangular(a, b):
    from scipy.linalg import solve_triangular
    from numpy import transpose

    b = transpose(b)
    b = solve_triangular(a, b, lower=True)
    b = transpose(b)
    return b


def gemm(alpha, a, b, c, beta):
    from scipy.linalg.blas import dgemm
    from numpy import transpose

    b = transpose(b)
    c = dgemm(alpha, a, b, c=c, beta=beta)
    return c


def syrk(a, b):
    from scipy.linalg.blas import dsyrk

    alpha = -1.0
    beta = 1.0
    b = dsyrk(alpha, a, c=b, beta=beta, lower=True)
    return b


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
        print("Running cholesky application with:")
        print(" - MSIZE = " + str(MSIZE))
        print(" - BSIZE = " + str(BSIZE))

    # Initialize matrix
    if __debug__:
        print("Initializing matrix")
    start_time = time.time()
    A = generate_matrix(MSIZE, BSIZE)
    compss_barrier()

    # Begin computation
    if __debug__:
        print("Performing computation")
    cholesky_start_time = time.time()
    cholesky_blocked(A, MSIZE, BSIZE)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = cholesky_start_time - start_time
    cholesky_time = end_time - cholesky_start_time

    print("RESULTS -----------------")
    print("VERSION AUTOPARALLEL")
    print("MSIZE " + str(MSIZE))
    print("BSIZE " + str(BSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("CHOLESKY_TIME " + str(cholesky_time))
    print("-------------------------")
