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

def generate_matrix(m_size, b_size):
    mat = []
    for i in range(m_size):
        mat.append([])
        for _ in range(m_size):
            mb = create_block(b_size)
            mat[i].append(mb)
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=list)
def create_block(b_size):
    import os
    np.random.seed(ord(os.urandom(1)))
    block = np.array(np.random.random((b_size, b_size)), dtype=np.float64, copy=False)
    mb = np.matrix(block, dtype=np.float64, copy=False)
    return mb


############################################
# MAIN FUNCTION
############################################

def lu_blocked(a, m_size, b_size):
    # Debug
    if __debug__:
        a = compss_wait_on(a)
        print("Matrix A:")
        print(a)

    # Compute expected result
    if __debug__:
        input_a = join_matrix(a)
        res_expected = np.zeros((m_size * b_size, m_size * b_size))

    if len(a) == 0:
        return

    # Initialization
    p_mat = [[np.matrix(np.zeros((b_size, b_size)), dtype=float)] * m_size for _ in range(m_size)]
    l_mat = [[None for _ in range(m_size)] for _ in range(m_size)]
    u_mat = [[None for _ in range(m_size)] for _ in range(m_size)]
    for i in range(m_size):
        for j in range(i + 1, m_size):
            l_mat[i][j] = np.matrix(np.zeros((b_size, b_size)), dtype=float)
            u_mat[j][i] = np.matrix(np.zeros((b_size, b_size)), dtype=float)
    aux = [None]
    aux2 = [None]

    # First element
    p_mat[0][0], l_mat[0][0], u_mat[0][0] = custom_lu(a[0][0])

    for j in range(1, m_size):
        aux[0] = invert_triangular(l_mat[0][0], lower=True)
        u_mat[0][j] = multiply([1], aux[0], p_mat[0][0], a[0][j])

    # The rest of elements
    for i in range(1, m_size):
        for j in range(i, m_size):
            for k in range(i, m_size):
                aux[0] = invert_triangular(u_mat[i - 1][i - 1], lower=False)
                aux2[0] = multiply([], a[j][i - 1], aux[0])
                a[j][k] = dgemm(-1, a[j][k], aux2[0], u_mat[i - 1][k])

        p_mat[i][i], l_mat[i][i], u_mat[i][i] = custom_lu(a[i][i])

        for j in range(0, i):
            aux[0] = invert_triangular(u_mat[j][j], lower=False)
            l_mat[i][j] = multiply([0], p_mat[i][i], a[i][j], aux[0])

        for j in range(i + 1, m_size):
            aux[0] = invert_triangular(l_mat[i][i], lower=True)
            u_mat[i][j] = multiply([1], aux[0], p_mat[i][i], a[i][j])

    # Debug result
    if __debug__:
        p_res = join_matrix(compss_wait_on(p_mat))
        l_res = join_matrix(compss_wait_on(l_mat))
        u_res = join_matrix(compss_wait_on(u_mat))

        print("Matrix P:")
        print(p_res)
        print("Matrix L:")
        print(l_res)
        print("Matrix U:")
        print(u_res)

    # Check result
    if __debug__:
        check_result(input_a, p_res, l_res, u_res, res_expected)


############################################
# MATHEMATICAL FUNCTIONS
############################################

@task(returns=1)
def invert_triangular(mat, lower=False):
    from scipy.linalg import solve_triangular

    print(mat)

    dim = len(mat)
    iden = np.matrix(np.identity(dim))
    return solve_triangular(mat, iden, lower=lower)


@task(returns=1)
def multiply(inv_list, *args):
    assert len(args) > 0

    input_args = list(args)
    if len(inv_list) > 0:
        from numpy.linalg import inv
        for elem in inv_list:
            input_args[elem] = inv(args[elem])

    if len(input_args) == 1:
        return input_args[0]

    result = np.dot(input_args[0], input_args[1])
    for i in range(2, len(input_args)):
        result = np.dot(result, input_args[i])
    return result


@task(returns=1)
def dgemm(alpha, a, b, c):
    mat = a + (alpha * np.dot(b, c))
    return mat


@task(returns=3)
def custom_lu(mat):
    from scipy.linalg import lu

    return lu(mat)


############################################
# BLOCK HANDLING FUNCTIONS
############################################

def join_matrix(mat):
    joint_mat = np.matrix([[]])
    for i in range(0, len(mat)):
        current_row = mat[i][0]
        for j in range(1, len(mat[i])):
            current_row = np.bmat([[current_row, mat[i][j]]])
        if i == 0:
            joint_mat = current_row
        else:
            joint_mat = np.bmat([[joint_mat], [current_row]])

    return np.matrix(joint_mat)


def check_result(input_a, p_res, l_res, u_res, result_expected):
    result = input_a - np.dot(np.dot(p_res, l_res), u_res)
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
        print("Running LU application with:")
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
    lu_start_time = time.time()
    lu_blocked(A, MSIZE, BSIZE)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = lu_start_time - start_time
    lu_time = end_time - lu_start_time

    print("RESULTS -----------------")
    print("VERSION USERPARALLEL")
    print("MSIZE " + str(MSIZE))
    print("BSIZE " + str(BSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("LU_TIME " + str(lu_time))
    print("-------------------------")
