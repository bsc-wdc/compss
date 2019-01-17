#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
# from pycompss.api.parallel import parallel


# Initializes a matrix with size (n x m) with blocks (bSize x bSize) randomly or not
def initialize(n_size, m_size, b_size, random):
    import numpy as np

    matrix = []
    for i in range(n_size):
        matrix.append([])
        for _ in range(m_size):
            if random:
                block = np.array(np.random.random((b_size, b_size)), dtype=np.double, copy=False)
            else:
                block = np.array(np.zeros((b_size, b_size)), dtype=np.double, copy=False)
            mb = np.matrix(block, dtype=np.double, copy=False)
            matrix[i].append(mb)
    return matrix


# Performs the matrix multiplication by blocks
# @parallel()
def matmul(m_size, n_size, k_size, b_size, debug):
    # Initialize
    a = initialize(m_size, n_size, b_size, True)
    b = initialize(n_size, k_size, b_size, True)
    c = initialize(m_size, k_size, b_size, False)

    # Debug
    if debug:
        print("Matrix A:")
        print(a)
        print("Matrix B:")
        print(b)
        print("Matrix C:")
        print(c)

    # Perform computation
    # c = a*b
    for i in range(m_size):
        for j in range(k_size):
            for k in range(n_size):
                c[i][j] += a[i][k] * b[k][j]

    # Debug
    if debug:
        print("Matrix C:")
        print(c)

    # Result
    return c


# MAIN CODE
if __name__ == "__main__":
    # Import libraries
    import time

    # Parse arguments
    m_mat_size = 5
    n_mat_size = 2
    k_mat_size = 3
    block_size = 1
    is_debug = True

    # Begin computation
    startTime = time.time()
    result = matmul(m_mat_size, n_mat_size, k_mat_size, block_size, is_debug)
    endTime = time.time()

    # Log results and time
    total_time = endTime - startTime
    print("TOTAL_TIME " + str(total_time))
