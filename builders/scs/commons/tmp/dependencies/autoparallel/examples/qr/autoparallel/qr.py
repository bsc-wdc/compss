#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from pycompss.api.parallel import parallel
from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.api import compss_barrier, compss_wait_on

import numpy as np


# TODO: Extend for non-square matrices. There is no size verifications.


############################################
# MATRIX GENERATION
############################################

def generate_matrix(m_size, b_size, block_type='random'):
    mat = []
    for i in range(m_size):
        mat.append([])
        for _ in range(m_size):
            mat[i].append(create_block_task(b_size, block_type=block_type))
    return mat


def generate_identity(m_size, b_size):
    mat = []
    for i in range(m_size):
        mat.append([])
        for _ in range(0, i):
            mat[i].append(create_block_task(b_size, block_type='zeros'))
        mat[i].append(create_block_task(b_size, block_type='identity'))
        for _ in range(i + 1, m_size):
            mat[i].append(create_block_task(b_size, block_type='zeros'))
    return mat


@constraint(ComputingUnits="${ComputingUnits}")
@task(returns=list)
def create_block_task(b_size, block_type='random'):
    return create_block(b_size, block_type=block_type)


def create_block(b_size, block_type='random'):
    if block_type == 'zeros':
        block = np.matrix(np.zeros((b_size, b_size)), dtype=np.float64, copy=False)
    elif block_type == 'identity':
        block = np.matrix(np.identity(b_size), dtype=np.float64, copy=False)
    else:
        import os
        np.random.seed(ord(os.urandom(1)))
        block = np.matrix(np.random.random((b_size, b_size)), dtype=np.float64, copy=False)
    return block


############################################
# MAIN FUNCTION
############################################

@parallel()
def qr_blocked(a, m_size, b_size):
    # Debug
    if __debug__:
        a = compss_wait_on(a)

        print("Matrix A:")
        print(a)

    # Initialize Q and R matrices
    q = generate_identity(m_size, b_size)
    r = copy_blocked(a)

    # Initialize intermediate iteration variables
    q_act = [None]
    q_sub = [[np.matrix(np.array([0])), np.matrix(np.array([0]))], [np.matrix(np.array([0])), np.matrix(np.array([0]))]]
    aux = [None, None]

    # Main loop
    for i in range(m_size):
        q_act[0], r[i][i] = qr(r[i][i], transpose=True)

        for j in range(m_size):
            q[j][i] = dot(q[j][i], q_act[0], transpose_b=True)

        for j in range(i + 1, m_size):
            r[i][j] = dot(q_act[0], r[i][j])

        # Update values of the respective column
        for j in range(i + 1, m_size):
            q_sub[0][0], q_sub[0][1], q_sub[1][0], q_sub[1][1], r[i][i], r[j][i] = little_qr(r[i][i], r[j][i], b_size,
                                                                                             transpose=True)

            # Update values of the row for the value updated in the column
            for k in range(i + 1, m_size):
                # [[r[i][k]], [r[j][k]]] = multiply_blocked_1(q_sub, [[r[i][k]], [r[j][k]]], b_size, transpose_b=False)
                aux[0] = create_block(b_size, block_type='zeros')
                aux[0] = multiply_single_block(q_sub[0][0], r[i][k], aux[0], transpose_b=False)
                aux[0] = multiply_single_block(q_sub[0][1], r[j][k], aux[0], transpose_b=False)

                aux[1] = create_block(b_size, block_type='zeros')
                aux[1] = multiply_single_block(q_sub[1][0], r[i][k], aux[1], transpose_b=False)
                aux[1] = multiply_single_block(q_sub[1][1], r[j][k], aux[1], transpose_b=False)

                r[i][k] = copy_reference(aux[0])
                r[j][k] = copy_reference(aux[1])

            for k in range(m_size):
                # [[q[k][i], q[k][j]]] = multiply_blocked_2([[q[k][i], q[k][j]]], q_sub, b_size, transpose_b=True)
                aux[0] = create_block(b_size, block_type='zeros')
                aux[0] = multiply_single_block(q[k][i], q_sub[0][0], aux[0], transpose_b=True)
                aux[0] = multiply_single_block(q[k][j], q_sub[0][1], aux[0], transpose_b=True)

                aux[1] = create_block(b_size, block_type='zeros')
                aux[1] = multiply_single_block(q[k][i], q_sub[1][0], aux[1], transpose_b=True)
                aux[1] = multiply_single_block(q[k][j], q_sub[1][1], aux[1], transpose_b=True)

                q[k][i] = copy_reference(aux[0])
                q[k][j] = copy_reference(aux[1])

    # Debug result
    if __debug__:
        input_a = join_matrix(compss_wait_on(a))
        q_res = join_matrix(compss_wait_on(q))
        r_res = join_matrix(compss_wait_on(r))

        print("Matrix A:")
        print(input_a)
        print("Matrix Q:")
        print(q_res)
        print("Matrix R:")
        print(r_res)

    # Check result
    if __debug__:
        check_result(q_res, r_res, input_a)


############################################
# MATHEMATICAL FUNCTIONS
############################################

def qr(a, mode='reduced', transpose=False):
    # Numpy call
    from numpy.linalg import qr as qr_numpy
    q, r = qr_numpy(a, mode=mode)

    # Transpose if requested
    if transpose:
        q = np.transpose(q)

    return q, r


def dot(a, b, transpose_result=False, transpose_b=False):
    if transpose_b:
        b = np.transpose(b)

    if transpose_result:
        return np.transpose(np.dot(a, b))
    else:
        return np.dot(a, b)


def little_qr(a, b, b_size, transpose=False):
    # Numpy call
    from numpy.linalg import qr as qr_numpy
    current_a = np.bmat([[a], [b]])
    sub_q, sub_r = qr_numpy(current_a, mode='complete')

    new_a = sub_r[0:b_size]
    new_b = sub_r[b_size:2 * b_size]
    sub_q = split_matrix(sub_q, 2)

    # Transpose if requested (care indexes)
    if transpose:
        return np.transpose(sub_q[0][0]), np.transpose(sub_q[1][0]), np.transpose(sub_q[0][1]), np.transpose(
            sub_q[1][1]), new_a, new_b
    else:
        return sub_q[0][0], sub_q[0][1], sub_q[1][0], sub_q[1][1], new_a, new_b


def multiply_single_block(a, b, c, transpose_b=False):
    # Transpose if requested
    if transpose_b:
        b = np.transpose(b)

    # Numpy operation
    return c + np.dot(a, b)


############################################
# BLOCK HANDLING FUNCTIONS
############################################

def copy_blocked(a, transpose=False):
    res = []
    for i in range(len(a)):
        res.append([])
        for j in range(len(a[0])):
            res[i].append(np.matrix([0]))
    for i in range(len(a)):
        for j in range(len(a[0])):
            if transpose:
                res[j][i] = a[i][j]
            else:
                res[i][j] = a[i][j]
    return res


def copy_reference(block):
    return block


def split_matrix(a, m_size):
    b_size = len(a) / m_size

    new_mat = [[None for _ in range(m_size)] for _ in range(m_size)]
    for i in range(m_size):
        for j in range(m_size):
            new_mat[i][j] = np.matrix(a[i * b_size:(i + 1) * b_size, j * b_size:(j + 1) * b_size])
    return new_mat


def join_matrix(a):
    res = np.matrix([[]])
    for i in range(0, len(a)):
        current_row = a[i][0]
        for j in range(1, len(a[i])):
            current_row = np.bmat([[current_row, a[i][j]]])
        if i == 0:
            res = current_row
        else:
            res = np.bmat([[res], [current_row]])
    return np.matrix(res)


def check_result(q_res, r_res, input_a):
    is_ok = np.allclose(np.dot(q_res, r_res), input_a)
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
        print("Running QR application with:")
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
    qr_start_time = time.time()
    qr_blocked(A, MSIZE, BSIZE)
    compss_barrier(True)
    end_time = time.time()

    # Log results and time
    if __debug__:
        print("Post-process results")
    total_time = end_time - start_time
    init_time = qr_start_time - start_time
    qr_time = end_time - qr_start_time

    print("RESULTS -----------------")
    print("VERSION AUTOPARALLEL")
    print("MSIZE " + str(MSIZE))
    print("BSIZE " + str(BSIZE))
    print("DEBUG " + str(__debug__))
    print("TOTAL_TIME " + str(total_time))
    print("INIT_TIME " + str(init_time))
    print("QR_TIME " + str(qr_time))
    print("-------------------------")
