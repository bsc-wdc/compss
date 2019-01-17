#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

# -*- coding: utf-8 -*-

"""
PyCOMPSs Mathematical Library: Classification: Linear Regression
================================================================
    This file contains the linear regression algorithm.

    # yi = alpha + betaxi + epsiloni
    # goal: y=alpha + betax
"""

from pycompss.api.task import task
from pycompss.functions.reduce import merge_reduce
import math


@task(returns=int)
def _add(x):
    """
    Add all elements of a list.

    :param x: List to sum
    :return: Sum of all elements in the input list
    """

    return sum(x)


@task(returns=int)
def reduce_add(x, y):
    """
    Add x and y.

    :param x: First element
    :param y: Second element
    :return: The sum of x and y.
    """

    return x + y


@task(returns=float)
def _mean(data, n):
    """
    Calculate the mean of a list,

    :param data: List of elements
    :param n: Number of elements
    :return: Mean
    """

    return sum(data) / float(n)


def mean(data, n, wait=False):
    """
    Calculate the mean of a list,

    :param data: List of elements
    :param n: Number of elements
    :param wait: <Boolean> Wait for the result
    :return: Mean
    """

    result = merge_reduce(reduce_add, [_mean(x, n) for x in data])
    if wait:
        from pycompss.api.api import compss_wait_on
        result = compss_wait_on(result)
    return result


@task(returns=list)
def _norm(data, m):
    """
    Normalize the elements of a list,

    :param data: List of elements
    :param m: Normalization value
    :return: the list normalized
    """

    return [x - m for x in data]


@task(returns=list)
def _pow(data, p=2):
    """
    Calculate the power of all list elements.

    :param data: List of elements
    :param p: Power value
    :return: the list with its elements elevated to p power
    """

    return [pow(x, p) for x in data]


@task(returns=float)
def _mul(x, y):
    """
    Multiply x and y.

    :param x: First element
    :param y: Second element
    :return: The multiplication result of x and y.
    """

    return x * y


def std(data, m, n, wait=False):
    """
    Calculate the standard deviation.

    :param data: List of elements
    :param m: M
    :param n: N
    :param wait: Wait for the result
    :return: the standard deviation
    """

    xs = [_norm(x, m) for x in data]
    xp = [_pow(x, 2) for x in xs]
    sum_a = merge_reduce(reduce_add, [_mean(x, n) for x in xp])
    if wait:
        from pycompss.api.api import compss_wait_on
        sum_a = compss_wait_on(sum_a)
    return sum_a


@task(returns=float)
def op_task(sum_x, sum_y, sum_a):
    """
    Return the sum_a divided by the root square of sum_x * sum_y

    :param sum_x: Sum of all X elements
    :param sum_y: Sum of all Y elements
    :param sum_a: Sum of all elements
    :return: the result of sum_a divided by the root square of sum_x * sum_y
    """

    return sum_a / float(math.sqrt(sum_x * sum_y))


@task(returns=float)
def mult_frag(a, b):
    """
    Multiply two lists.

    :param a: First list
    :param b: Second list
    :return: The result of multiplying the two lists
    """

    p = zip(a, b)
    result = 0
    for (a, b) in p:
        result += a * b
    return result


def pearson(data_x, data_y, mx, my):
    """
    Calculate the pearson coefficient.

    :param data_x: X data elements
    :param data_y: Y data elements
    :param mx: MX
    :param my: MY
    :return: The pearson coefficient.
    """

    xs = [_norm(x, mx) for x in data_x]
    ys = [_norm(y, my) for y in data_y]
    xxs = [_pow(x, 2) for x in xs]
    yys = [_pow(y, 2) for y in ys]

    suma = merge_reduce(reduce_add, [mult_frag(a, b) for (a, b) in zip(xs, ys)])

    sum_x = merge_reduce(reduce_add, map(_add, xxs))
    sum_y = merge_reduce(reduce_add, map(_add, yys))
    r = op_task(sum_x, sum_y, suma)
    return r


@task(returns=(float, float))
def compute_line(r, std_y, std_x, my, mx):
    """
    Compute line

    :param r: R
    :param std_y: Y standard deviation
    :param std_x: X standard deviation
    :param my: MY
    :param mx: MX
    :return: b and a
    """

    b = r * (math.sqrt(std_y) / math.sqrt(std_x))
    a = my - b * mx
    return b, a


def fit(data_x, data_y, n):
    """
    Main function.

    :param data_x: X data elements
    :param data_y: Y data elements
    :param n: Number of elements
    :return: The linear regression result
    """

    from pycompss.api.api import compss_wait_on
    mx = mean(data_x, n)
    my = mean(data_y, n)
    r = pearson(data_x, data_y, mx, my)
    std_x = std(data_x, mx, n)
    std_y = std(data_y, mx, n)

    line = compute_line(r, std_y, std_x, my, mx)

    line = compss_wait_on(line)
    print(line)
    return lambda x: line[0] * x + line[1]


'''
if __name__ == "__main__":
    from numpy import arange
    from numpy.random import randint
    from pylab import scatter, show, plot, savefig
    data = [[[1,2,3],[4,5,6]], [[1,2,3],[4,5,6]]]
    # data = [[list(randint(100, size=1000)) for _ in range(10)] for _ in range(2)]
    line = fit(data[0], data[1], 6)
    print([line(x) for x in arange(0.0,100.0,1.0)])
    datax = [item for sublist in data[0] for item in sublist]
    datay = [item for sublist in data[1] for item in sublist]
    scatter(datax, datay, marker='x')
    plot([line(x) for x in arange(0.0, 10.0, 0.1)], arange(0.0, 10.0, 0.1))
    savefig('lrd.png')
'''
