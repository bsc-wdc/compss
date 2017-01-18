#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: scorella

PyCOMPSs Mathematical Library: Classification: Linear Regression
================================================================
    This file contains the linear regression algorithm.

    # yi = alpha + betaxi + epsiloni
    # goal: y=alpha + betax
"""


from pycompss.api.task import task
from pycompss.functions.reduce import mergeReduce
import math


@task(returns=int)
def _add(x):
    return sum(x)


@task(returns=int)
def reduce_add(x, y):
    return x+y


@task(returns=float)
def _mean(X, n):
    return sum(X)/float(n)


def mean(X, n, wait=False):
    result = mergeReduce(reduce_add, [_mean(x, n) for x in X])
    if wait:
        from pycompss.api.api import compss_wait_on
        result = compss_wait_on(result)
    return result


@task(returns=list)
def _norm(X, m):
    return [x-m for x in X]


@task(returns=list)
def _pow(X, p=2):
    return [pow(x, 2) for x in X]


@task(returns=float)
def _mul(x, y):
    return x*y


def std(X, m, n, wait=False):
    xs = [_norm(x, m) for x in X]
    xp = [_pow(x, 2) for x in xs]
    suma = mergeReduce(reduce_add, [_mean(x, n) for x in xp])
    if wait:
        from pycompss.api.api import compss_wait_on
        suma = compss_wait_on(suma)
    return suma


@task(returns=float)
def op_task(sum_x, sum_y, suma):
    return suma/float(math.sqrt(sum_x*sum_y))


@task(returns=float)
def multFrag(a, b):
    p = zip(a, b)
    result = 0
    for (a, b) in p:
        result += a * b
    return result


def pearson(X, Y, mx, my):
    xs = [_norm(x, mx) for x in X]
    ys = [_norm(y, my) for y in Y]
    xxs = [_pow(x, 2) for x in xs]
    yys = [_pow(y, 2) for y in ys]

    suma = mergeReduce(reduce_add, [multFrag(a, b) for (a,b) in zip(xs, ys)])

    sum_x = mergeReduce(reduce_add, map(_add, xxs))
    sum_y = mergeReduce(reduce_add, map(_add, yys))
    r = op_task(sum_x, sum_y, suma)
    return r


#@task(returns=types.LambdaType)
@task(returns=(float, float))
def computeLine(r, stdy, stdx, my, mx):
    b = r * (math.sqrt(stdy) / math.sqrt(stdx))
    A = my - b*mx

    #def line(x):
    #    return b*x-A
    #line = lambda x: b*x-A
    #return line
    #return lambda x: b*x-A
    return b, A


def fit(X, Y, n):
    from pycompss.api.api import compss_wait_on
    mx = mean(X, n)
    my = mean(Y, n)
    r = pearson(X, Y, mx, my)
    stdx = std(X, mx, n)
    stdy = std(Y, mx, n)

    line = computeLine(r, stdy, stdx, my, mx)

    line = compss_wait_on(line)
    print line
    return lambda x: line[0] * x + line[1]


# if __name__ == "__main__":
#     from numpy import arange
#     from numpy.random import randint
#     from pylab import scatter, show, plot, savefig
#     data = [[[1,2,3],[4,5,6]], [[1,2,3],[4,5,6]]]
#     #data = [[list(randint(100, size=1000)) for _ in range(10)] for _ in range(2)]
#     line = fit(data[0], data[1], 6)
#     print [line(x) for x in arange(0.0,100.0,1.0)]
#     datax = [item for sublist in data[0] for item in sublist]
#     datay = [item for sublist in data[1] for item in sublist]
#     scatter(datax, datay, marker='x')
#     plot([line(x) for x in arange(0.0, 10.0, 0.1)], arange(0.0, 10.0, 0.1))
#     savefig('lrd.png')
