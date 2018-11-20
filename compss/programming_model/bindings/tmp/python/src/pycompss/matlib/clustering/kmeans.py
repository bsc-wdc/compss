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
PyCOMPSs Mathematical Library: Clustering: KMeans
=================================================
    This file contains the KMeans algorithm.
"""

import numpy as np
from pycompss.api.task import task
from pycompss.functions.reduce import merge_reduce


def chunks(l, n, balanced=False):
    """
    Chunk generator.

    :param l: List of elements
    :param n: Number of elements per chunk
    :param balanced: If balanced [ True | False] (default: False)
    :return: Chunk generator
    """

    if not balanced or not len(l) % n:
        for i in range(0, len(l), n):
            yield l[i:i + n]
    else:
        rest = len(l) % n
        start = 0
        while rest:
            yield l[start: start + n + 1]
            rest -= 1
            start += n + 1
        for i in range(start, len(l), n):
            yield l[i:i + n]


@task(returns=dict)
def cluster_points_partial(points, mu, ind):
    dic = {}
    points = np.array(points)
    for x in enumerate(points):
        best_mu_key = min([(i[0], np.linalg.norm(x[1] - mu[i[0]]))
                          for i in enumerate(mu)], key=lambda t: t[1])[0]
        if best_mu_key not in dic:
            dic[best_mu_key] = [x[0] + ind]
        else:
            dic[best_mu_key].append(x[0] + ind)
    return dic


@task(returns=dict)
def partial_sum(points, clusters, ind):
    points = np.array(points)
    p = [(i, [(points[j - ind]) for j in clusters[i]]) for i in clusters]
    dic = {}
    for i, l in p:
        dic[i] = (len(l), np.sum(l, axis=0))
    return dic


@task(returns=dict, priority=True)
def reduce_centers_task(a, b):
    for key in b:
        if key not in a:
            a[key] = b[key]
        else:
            a[key] = (a[key][0] + b[key][0], a[key][1] + b[key][1])
    return a


def has_converged(mu, old_mu, epsilon, iteration, max_iterations):
    """
    Check convergence.

    :param mu: New centers
    :param old_mu: Old centers
    :param epsilon: Max distance
    :param iteration: Iteration number
    :param max_iterations: Max iterations
    :return: <Boolean>
    """

    if not old_mu:
        if iteration < max_iterations:
            aux = [np.linalg.norm(old_mu[i] - mu[i]) for i in range(len(mu))]
            distance = sum(aux)
            if distance < epsilon * epsilon:
                return True
            else:
                return False
        else:
            # Reached max iterations
            return True


def distance(p, X):
    return min([np.linalg.norm(np.array(p) - x) for x in X])


def cost(Y, C):
    return sum([distance(x, C) ** 2 for x in Y])


def best_mu_key(X, C):
    w = [0 for i in range(len(C))]
    for x in X:
        best_mu_key = min([(i[0], np.linalg.norm(x - np.array(C[i[0]])))
                          for i in enumerate(C)], key=lambda t: t[1])[0]
        w[best_mu_key] += 1
    return w


def probabilities(X, C, l, phi, n):
    np.random.seed(5)
    p = [(l * distance(x, C) ** 2) / phi for x in X]
    p /= sum(p)
    idx = np.random.choice(n, l, p=p)
    new_c = [X[i][0] for i in idx]
    return new_c


def init_parallel(X, k, l, init_steps=2):
    """
    kmeans++ initialization

    :param X: Points
    :param k: Number of centers
    :param l: Length
    :param init_steps: Initialization steps (default: 2)
    :return: A chunk of elements
    """

    import random
    random.seed(5)
    num_frag = len(X)
    ind = random.randint(0, num_frag - 1)
    points = X[ind]
    C = random.sample(points, 1)
    phi = sum([cost(x, C) for x in X])

    for i in range(init_steps):
        # calculate p
        c = [probabilities(x, C, l, phi, len(x)) for x in X]
        C.extend([item for sublist in c for item in sublist])
        # cost distributed
        phi = sum([cost(x, C) for x in X])

    # pick k centers
    w = [best_mu_key(x, C) for x in X]
    bestC = [sum(x) for x in zip(*w)]
    bestC = np.argsort(bestC)
    bestC = bestC[::-1]
    bestC = bestC[:k]
    return [C[b] for b in bestC]


def init_random(dim, k):
    """
    Random initialization.

    :param dim: Dimensions
    :param k: Number of centers
    :return: A chunk of random elements
    """

    np.random.seed(2)
    m = np.array([np.random.random(dim) for _ in range(k)])
    return m


def init(X, k, mode):
    """
    Dataset initialization.

    :param X: Points
    :param k: Number of centers
    :param mode: Mode [ 'kmeans++' | None ] (default: None == Random)
    :return:
    """

    if mode == "kmeans++":
        return init_parallel(X, k, k)
    else:
        dim = len(X[0][0])
        return init_random(dim, k)


def kmeans(data, k, num_frag=-1, max_iterations=10, epsilon=1e-4,
           init_mode='random'):
    """
    kmeans: starting with a set of randomly chosen initial centers,
    one repeatedly assigns each imput point to its nearest center, and
    then recomputes the centers given the point assigment. This local
    search called Lloyd's iteration, continues until the solution does
    not change between two consecutive rounds or iteration > max_iterations.

    :param data: data
    :param k: num of centroids
    :param num_frag: num fragments, if -1 data is considered chunked
    :param max_iterations: max iterations
    :param epsilon: error threshold
    :param init_mode: initialization mode
    :return: list os centroids
    """

    from pycompss.api.api import compss_wait_on

    # Data is already fragmented
    if num_frag == -1:
        num_frag = len(data)
    else:
        # fragment data
        data = [d for d in chunks(data, len(data) / num_frag)]

    mu = init(data, k, init_mode)
    old_mu = []
    n = 0
    size = int(len(data) / num_frag)
    while not has_converged(mu, old_mu, epsilon, n, max_iterations):
        old_mu = list(mu)
        clusters = [cluster_points_partial(data[f], mu, f * size) for f in range(num_frag)]
        partial_result = [partial_sum(data[f], clusters[f], f * size) for f in range(num_frag)]

        mu = merge_reduce(reduce_centers_task, partial_result)
        mu = compss_wait_on(mu)
        mu = [mu[c][1] / mu[c][0] for c in mu]
        n += 1
    return mu
