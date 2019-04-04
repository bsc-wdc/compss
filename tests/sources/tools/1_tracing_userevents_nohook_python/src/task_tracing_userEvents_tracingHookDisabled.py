#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench KMeans
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.functions.reduce import merge_reduce

import random

USER_EVENTS = 70000100
CLUSTER_MAIN_LOOP = 400
PARTIAL_SUM = 401
REDUCE_KEY = 402
REDUCE_NO_KEY = 403
GENERATE = 404


def init_board_gauss(numV, dim, K):
    n = int(float(numV) / K)
    data = []
    random.seed(5)
    for k in range(K):
        c = [random.uniform(-1, 1) for i in range(dim)]
        s = random.uniform(0.05, 0.5)
        for i in range(n):
            d = np.array([np.random.normal(c[j], s) for j in range(dim)])
            data.append(d)

    Data = np.array(data)[:numV]
    return Data


def init_board_random(numV, dim):
    from numpy import random
    random.seed(5)
    return [random.random(dim) for _ in range(numV)]


@task(returns=dict, tracing_hook=False)
def cluster_points_partial(XP, mu, ind):
    import pyextrae.multiprocessing as pyextrae
    import numpy as np
    dic = {}
    XP = np.array(XP)
    pyextrae.eventandcounters(USER_EVENTS, CLUSTER_MAIN_LOOP)
    for x in enumerate(XP):
        bestmukey = min([(i[0], np.linalg.norm(x[1] - mu[i[0]]))
                         for i in enumerate(mu)], key=lambda t: t[1])[0]
        if bestmukey not in dic:
            dic[bestmukey] = [x[0] + ind]
        else:
            dic[bestmukey].append(x[0] + ind)
    pyextrae.eventandcounters(USER_EVENTS, 0)
    return dic


@task(returns=dict, tracing_hook=False)
def partial_sum(XP, clusters, ind):
    import pyextrae.multiprocessing as pyextrae
    import numpy as np
    XP = np.array(XP)
    pyextrae.eventandcounters(USER_EVENTS, PARTIAL_SUM)
    p = [(i, [(XP[j - ind]) for j in clusters[i]]) for i in clusters]
    pyextrae.eventandcounters(USER_EVENTS, 0)
    dic = {}
    for i, l in p:
        dic[i] = (len(l), np.sum(l, axis=0))
    return dic


@task(returns=dict, priority=True, tracing_hook=False)
def reduceCentersTask(a, b):
    import pyextrae.multiprocessing as pyextrae
    for key in b:
        if key not in a:
            pyextrae.eventandcounters(USER_EVENTS, REDUCE_NO_KEY)
            a[key] = b[key]
            pyextrae.eventandcounters(USER_EVENTS, 0)
        else:
            pyextrae.eventandcounters(USER_EVENTS, REDUCE_KEY)
            a[key] = (a[key][0] + b[key][0], a[key][1] + b[key][1])
            pyextrae.eventandcounters(USER_EVENTS, 0)
    return a


def has_converged(mu, oldmu, epsilon, iter, maxIterations):
    print("iter: " + str(iter))
    print("maxIterations: " + str(maxIterations))
    if oldmu != []:
        if iter < maxIterations:
            aux = [np.linalg.norm(oldmu[i] - mu[i]) for i in range(len(mu))]
            distancia = sum(aux)
            if distancia < epsilon * epsilon:
                print("Distancia_T: " + str(distancia))
                return True
            else:
                print("Distancia_F: " + str(distancia))
                return False
        else:
            # detencion pq se ha alcanzado el maximo de iteraciones
            return True


def init_random(dim, k):
    from numpy import random
    random.seed(5)
    # ind = random.randint(0, len(X) - 1)
    m = np.array([random.random(dim) for _ in range(k)])
    # return random.sample(X[ind], k)
    return m


@task(returns=list, tracing_hook=False)
def genFragment(numv, dim):
    import pyextrae.multiprocessing as pyextrae
    # if mode == "gauss":
    #    return init_board_gauss(numv, dim, k)
    # else:
    pyextrae.eventandcounters(USER_EVENTS, GENERATE)
    frag = init_board_random(numv, dim)
    pyextrae.eventandcounters(USER_EVENTS, 0)
    return frag


def kmeans_frag(numV, k, dim, epsilon, maxIterations, numFrag):
    from pycompss.api.api import compss_wait_on
    import time
    size = int(numV / numFrag)

    startTime = time.time()
    X = [genFragment(size, dim) for _ in range(numFrag)]
    print("Points generation Time {} (s)".format(time.time() - startTime))

    mu = init_random(dim, k)
    oldmu = []
    n = 0
    startTime = time.time()
    while not has_converged(mu, oldmu, epsilon, n, maxIterations):
        oldmu = mu
        clusters = [cluster_points_partial(
            X[f], mu, f * size) for f in range(numFrag)]
        partialResult = [partial_sum(
            X[f], clusters[f], f * size) for f in range(numFrag)]

        mu = merge_reduce(reduceCentersTask, partialResult)
        mu = compss_wait_on(mu)
        mu = [mu[c][1] / mu[c][0] for c in mu]
        print(mu)
        n += 1
    print("Kmeans Time {} (s)".format(time.time() - startTime))
    return (n, mu)


if __name__ == "__main__":
    import time
    import numpy as np

    numV = 100
    dim = 2
    k = 2
    numFrag = 2

    startTime = time.time()
    result = kmeans_frag(numV, k, dim, 1e-4, 10, numFrag)
    print("Elapsed Time {} (s)".format(time.time() - startTime))
