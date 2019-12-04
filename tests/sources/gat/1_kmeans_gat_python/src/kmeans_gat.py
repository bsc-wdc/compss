#!/usr/bin/python
# -*- coding: utf-8 -*-
#
#  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
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
__copyright__ = '2018 Barcelona Supercomputing Center (BSC-CNS)'

import numpy as np
import time
import unittest
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task


def parse_arguments():
    """
    Parse command line arguments. Make the program generate
    a help message in case of wrong usage.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='An MPI Kmeans implementation.')
    parser.add_argument('-s', '--seed', type=int, default=0,
                        help='Pseudo-random seed. Default = 0'
                        )
    parser.add_argument('-n', '--num_points', type=int, default=100,
                        help='Number of points. Default = 100'
                        )
    parser.add_argument('-d', '--dimensions', type=int, default=2,
                        help='Number of dimensions. Default = 2'
                        )
    parser.add_argument('-c', '--num_centers', type=int, default=2,
                        help='Number of centers'
                        )
    parser.add_argument('-i', '--max_iterations', type=int, default=6,
                        help='Maximum number of iterations'
                        )
    parser.add_argument('-e', '--epsilon', type=float, default=1e-9,
                        help='Epsilon. Kmeans will stop when |old - new| < epsilon.'
                        )
    parser.add_argument('--num_fragments', type=int, default=4,
                        help='Total number of fragments')
    parser.add_argument('--distributed_read', action='store_true',
                        help='Boolean indicating if data should be read distributed.'
                        )

    return parser.parse_args()


# Data generation functions

def init_board_random(num_points, dim, seed):
    np.random.seed(seed)
    return np.random.random((num_points, dim))


def init_centers_random(dim, k, seed):
    np.random.seed(seed)
    m = np.random.random((k, dim))
    return m


@task(returns=list)
def generate_fragment(numv, dim, seed):
    return init_board_random(numv, dim, seed)


def generate_data(num_points, num_fragments, dimensions, seed,
                  distributed_read):
    startTime = time.time()

    size = int(num_points / num_fragments)

    if distributed_read:
        X = [generate_fragment(size, dimensions, seed + i) for i in
             range(num_fragments)]
    else:
        X = [init_board_random(size, dimensions, seed + i) for i in
             range(num_fragments)]
    print("Points generation Time {} (s)".format(time.time() - startTime))

    return X


# PyCOMPSs auxiliar functions
@task(returns=dict)
def merge_reduce_task(*data):
    reduce_value = data[0]
    for i in range(1, len(data)):
        reduce_value = reduce_centers(reduce_value, data[i])
    return reduce_value


def merge_reduce(data, chunk=50):
    while len(data) > 1:
        dataToReduce = data[:chunk]
        data = data[chunk:]
        data.append(merge_reduce_task(*dataToReduce))
    return data[0]


# Main implementation functions

@task(returns=dict)
def cluster_points_sum(fragment_points, centers, ind):
    """
    For each point computes the nearest center.
    :param fragment_points: list of points of a fragment
    :param centers: current centers
    :param ind: original index of the first point in the fragment
    :return: {centers_ind: [pointInd_i, ..., pointInd_n]}
    """
    center2points = {c: [] for c in range(0, len(centers))}
    for x in enumerate(fragment_points):
        closest_center = min([(i[0], np.linalg.norm(x[1] - centers[i[0]]))
                              for i in enumerate(centers)], key=lambda t: t[1])[
            0]
        center2points[closest_center].append(x[0] + ind)
    return partial_sum(fragment_points, center2points, ind)


def partial_sum(fragment_points, clusters, ind):
    """
    For each cluster returns the number of points and the sum of all the
    points that belong to the cluster.
    :param fragment_points: points
    :param clusters: partial cluster {mu_ind: [pointInd_i, ..., pointInd_n]}
    :param ind: point first ind
    :return: {cluster_ind: (#points, sum(points))}
    """
    dic = {}
    for i in clusters:
        p_idx = np.array(clusters[i]) - ind
        dic[i] = (len(p_idx), np.sum(fragment_points[p_idx], axis=0))
    return dic


def reduce_centers(a, b):
    """
    Reduce method to sum the result of two partial_sum methods
    :param a: partial_sum {cluster_ind: (#points_a, sum(points_a))}
    :param b: partial_sum {cluster_ind: (#points_b, sum(points_b))}
    :return: {cluster_ind: (#points_a+#points_b, sum(points_a+points_b))}
    """

    for key in b:
        if key not in a:
            a[key] = b[key]
        else:
            a[key] = (a[key][0] + b[key][0], a[key][1] + b[key][1])
    return a


def has_converged(mu, oldmu, epsilon, iter, max_iterations):
    if len(oldmu) > 0:
        if iter < max_iterations:
            aux = [np.linalg.norm(oldmu[i] - mu[i]) for i in range(len(mu))]
            distancia = sum(aux)
            if distancia < epsilon * epsilon:
                return True
            else:
                return False
        else:
            return True
    else:
        # Only empty on the first check
        return False


def kmeans_frag(X, num_points, num_centers, dimensions, epsilon, max_iterations,
                num_fragments, seed):
    size = int(num_points / num_fragments)
    mu = init_centers_random(dimensions, num_centers, seed)
    oldmu = []
    it = 0
    startTime = time.time()

    while not has_converged(mu, oldmu, epsilon, it, max_iterations):
        oldmu = mu
        partialResult = []
        # clusters = []
        for f in range(num_fragments):
            partialResult.append(cluster_points_sum(X[f], mu, f * size))

        mu = merge_reduce(partialResult, chunk=50)
        mu = compss_wait_on(mu)
        mu = [mu[c][1] / mu[c][0] for c in mu]

        it += 1
        print("Iteration Time {} (s)".format(time.time() - startTime))
    print("Kmeans Time {} (s)".format(time.time() - startTime))

    return mu


class KmeansTest(unittest.TestCase):
    def test_kmeans(self):
        args = parse_arguments()

        print("Execution arguments:\n%s" % args)
        t0 = time.time()

        X = generate_data(num_points=args.num_points,
                          num_fragments=args.num_fragments,
                          dimensions=args.dimensions,
                          seed=args.seed,
                          distributed_read=args.distributed_read)

        centers = kmeans_frag(X=X,
                              num_points=args.num_points,
                              num_centers=args.num_centers,
                              dimensions=args.dimensions,
                              epsilon=args.epsilon,
                              max_iterations=args.max_iterations,
                              num_fragments=args.num_fragments,
                              seed=args.seed)

        t1 = time.time()
        print("Total elapsed time: %s" % (t1 - t0))

        expected_centers = [np.array([0.56849316, 0.70270945]),
                            np.array([0.3036273, 0.31187001])]

        if not np.allclose(centers, expected_centers):
            print("Kmeans centers results are incorrect.")
            print("Expected: \n %s" % expected_centers)
            print("Got: \n %s" % centers)
        self.assertTrue(np.allclose(centers, expected_centers))


if __name__ == '__main__':
    unittest.main()
