#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs DDS - Examples.

This file contains the DDS examples.
"""

import os
import sys
import time
import random
from collections import deque
from collections import defaultdict
import numpy as np

from pycompss.api.api import compss_wait_on as cwo
from pycompss.dds import DDS
from pycompss.dds.example_tasks import cluster_points_partial
from pycompss.dds.example_tasks import partial_sum
from pycompss.dds.example_tasks import task_count_locally
from pycompss.dds.example_tasks import reduce_centers
from pycompss.dds.example_tasks import get_similar_files


def inside(_):
    """Check if inside.

    :returns: If inside.
    """
    rand_x = random.random()
    rand_y = random.random()
    return (rand_x * rand_x) + (rand_y * rand_y) < 1


def _generate_graph():
    """Generate graph.

    :returns: Set of edges.
    """
    num_edges = 10
    num_vertices = 5
    rand = random.Random(42)

    edges = set()
    while len(edges) < num_edges:
        src = rand.randrange(0, num_vertices)
        dst = rand.randrange(0, num_vertices)
        if src != dst:
            edges.add((src, dst))
    return edges


def files_to_pairs(element):
    """Pair files.

    :param element: String of elements.
    :returns: List of pairs.
    """
    tuples = []
    lines = element[1].split("\n")
    for _l in lines:
        if not _l:
            continue
        k_v = _l.split(",")
        tuples.append(tuple(k_v))

    return tuples


def _invert_files(pair):
    """Invert files.

    :param pair: Pair.
    :results: List of items.
    """
    res = {}
    for word in pair[1].split():
        res[word] = [pair[0]]
    return list(res.items())


def has_converged(mu, old_mu, epsilon):
    """Check if Kmeans has converged.

    Checks the distance difference between centers and old centers and
    compares with epsilon.

    :param mu: Centers.
    :param old_mu: Old centers.
    :param epsilon: Epsilon.
    :returns: If has converged.
    """
    if not old_mu:
        return False

    aux = [np.linalg.norm(old_mu[i] - mu[i]) for i in range(len(mu))]
    distance = sum(aux)
    print("Distance_T: " + str(distance))
    return distance < (epsilon**2)


def merge_reduce(f, data):
    """Merge reducing as binary tree.

    :param f: Merging function.
    :param data: List of elements to reduce.
    :results: Reduced result.
    """
    q = deque(list(range(len(data))))
    while len(q):
        x = q.popleft()
        if len(q):
            y = q.popleft()
            data[x] = f(data[x], data[y])
            q.append(x)
        else:
            return data[x]


# # Used only in step 2 of wordcount k-means
# def __count_locally__(element):
#     from collections import Counter
#     file_name, text = element
#
#     filtered_words = [word for word in text.split() if word.isalnum()]
#     cnt = Counter(filtered_words)
#
#     for _word in vocab.keys():
#         if _word not in cnt:
#             cnt[_word] = 0
#
#     return file_name, sorted(cnt.items())
#
#
# # Used only in step 2 of wordcount k-means
# def __gen_array__(element):
#     import numpy as np
#     values = [int(v) for k, v in element[1]]
#     return np.array(values)


def wordcount_k_means(dim=742):
    """Wordcount Kmeans.

    TODO: Missing documentation.

    :param dim:
    :returns:
    """
    f_path = sys.argv[1]

    start_time = time.time()

    vocab = (
        DDS()
        .load_files_from_dir(f_path, num_of_parts=4)
        .flat_map(lambda x: x[1].split())
        .map(lambda x: "".join(e for e in x if e.isalnum()))
        .count_by_value(arity=2, as_dict=True, as_fo=True)
    )

    total = len(os.listdir(f_path))
    max_iter = 2
    frags = 4
    epsilon = 1e-10
    size = total / frags
    k = 4
    # The number of dimensions corresponds to: dim = len(vocabulary)
    # dim = 742  # added as parameter to allow unittests with different dataset

    # to access file names by index returned from the clusters..
    # load_files_from_list will also sort them alphabetically
    indexes = [os.path.join(f_path, f) for f in sorted(os.listdir(f_path))]

    # step 2
    # wc_per_file = DDS().load_files_from_dir(files_path, num_of_parts=frags)\
    #     .map(__count_locally__, vocabulary)\
    #     .map(__gen_array__)\

    wc_per_file = []

    for fn in sorted(os.listdir(f_path)):
        wc_per_file.append(task_count_locally(os.path.join(f_path, fn), vocab))

    mu = [np.random.randint(1, 3, dim) for _ in range(frags)]

    old_mu = []
    clusters = []
    n = 0

    while n < max_iter and not has_converged(mu, old_mu, epsilon):
        old_mu = mu
        clusters = [
            cluster_points_partial([wc_per_file[f]], mu, int(f * size))
            for f in range(frags)
        ]
        partial_result = [
            partial_sum([wc_per_file[f]], clusters[f], int(f * size))
            for f in range(frags)
        ]
        mu = merge_reduce(reduce_centers, partial_result)
        mu = cwo(mu)
        mu = [mu[c][1] / mu[c][0] for c in mu]
        while len(mu) < k:
            # Add a new random center if one of the centers has no points.
            mu.append(np.random.randint(1, 3, dim))
        n += 1

    clusters_with_frag = cwo(clusters)
    cluster_sets = defaultdict(list)

    for _d in clusters_with_frag:
        for _k in _d:
            cluster_sets[_k] += [indexes[i] for i in _d[_k]]

    # step 4 and 5 combined
    sims_per_file = {}

    for k in cluster_sets:
        clus = cluster_sets[k]
        for fayl in clus:
            sims_per_file[fayl] = get_similar_files(fayl, clus)

    sims_per_file = cwo(sims_per_file)

    for k in list(sims_per_file.keys())[:10]:
        print(k, "-----------sims --------->", sims_per_file[k][:5])

    print("-----------------------------")
    elapsed_time = time.time() - start_time
    print(f"Kmeans Timed {elapsed_time} (s)")
    print("Iterations: ", n)


def word_count():
    """Word count.

    TODO: Missing documentation

    :results:
    """
    path_file = sys.argv[1]
    start = time.time()

    results = (
        DDS()
        .load_files_from_dir(path_file)
        .flat_map(lambda x: x[1].split())
        .map(lambda x: "".join(e for e in x if e.isalnum()))
        .count_by_value(as_dict=True)
    )

    print("Results: " + str(results))
    print("Elapsed Time: ", time.time() - start)


def pi_estimation():
    """Pi estimation.

    Example is taken from: https://spark.apache.org/examples.html

    :returns: The estimated pi value.
    """
    print("Estimating Pi by 'throwing darts' algorithm.")
    tries = 100000
    print(f"Number of tries: {tries}")

    count = DDS().load(range(0, tries), 10).filter(inside).count()
    rough_pi = 4.0 * count / tries
    print(f"Pi is roughly {rough_pi}")


def terasort():
    """Terasort.

    TODO: Missing documentation

    :returns: Sorting result.
    """
    dir_path = sys.argv[1]
    dest_path = sys.argv[2]
    # Commented out code for unknown reason:
    # partitions = sys.argv[2] if len(sys.argv) > 2 else -1

    start_time = time.time()

    dds = (
        DDS()
        .load_files_from_dir(dir_path)
        .flat_map(files_to_pairs)
        .sort_by_key()
        .save_as_text_file(dest_path)
    )

    # Commented out code for unknown reason:
    # compss_barrier()
    # test = DDS().load_pickle_files(dest_path).map(lambda x: x).collect()
    # print(test[-1:])

    print(f"Result: {str(dds)}")
    elapsed_time = time.time() - start_time
    print(f"Elapsed Time {elapsed_time} (s)")


def inverted_indexing():
    """Inverted indexing.

    TODO: Missing documentation

    :results: Inverted indexing result.
    """
    path = sys.argv[1]
    start_time = time.time()
    result = (
        DDS()
        .load_files_from_dir(path)
        .flat_map(_invert_files)
        .reduce_by_key(lambda a, b: a + b)
        .collect()
    )
    print(result[-1:])
    elapsed_time = time.time() - start_time
    print(f"Elapsed Time {elapsed_time} (s)")


def transitive_closure(partitions=None):
    """Transitive closure.

    TODO: Missing documentation

    :param partitions:
    :results: Transitive closure result.
    """
    if not partitions:
        partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    # Commented out code for unknown reason:
    # path = sys.argv[1]
    # od = DDS().load_text_file(path, partitions) \
    #     .map(lambda line: (int(line.split(",")[0]), int(line.split(",")[1])))\
    #     .collect(future_objects=True)
    edges = _generate_graph()
    od = DDS().load(edges, partitions).collect(future_objects=True)

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = DDS().load(od, -1).map(lambda x_y: (x_y[1], x_y[0]))

    next_count = DDS().load(od, -1).count()

    while True:
        old_count = next_count
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = (
            DDS().load(od, -1).join(edges).map(lambda __a_b: (__a_b[1][1], __a_b[1][0]))
        )
        od = DDS().load(od, -1).union(new_edges).distinct().collect(future_objects=True)

        next_count = DDS().load(od, -1).count()

        if next_count == old_count:
            break

    print(f"TC has {next_count} edges")


def main_program():
    """Run all examples.

    TODO: Missing documentation

    :returns: None.
    """
    print("________RUNNING EXAMPLES_________")
    # Available examples:
    # pi_estimation()
    # word_count()
    # terasort()
    # inverted_indexing()
    # transitive_closure()
    wordcount_k_means()


if __name__ == "__main__":
    main_program()
