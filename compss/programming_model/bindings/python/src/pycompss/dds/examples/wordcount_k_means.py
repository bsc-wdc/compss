#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs DDS - Examples - wordcount k-means.

This file contains the DDS wordcount k-means example.
"""

import os
import time
import numpy as np
from collections import deque
from collections import defaultdict

from pycompss.api.api import compss_wait_on
from pycompss.dds import DDS
from pycompss.dds.examples.word_count import create_dataset
from pycompss.dds.examples.word_count import clean_dataset
from pycompss.dds.examples.wordcount_k_means_tasks import (
    cluster_points_partial,
)
from pycompss.dds.examples.wordcount_k_means_tasks import partial_sum
from pycompss.dds.examples.wordcount_k_means_tasks import task_count_locally
from pycompss.dds.examples.wordcount_k_means_tasks import reduce_centers
from pycompss.dds.examples.wordcount_k_means_tasks import get_similar_files


def has_converged(mu, old_mu, epsilon):
    """Check if Kmeans has converged.

    Checks the distance difference between centers and old centers and
    compares with epsilon.

    :param mu: Centers.
    :param old_mu: Old centers.
    :param epsilon: Epsilon.
    :return: If has converged.
    """
    if not old_mu:
        return False

    aux = [np.linalg.norm(old_mu[i] - mu[i]) for i in range(len(mu))]
    distance = sum(aux)
    print("Distance_T: " + str(distance))
    return distance < (epsilon**2)


def merge_reduce(function, data):
    """Merge reducing as binary tree.

    :param function: Merging function.
    :param data: List of elements to reduce.
    :results: Reduced result.
    """
    queue = deque(list(range(len(data))))
    while len(queue):
        elem_x = queue.popleft()
        if len(queue):
            elem_y = queue.popleft()
            data[elem_x] = function(data[elem_x], data[elem_y])
            queue.append(elem_x)
        else:
            return data[elem_x]


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


def check_results(results) -> bool:
    """Check if the given results match the expected result.

    CAUTION: Only works for the dummy dataset.

    :param results: Dictionary containing the words and their appearance.
    :return: If the result is the expected or not.
    """
    expected = [
        [
            "file_0.txt",
            [
                ("file_1.txt", 0.9764065991188053),
                ("file_2.txt", 0.973527551367599),
                ("file_3.txt", 0.9737561884661387),
            ],
        ],
        [
            "file_1.txt",
            [
                ("file_0.txt", 0.9764065991188053),
                ("file_2.txt", 0.9955979819385568),
                ("file_3.txt", 0.9946889892919744),
            ],
        ],
        [
            "file_2.txt",
            [
                ("file_0.txt", 0.973527551367599),
                ("file_1.txt", 0.9955979819385568),
                ("file_3.txt", 0.9947750570357584),
            ],
        ],
        [
            "file_3.txt",
            [
                ("file_0.txt", 0.9737561884661387),
                ("file_1.txt", 0.9946889892919744),
                ("file_2.txt", 0.9947750570357584),
            ],
        ],
    ]
    return results == expected


def wordcount_k_means(dim=16):
    """Wordcount K-means.

    The number of dimensions corresponds to: dim = len(vocabulary)

    :param dim: Dimensions.
    :return: The wordcount k-means evaluation check.
    """
    print("--- WORDCOUNT K-MEANS ---")
    np.random.seed(1)

    # By default, create a dummy dataset and perform wordcount over it.
    # It could be changed to:
    #   path_file = sys.argv[1]
    # if you desire to perform the word count over a given dataset
    # (remember to comment the check_results call in this case).
    path_file = create_dataset()
    start_time = time.time()

    vocab = (
        DDS()
        .load_files_from_dir(path_file, num_of_parts=4)
        .flat_map(lambda x: x[1].split())
        .map(lambda x: "".join(e for e in x if e.isalnum()))
        .count_by_value(arity=2, as_dict=True, as_fo=True)
    )

    total = len(os.listdir(path_file))
    max_iter = 2
    frags = 4
    epsilon = 1e-10
    size = total / frags
    k = 4

    # to access file names by index returned from the clusters.
    # load_files_from_list will also sort them alphabetically
    indexes = [
        os.path.join(path_file, f) for f in sorted(os.listdir(path_file))
    ]

    # step 2
    # wc_per_file = DDS().load_files_from_dir(files_path, num_of_parts=frags)\
    #     .map(__count_locally__, vocabulary)\
    #     .map(__gen_array__)\

    wc_per_file = []

    for file_name in sorted(os.listdir(path_file)):
        wc_per_file.append(
            task_count_locally(os.path.join(path_file, file_name), vocab)
        )

    mu = [np.random.randint(1, 3, dim) for _ in range(frags)]

    old_mu = []
    clusters = []
    iteration = 0

    while iteration < max_iter and not has_converged(mu, old_mu, epsilon):
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
        mu = compss_wait_on(mu)
        mu = [mu[c][1] / mu[c][0] for c in mu]
        while len(mu) < k:
            # Add a new random center if one of the centers has no points.
            mu.append(np.random.randint(1, 3, dim))
        iteration += 1

    clusters_with_frag = compss_wait_on(clusters)
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

    sims_per_file = compss_wait_on(sims_per_file)

    results = []
    for file_path in list(sims_per_file.keys())[:10]:
        file_name = os.path.basename(file_path)
        sims = []
        for sim in sims_per_file[file_path][:5]:
            sims.append((os.path.basename(sim[0]), sim[1]))
        results.append([file_name, sims])
        print(f"- {file_name} --- sims --> {sims}")

    print(f"- Iterations: {iteration}")
    print(f"- Elapsed Time: {time.time() - start_time} (s)")
    print("-------------------------")

    clean_dataset(path_file)

    return check_results(results)
