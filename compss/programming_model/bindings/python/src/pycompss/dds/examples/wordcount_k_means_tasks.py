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
PyCOMPSs DDS - examples - wordcount k-means tasks.

This file contains the DDS tasks example.
"""

from collections import Counter
import numpy as np
from pycompss.api.parameter import COLLECTION_IN

from pycompss.api.task import task


@task(returns=dict, xp=COLLECTION_IN)
def cluster_points_partial(xp, mu, ind):
    """Measure the distance from the centers to the given points.

    :param xp: Points.
    :param mu: Centers.
    :param ind: Offset.
    :returns: Dictionary with the distances.
    """
    dic = {}
    for x in enumerate(xp):
        bestmukey = min(
            [(i[0], np.linalg.norm(x[1] - mu[i[0]])) for i in enumerate(mu)],
            key=lambda t: t[1],
        )[0]

        if bestmukey not in dic:
            dic[bestmukey] = [x[0] + ind]
        else:
            dic[bestmukey].append(x[0] + ind)

    return dic


@task(returns=dict, xp=COLLECTION_IN)
def partial_sum(xp, clusters, ind):
    """Accumulates the distances.

    :param xp: Points.
    :param clusters: Clusters (points associated to each cluster).
    :param ind: Offset.
    :returns: Dictionary with the accumulated distance.
    """
    p = [(i, [(xp[j - ind]) for j in clusters[i]]) for i in clusters]
    dic = {}
    for i, l in p:
        dic[i] = (len(l), np.sum(l, axis=0))
    return dic


@task()
def task_count_locally(file_path, vocab):
    """Task count task.

    :param file_path: Input file.
    :param vocab: Words filter.
    :returns: np array with the appearances.
    """
    # read the file
    with open(file_path) as file_path_fd:  # pylint: disable=W1514
        text = file_path_fd.read()

    filtered_words = [word for word in text.split() if word.isalnum()]
    cnt = Counter(filtered_words)

    for _word in vocab.keys():
        if _word not in cnt:
            cnt[_word] = 0

    values = [int(v) for k, v in sorted(cnt.items())]
    return np.array(values)


# dict inout??
@task(returns=dict, priority=True)
def reduce_centers(a, b):
    """Reduce centers.

    :param a: First dictionary.
    :param b: Second dictionary.
    :results: Updated a
    """
    for key in b:
        if key not in a:
            a[key] = b[key]
        else:
            a[key] = (a[key][0] + b[key][0], a[key][1] + b[key][1])
    return a


@task(returns=list)
def get_similar_files(fayl, cluster, threshold=0.90):
    """Calculate average similarity of a file against a list of files.

    :param threshold: Threshold level.
    :param fayl: File to be compared with its cluster.
    :param cluster: File names to be compared with the file.
    :return: Average similarity.
    """
    import spacy  # pylint: disable=import-outside-toplevel

    nlp = spacy.load("en_core_web_sm")

    with open(fayl) as fayl_fd:  # pylint: disable=W1514
        d1 = nlp(fayl_fd.read())
    ret = []

    for other in cluster:
        if other == fayl:
            continue
        with open(other) as other_fd:  # pylint: disable=W1514
            d2 = nlp(other_fd.read())
        s = d1.similarity(d2)
        if s >= threshold:
            ret.append((other, s))
    return ret
