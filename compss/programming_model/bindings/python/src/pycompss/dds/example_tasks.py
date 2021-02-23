#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import numpy as np
from pycompss.api.parameter import COLLECTION_IN

from pycompss.api.task import task


@task(returns=dict, xp=COLLECTION_IN)
def cluster_points_partial(xp, mu, ind):
    dic = {}
    for x in enumerate(xp):

        bestmukey = min([(i[0], np.linalg.norm(x[1] - mu[i[0]]))
                         for i in enumerate(mu)], key=lambda t: t[1])[0]

        if bestmukey not in dic:
            dic[bestmukey] = [x[0] + ind]
        else:
            dic[bestmukey].append(x[0] + ind)

    return dic


@task(returns=dict, xp=COLLECTION_IN)
def partial_sum(xp, clusters, ind):
    p = [(i, [(xp[j - ind]) for j in clusters[i]]) for i in clusters]
    dic = {}
    for i, l in p:
        dic[i] = (len(l), np.sum(l, axis=0))
    return dic


@task()
def task_count_locally(file_path, vocab):
    from collections import Counter
    import numpy as np

    # read the file
    text = open(file_path).read()

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
    for key in b:
        if key not in a:
            a[key] = b[key]
        else:
            a[key] = (a[key][0] + b[key][0], a[key][1] + b[key][1])
    return a


@task(returns=list)
def get_similar_files(fayl, cluster, threshold=0.90):
    """
    Calculate average similarity of a file againt a list of files
    :param threshold:
    :param fayl: file to be compared with its cluster
    :param cluster: file names to be compared with the file
    :return: average similarity
    """
    import spacy
    nlp = spacy.load("en_core_web_sm")

    d1 = nlp(open(fayl).read())
    ret = []

    for other in cluster:
        if other == fayl:
            continue
        d2 = nlp(open(other).read())
        s = d1.similarity(d2)
        if s >= threshold:
            ret.append((other, s))
    return ret
