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

import os, sys
import time
import numpy as np

from random import Random

from pycompss.api.api import compss_barrier as cb, compss_wait_on as cwo
from pycompss.api.parameter import FILE_IN, COLLECTION_FILE_IN
from pycompss.api.task import task
from pycompss.dds import DDS


def inside(_):
    import random
    x, y = random.random(), random.random()  # NOSONAR
    if (x * x) + (y * y) < 1:
        return True


def _generate_graph():
    num_edges = 10
    num_vertices = 5
    rand = Random(42)

    edges = set()
    while len(edges) < num_edges:
        src = rand.randrange(0, num_vertices)
        dst = rand.randrange(0, num_vertices)
        if src != dst:
            edges.add((src, dst))
    return edges


def files_to_pairs(element):
    tuples = list()
    lines = element[1].split("\n")
    for _l in lines:
        if not _l:
            continue
        k_v = _l.split(",")
        tuples.append(tuple(k_v))

    return tuples


def _invert_files(pair):
    res = dict()
    for word in pair[1].split():
        res[word] = [pair[0]]
    return list(res.items())


def has_converged(mu, old_mu, epsilon):

    if not old_mu:
        return False

    aux = [np.linalg.norm(old_mu[i] - mu[i]) for i in range(len(mu))]
    distance = sum(aux)
    print("Distance_T: " + str(distance))
    return distance < (epsilon ** 2)


@task(returns=dict)
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


@task(returns=dict)
def partial_sum(xp, clusters, ind):
    p = [(i, [(xp[j - ind]) for j in clusters[i]]) for i in clusters]
    dic = {}
    for i, l in p:
        dic[i] = (len(l), np.sum(l, axis=0))
    return dic


# dict inout??
@task(returns=dict, priority=True)
def reduce_centers(a, b):
    for key in b:
        if key not in a:
            a[key] = b[key]
        else:
            a[key] = (a[key][0] + b[key][0], a[key][1] + b[key][1])
    return a


def calculate_avg_similarity(fayl, clusterr):
    """
    Calculate average similarity of a file againt a list of files
    :param fayl: file to be compared with its cluster
    :param cluster: file names to be compared with the file
    :return: average similarity
    """
    import spacy
    nlp = spacy.load("en_core_web_sm")

    d1 = nlp(unicode(open(fayl).read()))
    total = 0

    for other in cluster:
        if other == fayl:
            continue
        d2 = nlp(unicode(open(other).read()))
        total += d1.similarity(d2)

    return total / len(cluster)


def merge_reduce(f, data):
    from collections import deque
    q = deque(list(range(len(data))))
    while len(q):
        x = q.popleft()
        if len(q):
            y = q.popleft()
            data[x] = f(data[x], data[y])
            q.append(x)
        else:
            return data[x]


def plot_k_means(dim, mu, clusters, data):
    import pylab as plt
    colors = ['b','g','r','c','m','y','k']
    if dim == 2:
        from matplotlib.patches import Circle
        from matplotlib.collections import PatchCollection
        fig, ax = plt.subplots(figsize=(10, 10))
        patches = []
        pcolors = []
        for i in range(len(clusters)):
            for key in clusters[i].keys():
                d = clusters[i][key]
                for j in d:
                    j = j - i * len(data[0])
                    C = Circle((data[i][j][0], data[i][j][1]), .05)
                    pcolors.append(colors[key])
                    patches.append(C)
        collection = PatchCollection(patches)
        collection.set_facecolor(pcolors)
        ax.add_collection(collection)

        # todo: check this
        x, y = mu

        plt.plot(x, y, '*', c='y', markersize=20)
        plt.autoscale(enable=True, axis='both', tight=False)
        plt.show()

    elif dim == 3:
        from mpl_toolkits.mplot3d import Axes3D
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        for i in range(len(clusters)):
            for key in clusters[i].keys():
                d = clusters[i][key]
                for j in d:
                    j = j - i * len(data[0])
                    ax.scatter(data[i][j][0], data[i][j][1], data[i][j][2], 'o', c=colors[key])
        x, y, z = zip(*mu)
        for i in range(len(mu)):
            ax.scatter(x[i], y[i], z[i], s=80, c='y', marker='D')
        plt.show()

    else:
        print("No representable dim")


def complex_wc():
    files_path = sys.argv[1]

    startTime = time.time()

    # (key, value) pairs
    total_wc_dict = DDS().load_files_from_dir(files_path).\
        map_and_flatten(lambda x: x[1].split()) \
        .map(lambda x: ''.join(e for e in x if e.isalnum())) \
        .distinct().collect()

    def count_locally(element):
        from collections import Counter
        file_name, text = element

        filtered_words = [word for word in text.split() if word.isalnum()]
        cnt = Counter(filtered_words)

        for _word in total_wc_dict:
            if _word not in cnt:
                cnt[_word] = 0

        return file_name, sorted(cnt.items())

    def gen_array(element):
        import numpy as np
        values = [int(v) for k, v in element[1]]
        return np.array(values)

    total = len(os.listdir(files_path))
    max_iter = 10
    frags = 2
    epsilon = 1e-10
    size = int(total / frags)
    k = 2
    dim = 2

    # X
    # dict ( index_in_wc_per_file : file_name )

    # to acces file names by index returned from the clusters..
    # load_files_from_list will also sort them alphabetically
    indexes = [os.path.join(files_path, f)
               for f in sorted(os.listdir(files_path))]

    wc_per_file = DDS().load_files_from_dir(files_path, num_of_parts=frags)\
        .map(count_locally)\
        .map(gen_array)\
        .collect(keep_partitions=True)

    mu = wc_per_file[:2]

    old_mu = []
    clusters = []
    n = 0

    while n < max_iter and not has_converged(mu, old_mu, epsilon):
        old_mu = mu
        clusters = [cluster_points_partial(wc_per_file[f], mu, f * size)
                    for f in range(frags)]

        partial_result = [partial_sum(wc_per_file[f], clusters[f], f * size)
                          for f in range(frags)]

        mu = merge_reduce(reduce_centers, partial_result)

        mu = cwo(mu)

        mu = [mu[c][1] / mu[c][0] for c in mu]

        while len(mu) < k:
            # Add new random center if one of the centers has no points.
            print("______ adding a new point..")
            ind_p = np.random.randint(0, size)
            ind_f = np.random.randint(0, frags)
            mu.append(wc_per_file[ind_f][ind_p])

        n += 1

    clusters_with_frag = cwo(clusters)

    from collections import defaultdict
    cluster_sets = defaultdict(list)

    for _d in clusters_with_frag:
        for _k in _d:
            cluster_sets[_k] += [indexes[i] for i in _d[_k]]

    sims_per_file = {}

    for k in cluster_sets:
        clus = cluster_sets[k]
        for fayl in clus:
            sims_per_file[fayl] = calculate_avg_similarity(fayl, clus)

    import pdb; pdb.set_trace()

    print("-----------------------------")
    print("Kmeans Time {} (s)".format(time.time() - startTime))
    print("-----------------------------")
    print("Result:")
    print("Iterations: ", n)
    print("Centers: ", mu)
    # plot_k_means(dim, mu, clusters, wc_per_file)


def word_count():

    path_file = sys.argv[1]
    start = time.time()

    results = DDS().load_files_from_dir(path_file).\
        map_and_flatten(lambda x: x[1].split()) \
        .map(lambda x: ''.join(e for e in x if e.isalnum())) \
        .count_by_value(arity=4, as_dict=True)

    print("Results: " + str(results))
    print("Elapsed Time: ", time.time()-start)


def pi_estimation():
    """
    Example is taken from: https://spark.apache.org/examples.html
    """
    print("Estimating Pi by 'throwing darts' algorithm.")
    tries = 100000
    print("Number of tries: {}".format(tries))

    count = DDS().load(range(0, tries), 10) \
        .filter(inside).count()
    print("Pi is roughly %f" % (4.0 * count / tries))


def terasort():
    """
    """

    dir_path = sys.argv[1]
    dest_path = sys.argv[2]
    # partitions = sys.argv[2] if len(sys.argv) > 2 else -1

    start_time = time.time()

    dds = DDS().load_files_from_dir(dir_path) \
        .map_and_flatten(files_to_pairs) \
        .sort_by_key().save_as_text_file(dest_path)

    # compss_barrier()
    # test = DDS().load_pickle_files(dest_path).map(lambda x: x).collect()
    # print(test[-1:])

    print("Result: " + str(dds))
    print("Elapsed Time {} (s)".format(time.time() - start_time))


def inverted_indexing():

    path = sys.argv[1]
    start_time = time.time()
    result = DDS().load_files_from_dir(path).map_and_flatten(_invert_files)\
        .reduce_by_key(lambda a, b: a + b).collect()
    print(result[-1:])
    print("Elapsed Time {} (s)".format(time.time() - start_time))


def transitive_closure(partitions=None):

    # path = sys.argv[1]
    if not partitions:
        partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    #
    # od = DDS().load_text_file(path, partitions) \
    #     .map(lambda line: (int(line.split(",")[0]), int(line.split(",")[1])))\
    #     .collect(future_objects=True)
    edges = _generate_graph()
    od = DDS().load(edges, partitions).collect(future_objects=True)

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = DDS().load(od, -1).map(lambda x_y: (x_y[1], x_y[0]))

    old_count = 0
    next_count = DDS().load(od, -1).count()

    while True:
        old_count = next_count
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = DDS().load(od, -1).join(edges)\
            .map(lambda __a_b: (__a_b[1][1], __a_b[1][0]))
        od = DDS().load(od, -1).union(new_edges).distinct()\
            .collect(future_objects=True)

        next_count = DDS().load(od, -1).count()

        if next_count == old_count:
            break

    print("TC has %i edges" % next_count)


def main_program():
    print("________RUNNING EXAMPLES_________")
    # pi_estimation()
    # word_count()
    # terasort()
    # inverted_indexing()
    # transitive_closure()
    complex_wc()


if __name__ == '__main__':
    main_program()
