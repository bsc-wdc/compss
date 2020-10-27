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

import sys
import time
from random import Random

from pycompss.api.api import compss_barrier
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


def transitive_closure():

    # path = sys.argv[1]
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


# ################################### #
#             UNITTESTS               #
# ################################### #

def test_pi_estimation():
    pi_estimation()


def test_word_count():
    word_count()


def test_terasort():
    terasort()


def test_inverted_indexing():
    inverted_indexing()


def test_transitive_closure():
    transitive_closure()
