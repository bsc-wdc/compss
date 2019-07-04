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

from pycompss.dds.new_dds import DDS


def inside(_):
    import random
    x, y = random.random(), random.random()
    if (x * x) + (y * y) < 1:
        return True


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
        map_and_flatten(lambda x: x[1].split())\
        .count_by_value(arity=4, as_dict=True)

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
    partitions = sys.argv[2] if len(sys.argv) > 2 else -1

    start_time = time.time()

    dds = DDS().load_files_from_dir(dir_path, partitions)\
        .map_and_flatten(files_to_pairs)\
        .sort_by_key().collect()

    temp = 0
    for i, k in dds:
        if i < temp:
            print("FAILED")
            break
        temp = i
    print(dds[-1:])
    print("Elapsed Time {} (s)".format(time.time() - start_time))


def inverted_indexing():

    path = sys.argv[1]
    start_time = time.time()
    result = DDS().load_files_from_dir(path).map_and_flatten(_invert_files)\
        .reduce_by_key(lambda a, b: a + b).collect()
    print(result[-1:])
    print("Elapsed Time {} (s)".format(time.time() - start_time))


def main_program():
    print("________RUNNING EXAMPLES_________")
    # pi_estimation()
    # word_count()
    # terasort()
    inverted_indexing()


if __name__ == '__main__':
    main_program()
