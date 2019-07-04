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


def to_list(a): return [a]


def append(a, b):
    a.append(b)
    return a


def _sum(a, b): return a + b


def _finished(a):
    if len(a[1]) > 2:
        return True


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
    return


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
    print('______________END OF THE EXAMPLE________________\n')


def example_1():
    print("Creating a DDS with range(10) and 5 partitions:")
    dds = DDS().load(range(10), 5).filter(lambda x: x % 2).collect()
    print("Elements of the DDS:")
    print(dds)


def example_2():
    # EXAMPLE 3
    print("Occurrences of letters in different strings are as following:")
    occurrences = [
        ("a", 1), ("b", 1), ("c", 2), ("d", 7), ("a", 2), ("b", 7), ("b", 6),
        ("a", 2), ("c", 7), ("d", 6), ("e", 2), ("n", 7), ("m", 2), ("n", 6),
        ("e", 2), ("e", 12)]
    print(occurrences)
    print("Retrieve the letters that have more than 5 occurrences in total:")

    dds = DDS().load(occurrences)
    print(dds.reduce_by_key(_sum).filter(lambda x: x[1] > 5).keys().collect())
    print('______________END OF THE EXAMPLE________________\n')


def example_3():

    def extender(a, b):
        a.extend(b)
        return a

    print("Given: ID's of players and their points from different tries are:")
    results = [(1, 10), (1, 7), (2, 5), (3, 8), (2, 6), (3, 15), (1, 5), (2, 6)]
    print(results)
    print("Knowing that maximum tries is 3, show the results of the players "
          "who have finished the game :)")
    dds = DDS().load(results, 3)
    completed = dds.map(lambda x: (x[0], x[1] + 1))\
        .combine_by_key(to_list, append, extender)\
        .filter(_finished).collect()

    for k, v in completed:
        print("Player ID: ", k)
        print("Points: ", v)

    print('______________END OF THE EXAMPLE________________\n')


def example_4():
    """
    Just a crazy example to try as many methods as possible
    :return:
    """
    print("Some words:")
    words = ["This is a test", "This is an example.", "H o l a", "Lorem ipsum.",
             "Foo, bar", "Examples of some DDS methods..."]
    print(words)

    print("Some letters:")
    letters = "someletterswilloccurmorethanonceandthisisreallycool! \n"
    print(letters)

    # Extract single letter words from 'words' and count them
    dds_words = DDS().load(words, 10).map_and_flatten(lambda x: x.split(" ")) \
        .filter(lambda x: len(x) == 1).count_by_value()

    # Extract letters from 'letters' and count them
    dds_letters = DDS().load(letters, 5).map_and_flatten(lambda x: list(x)) \
        .count_by_value()
    print()
    print("Amongst single letter words and letters, the highest occurrence is:")
    # Join, group by letter and show the highest occurrence
    print(dds_words.union(dds_letters).reduce_by_key(_sum).max(lambda x: x[1]))
    print('______________END OF THE EXAMPLE________________\n')


def example_5():
    print("WordCount for lines containing '#' (sharp) in a file.")

    file_name = 'test.txt'
    f = open(file_name, 'w')
    for i in range(1000):
        f.write("This is a line with # \n")
        f.write("This one doesn't have a sharp {}\n")
    f.close()

    results = DDS().load_text_file(file_name, chunk_size=100) \
        .filter(lambda line: '#' in line) \
        .map_and_flatten(lambda line: line.split(" ")) \
        .count_by_value() \
        .filter(lambda x: len(x[0]) > 2) \
        .collect_as_dict()

    print("Words of lines containing '#':")
    print(results)

    import os
    os.remove(file_name)
    print("______________END OF THE EXAMPLE________________\n")


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

    print("Elapsed Time {} (s)".format(time.time() - start_time))


def test_new_dds():

    path_file = sys.argv[1]

    test = DDS().\
        load_files_from_dir(path_file).\
        map(lambda x: x).\
        map_and_flatten(lambda x: x[1].split()).\
        map(lambda x: (x, x)).\
        filter(lambda x: True).\
        map_partitions(lambda x: x).\
        map(lambda x: x[0]).\
        count_by_value(arity=4, as_dict=True)

    print(test)
    return


def main_program():
    print("________RUNNING EXAMPLES_________")
    # example_1()
    # example_2()
    # example_3()
    # example_4()
    # example_5()
    # pi_estimation()
    # See 'launch.sh' for WordCount example.
    # word_count()
    # reduce_example()
    # load_n_map_example()
    terasort()
    # test_new_dds()
    inverted_indexing()


if __name__ == '__main__':
    main_program()
