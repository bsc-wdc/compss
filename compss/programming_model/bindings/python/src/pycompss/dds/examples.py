#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

from pycompss.dds import DDS


def to_list(a): return [a]


def append(a, b):
    a.append(b)
    return a


def extender(a, b):
    a.extend(b)
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


def reduce_example():
    test = DDS(range(100), 50).reduce((lambda b, a: b + a), initial=100,
                                      arity=3, collect=False)\
                              .map(lambda a: a+1).collect()
    print(test)


def word_count():

    path_file = sys.argv[1]
    size_block = int(sys.argv[3])

    start = time.time()
    result = DDS().load_file(path_file, chunk_size=size_block, worker_read=True)\
        .map_and_flatten(lambda x: x.split()).count_by_value(as_dict=True)

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
    dds = DDS(range(10), 5)
    print("Elements of the DDS:")
    print(dds.collect())

    print("Elements & Partitions of the DDS:")
    dds = DDS(range(10), 5)
    print(dds.collect(True))
    print('______________END OF THE EXAMPLE________________\n')


def example_2():
    # EXAMPLE 3
    print("Occurrences of letters in different strings are as following:")
    occurrences = [
        ("a", 1), ("b", 1), ("c", 2), ("d", 7), ("a", 2), ("b", 7), ("b", 6),
        ("a", 2), ("c", 7), ("d", 6), ("e", 2), ("n", 7), ("m", 2), ("n", 6),
        ("e", 2), ("e", 12)]
    print(occurrences)
    print("Retrieve the letters that have more than 5 occurrences in total:")

    dds = DDS(occurrences)
    print(dds.reduce_by_key(_sum).filter(lambda x: x[1] > 5).keys().collect())
    print('______________END OF THE EXAMPLE________________\n')


def example_3():
    print("Given: ID's of players and their points from different tries are:")
    results = [(1, 10), (2, 5), (3, 8), (1, 7), (2, 6), (3, 15), (1, 5), (2, 6)]
    print(results)
    print("Knowing that maximum tries is 3, show the results of the players "
          "who have finished the game :)")
    dds = DDS(results)
    completed = dds.combine_by_key(to_list, append, extender).filter(
        _finished).collect()
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
    dds_words = DDS().load(words, 10).map_and_flatten(lambda x: x.split(" "))\
        .filter(lambda x: len(x) == 1).count_by_value()

    # Extract letters from 'letters' and count them
    dds_letters = DDS().load(letters, 5).map_and_flatten(lambda x: list(x))\
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

    results = DDS().load_text_file(file_name, chunk_size=100)\
        .filter(lambda line: '#' in line)\
        .map_and_flatten(lambda line: line.split(" ")) \
        .count_by_value()\
        .filter(lambda x: len(x[0]) > 2)\
        .collect_as_dict()

    print("Words of lines containing '#':")
    print(results)

    import os
    os.remove(file_name)
    print("______________END OF THE EXAMPLE________________\n")


def load_n_map_example():

    fayl = 'test.txt'
    test = open(fayl, 'w')
    for number in range(100):
        test.write("This is line # {} \n".format(number))
    test.close()

    def sum_line_numbers(partition, initial=0):
        """
        Doesn't return a list, but a single value...
        """
        sum = initial
        for line in partition:
            sum += int(line.split()[-1])
        return sum

    result = DDS().load_and_map_partitions(fayl, sum_line_numbers, initial=9).collect()
    import os
    os.remove(fayl)
    print(result)


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
    load_n_map_example()


if __name__ == '__main__':
    main_program()
