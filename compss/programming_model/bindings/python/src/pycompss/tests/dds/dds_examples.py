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

from random import Random

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


def transitive_closure(partitions):

    edges = _generate_graph()
    od = DDS().load(edges, partitions).collect(future_objects=True)

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = DDS().load(od, -1).map(lambda x_y: (x_y[1], x_y[0]))

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

def pi_estimation_example():
    pi_estimation()


def transitive_closure_example():
    transitive_closure(2)


def main():
    pi_estimation_example()
    transitive_closure_example()
