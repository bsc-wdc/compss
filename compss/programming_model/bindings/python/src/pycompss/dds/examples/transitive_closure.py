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

# -*- co_ding: utf-8 -*-

"""
PyCOMPSs DDS - Examples - transitive closure.

This file contains the DDS transitive closure example.
"""

import time
import random

from pycompss.dds import DDS


def _generate_graph():
    """Generate graph.

    :return: Set of edges.
    """
    random.seed(1)
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


def transitive_closure(partitions=2):
    """Transitive closure.

    :param partitions: Number of partitions.
    :results: Transitive closure result.
    """
    print("--- TRANSITIVE CLOSURE ---")

    edges = _generate_graph()
    start_time = time.time()

    o_d = DDS().load(edges, partitions).collect(future_objects=True)

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = DDS().load(o_d, -1).map(lambda x_y: (x_y[1], x_y[0]))

    next_count = DDS().load(o_d, -1).count()

    while True:
        old_count = next_count
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = (
            DDS()
            .load(o_d, -1)
            .join(edges)
            .map(lambda __a_b: (__a_b[1][1], __a_b[1][0]))
        )
        o_d = (
            DDS()
            .load(o_d, -1)
            .union(new_edges)
            .distinct()
            .collect(future_objects=True)
        )

        next_count = DDS().load(o_d, -1).count()

        if next_count == old_count:
            break

    print(f"- TC has {next_count} edges")
    print(f"- Elapsed Time: {time.time() - start_time} (s)")
    print("--------------------------")

    return next_count == 20
