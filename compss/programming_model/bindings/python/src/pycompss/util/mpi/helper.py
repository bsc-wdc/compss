#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Util - MPI
=====================
    This file contains all MPI helper methods.
"""

from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank


def rank_distributor(collection_layout):
    # type: (tuple) -> list
    """
    Distributes mpi ranks to data given a collection layout

    :param collection_layout: Layout of the collection.
    :return distribution: distribution of rank x
    """
    block_count, block_length, stride = collection_layout
    if block_count == -1:
        block_count = size
    if stride == -1:
        stride = 1
    if block_length == -1:
        block_length = 1
    distribution = []
    if block_count == size:
        # Number of block bigger than processes
        # (one block per process)
        offset = rank * stride
        distribution = list(range(offset, offset+block_length))
    else:
        # If number of blocks is bigger than processes
        # (blocks round-robin distributed)
        block = rank
        while block < block_count:
            offset = block * stride
            distribution = distribution + list(range(offset,
                                                     offset + block_length))
            block = block + size
    return distribution
