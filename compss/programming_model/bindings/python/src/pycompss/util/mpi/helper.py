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
    """
    Distributes mpi ranks to data given a collection layout

    :param collection_layout: Layout of the collection.
    :return distribution: distribution of rank x
    """

    block_count, block_length, stride = collection_layout
    stride += 1
    distribution = []
    if block_count == size:
        chunksize = block_count / size
        offset = rank * chunksize * stride
        remainder = 0
        distribution = [offset]
    elif block_count > size:
        chunksize = block_count / size
        offset = rank * chunksize * stride

        if rank == size - 1:
            remainder = block_count % size
            chunksize = chunksize + remainder
        if rank == size - 1:
            distribution = range(offset, offset + chunksize + 1)
        else:
            distribution = range(offset, offset + chunksize)
    elif block_count < size:
        mpi_per_block = size / block_count
        remainder = size % block_count
        block_range = range(block_count)
        mpi_per_block_list = [mpi_per_block] * block_count
        if remainder != 0:
            mpi_per_block_list[block_count-1] += remainder
        distribution = []
        last_index = 0
        for _ in range(size):
            distribution.append(block_range[last_index])
            if mpi_per_block_list[last_index] == 0:
                if last_index+1 < block_count:
                    last_index += 1
            else:
                mpi_per_block_list[last_index] -= 1
        distribution = [distribution[rank]]
    return distribution
