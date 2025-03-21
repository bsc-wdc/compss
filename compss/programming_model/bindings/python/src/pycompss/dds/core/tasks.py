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

# -*- coding: utf-8 -*-

"""PyCOMPSs DDS - Tasks."""

import os
import pickle

from pycompss.api.parameter import INOUT
from pycompss.api.parameter import IN
from pycompss.api.parameter import COLLECTION_OUT
from pycompss.api.parameter import COLLECTION_IN
from pycompss.api.parameter import Type
from pycompss.api.parameter import Depth
from pycompss.api.task import task
from pycompss.dds.core.partition_generators import IPartitionGenerator

MARKER = "COMPSS_DEFAULT_VALUE_TO_BE_USED_AS_A_MARKER"
FILE_NAME_LENGTH = 5


@task(returns=1, collection={Type: COLLECTION_IN, Depth: 1})
def map_partition(func, partition, collection=None):
    """Map the given function to the partition.

    :param func: A function that return only one argument which is iterable.
    :param partition: The partition itself or a partition generator object.
    :param collection: Partition when partition is a collection.
    :return: The transformed partition.
    """
    partition = partition or collection or []
    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    res = func(partition)
    del partition
    return res


@task(
    col={Type: COLLECTION_OUT, Depth: 1},
    collection={Type: COLLECTION_IN, Depth: 1},
)
def distribute_partition(
    col, func, partitioner_func, partition, collection=None
):
    """Distribute (k, v) structured elements of the partition on 'buckets'.

    :param col: Empty 'buckets', must be repleced with COLLECTION_OUT.
    :param func: Function from DDS object to be applied to the partition before
                 the distribution.
    :param partitioner_func: A function to find element's corresponding bucket.
    :param partition: The partition itself or a partition generator object.
    :param collection: If the partition is a collection of future objects.
    :return: Fill the empty 'buckets' with the elements of the partition.
    """
    partition = partition or collection or []

    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    partition = func(partition) if func else partition

    nop = len(col)
    for key, value in partition:
        col[partitioner_func(key) % nop].append((key, value))


@task(first=INOUT, rest={Type: COLLECTION_IN, Depth: 1})
def reduce_dicts(first, rest):
    """Reduce dictionaries.

    CAUTION! Modifies first dictionary.

    :param first: First dictionary.
    :param rest: Second dictionary.
    :return: None.
    """
    dicts = iter(rest)

    for _dict in dicts:
        for k in _dict:
            first[k] += _dict[k]


@task(returns=list, iterator=IN)
def task_dict_to_list(iterator, total_parts, partition_num):
    """Convert dictionary to (key, value) pairs.

    :param iterator: Iterator object.
    :param total_parts: Total parts.
    :param partition_num: Number of partitions.
    :return: List of (key, value) pairs
    """
    ret = []
    sorted_keys = sorted(iterator.keys())
    total = len(sorted_keys)
    chunk_size = max(1, total / total_parts)
    start = chunk_size * partition_num
    is_last = total_parts == partition_num + 1

    if is_last:
        for i in sorted_keys[start:]:
            ret.append((i, iterator[i]))
    else:
        for i in sorted_keys[start : start + chunk_size]:  # noqa: E203
            ret.append((i, iterator[i]))

    return ret


@task(returns=1, parts={Type: COLLECTION_IN, Depth: 1})
def reduce_multiple(function, parts):
    """Reduce multiple.

    :param function: Reducing function.
    :param parts: List of elements.
    :returns: Reduction result.
    """
    partitions = iter(parts)
    try:
        res = next(partitions)[0]
    except StopIteration:
        return None

    for part in partitions:
        if part:
            res = function(res, part[0])

    return [res]


@task(returns=list)
def task_collect_samples(partition, num_of_samples, key_func):
    """Collect samples.

    :param partition: List of elements.
    :param num_of_samples: Number of samples.
    :param key_func: Key function.
    :return: Collected samples.
    """
    ret = []
    total = len(partition)
    step = max(total // num_of_samples, 1)
    for _i in range(0, total, step):
        ret.append(key_func(partition[_i][0]))

    return ret


@task(collection={Type: COLLECTION_IN, Depth: 1})
def map_and_save_text_file(func, index, path, partition, collection=None):
    """Map and save text file.

    Same as 'map_partition' function with the only difference that this one
    saves the result as a text file.

    :param func: Function to apply.
    :param index: Important to keep the order of the partitions.
    :param path: Directory to save the partition.
    :param partition: Partition.
    :param collection: Is collection?
    :return: No return value skips the serialization phase.
    """
    partition = partition or collection or []

    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    partition = func(partition) if func else partition

    file_name = os.path.join(path, str(index).zfill(FILE_NAME_LENGTH))
    with open(file_name, "w") as _:  # pylint: disable=W1514
        for item in partition:
            _.write("\n".join([str(item)]))


@task(collection={Type: COLLECTION_IN, Depth: 1})
def map_and_save_pickle(func, index, path, partition, collection=None):
    """Map and save pickled file.

    Same as 'map_partition' function with the only difference that this one
    saves the result as a pickle file.

    :param func: Function to apply.
    :param index: Important to keep the order of the partitions.
    :param path: Directory to save the partition.
    :param partition: Partition.
    :param collection: Is collection?
    :return: None.
    """
    partition = partition or collection or []

    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    partition = func(partition) if func else partition

    file_name = os.path.join(path, str(index).zfill(FILE_NAME_LENGTH))
    with open(file_name, "wb") as file_name_fd:
        pickle.dump(list(partition), file_name_fd)
