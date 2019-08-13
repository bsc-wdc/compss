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
import os
import pickle

from pycompss.api.api import compss_wait_on as cwo
from pycompss.api.parameter import INOUT, IN, COLLECTION_INOUT
from pycompss.api.task import task
from pycompss.dds.partition_generators import IPartitionGenerator

marker = "COMPSS_DEFAULT_VALUE_TO_BE_USED_AS_A_MARKER"
FILE_NAME_LENGTH = 5


@task(returns=1)
def map_partition(func, partition, *collection):

    partition = partition or list(collection)
    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    res = func(partition)
    del partition
    return res


@task(col=COLLECTION_INOUT)
def distribute_partition(col, func, partitioner_func, partition, *collection):
    """ Distribute (key, value) structured elements of the partition on
    'buckets'.
    :param partition:
    :param partitioner_func: a function to find element's corresponding bucket
    :param col: empty 'buckets'.
    :param func: function from DDS object to be applied to the parition before
                 the distribution.
    :return: fill the empty 'buckets' with the elements of the partition.
    """
    partition = partition or list(collection)

    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    partition = func(partition) if func else partition

    nop = len(col)
    for k, v in partition:
        col[partitioner_func(k) % nop].append((k, v))


@task(first=INOUT)
def reduce_dicts(first, *args):
    dicts = iter(args)

    for _dict in dicts:
        for k in _dict:
            first[k] += _dict[k]


@task(returns=list, iterator=IN)
def task_dict_to_list(iterator, total_parts, partition_num):
    """ Disctionary to (key, value) pairs.
    :return:
    """
    ret = list()
    sorted_keys = sorted(iterator.keys())
    total = len(sorted_keys)
    chunk_size = max(1, total / total_parts)
    start = chunk_size * partition_num
    is_last = (total_parts == partition_num + 1)

    if is_last:
        for i in sorted_keys[start:]:
            ret.append((i, iterator[i]))
    else:
        for i in sorted_keys[start:start+chunk_size]:
            ret.append((i, iterator[i]))

    return ret


@task(returns=1)
def reduce_multiple(f, *args):
    """
    """
    partitions = iter(args)
    try:
        res = next(partitions)[0]
    except StopIteration:
        return

    for part in partitions:
        if part:
            res = f(res, part[0])

    return [res]


@task(returns=list)
def task_collect_samples(partition, num_of_samples, key_func):
    """
    """
    ret = list()
    total = len(partition)
    step = max(total // num_of_samples, 1)
    for _i in range(0, total, step):
        ret.append(key_func(partition[_i][0]))

    return ret


@task()
def map_and_save_text_file(func, index, path, partition, *collection):

    partition = partition or list(collection)

    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    partition = func(partition) if func else partition

    file_name = os.path.join(path, str(index).zfill(FILE_NAME_LENGTH))
    with open(file_name, "w") as _:
        _.write("\n".join([str(item) for item in partition]))


@task()
def map_and_save_pickle(func, index, path, partition, *collection):

    partition = partition or list(collection)

    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    partition = func(partition) if func else partition

    file_name = os.path.join(path, str(index).zfill(FILE_NAME_LENGTH))
    pickle.dump(list(partition), open(file_name, "wb"))

