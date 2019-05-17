#
#  Copyright 2018 Barcelona Supercomputing Center (www.bsc.es)
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
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import INOUT, IN
from pycompss.api.task import task
from pycompss.dds.partition_generators import IPartitionGenerator

marker = "COMPSS_DEFAULT_VALUE_TO_BE_USED_AS_A_MARKER"


@task(iterator=IN, returns=list)
def get_next_partition(iterable, start, end):
    """
    Divide and retrieve the next partition.
    :return:
    """

    # If it's a dict
    if isinstance(iterable, dict):
        sorted_keys = sorted(iterable.keys())
        for key in sorted_keys[start:end]:
            yield key, iterable[key]
    elif isinstance(iterable, list):
        for item in iter(iterable[start:end]):
            yield item
    else:
        index = 0
        for item in iter(iterable):
            index += 1
            if index > end:
                break
            elif index > start:
                yield item


@task(files=list, returns=list)
def task_read_files(file_paths):
    """

    :param file_paths:
    :return:
    """
    ret = list()
    for file_path in file_paths:
        content = open(file_path).read()
        ret.append((file_path, content))

    return ret


def map_partition(f, partition, col=False):
    return _map_collection(f, *partition) if col \
        else _map_partition(f, partition)


@task(returns=1)
def _map_partition(f, partition):
    """
    Apply a function to a partition in a new task. The function should take an
    iterable as the parameter and return a list.
    :param f: A function that takes an iterable as a parameter
    :param partition: partition generator
    :return: future object of the list containing results
    """
    if isinstance(partition, IPartitionGenerator):
        partition = partition.retrieve_data()

    res = f(partition)
    del partition
    return res


@task(returns=1)
def _map_collection(f, *partition):
    """
    Apply a function to a partition in a new task. The function should take an
    iterable as the parameter and return a list.
    :param f: A function that takes an iterable as a parameter
    :param partition: partition generator
    :return: future object of the list containing results
    """
    res = f(list(partition))
    return res


@task(first=INOUT)
def reduce_dicts(first, *args):
    dicts = iter(args)

    for _dict in dicts:
        for k in _dict:
            first[k] += _dict[k]


@task(returns=list, iterator=IN)
def task_dict_to_list(iterator, total_parts, partition_num):
    """
    Convert a dictionary to a list to be used as partitions later.
    :param iterator:
    :param total_parts:
    :param partition_num:
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


@task(returns=1, first=INOUT)
def merge_dicts(first, second, merger_function):
    """
    """
    for key in second:
        val = second[key]
        first[key] = merger_function(first[key], val) if key in first else val


@task(returns=1)
def reduce_partition(f, partition):
    """
    TODO: Should it be a Tree Reducer as well?
    """
    iterator = iter(partition)
    try:
        res = next(iterator)
    except StopIteration:
        return

    for el in partition:
        res = f(res, el)

    return res


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
def combine_lists(*args):
    ret = list()
    for _list in args:
        ret.extend(_list[1])
    return ret


@task(returns=list)
def task_collect_samples(partition, num_of_samples, key_func):
    """
    """
    ret = list()
    total = len(partition)
    step = max(total / num_of_samples, 1)
    for _i in range(0, total, step):
        ret.append(key_func(partition[_i][0]))

    return ret

