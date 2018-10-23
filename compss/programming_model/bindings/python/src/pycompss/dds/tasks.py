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

from pycompss.api.parameter import INOUT, IN, FILE_IN
from pycompss.api.task import task

marker = "COMPSS_DEFAULT_VALUE_TO_BE_USED_AS_A_MARKER"


def default_hash(x):
    """
    :param x:
    :return: hash value
    """
    return hash(x)


"""
Helper functions to convert regular python functions to PyCOMPSs tasks. All 
functions return future objects (of a list or a single value).
"""


@task(returns=1)
def task_map_partition(f, partition):
    """
    Apply a function to a partition in a new task. The function should take an
    iterable as a parameter and return a list.
    :param f: A function that takes an iterable as a parameter
    :param map_func: in case 'f' is a reverse_mapper function, the real map
                    function should be sent as 'map_func'. This is important
                    because nested functions cannot be serialized.
    :param partition:
    :return: future object of the list containing results
    """
    res = f(partition)
    del partition
    return res


@task(returns=0)
def apply_to_all(f, iterator):
    """
    Apply a void function to each element of an iteratos in a new task
    :param f: a void function
    :param iterator:
    """
    for x in iterator:
        f(x)


@task(returns=list)
def task_filter(f, iterator):
    """

    :param f:
    :param iterator:
    :return:
    """
    ret = list()
    for i in iterator:
        if f(i):
            ret.append(i)
    return ret


@task(returns=1)
def task_reduce_partition(f, partition):
    """

    :param f:
    :param partition:
    :return:
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
def task_reduce(f, *args, **kwargs):
    """
    """
    iterator = iter(args)
    try:
        res = next(iterator)
    except StopIteration:
        return

    for el in iterator:
        res = f(res, el)

    if kwargs.get('as_list', False):
        res = [res]

    return res


@task(buckets=INOUT)
def distribute(iterator, partition_func, buckets, num_of_partitions):
    """
    Distribute data on buckets based on a partition function.
    :param partition_func: a function to generate an integer for values.
    :param buckets: future partitions
    :param iterator: current partition or another iterable
    :param num_of_partitions: total amount of partitions to be created in the end
    :return:
    """
    for k, v in iterator:
        buckets[partition_func(k) % num_of_partitions].append((k, v))


@task(returns=dict)
def task_combine(iterator, creator_func, combiner_func):
    """
    Combine elements of an iterator (partition) by key in a dictionary.
    :param iterator:
    :param creator_func: a function to apply to the value, in its key's first
                        occurrence.
    :param combiner_func: a function to combine second and later occurrences
    :return:
    """
    r = dict()
    for k, v in iterator:
        r[k] = combiner_func(r[k], v) if k in r else creator_func(v)
    return r


@task(returns=1)
def task_merge(a, b, merger_function):
    """
    Merge two combined values and update the value in the first incoming dict.
    :param a:
    :param b:
    :param merger_function:
    :return:
    """

    for k, v in b.items():
        temp = merger_function(a[k], v) if k in a else v
        a[k] = temp

    return a


@task(dic1=INOUT)
def reduce_dicts(dic1, dic2):
    for k in dic2:
        dic1[k] += dic2[k]


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


@task(returns=list)
def another_task_dict_to_list(iterator):
    """
    Convert a dictionary to a list to be used as partitions later.
    :param iterator:
    :return:
    """
    ret = list()
    for i in iterator.keys():
        ret.append((i, iterator[i]))

    return ret


@task(returns=list, iterator=INOUT)
def task_next_bucket(iterator, partition_num):
    """
    Get the next partition of incoming data depending on its size and number of
    total partitions to be created left. After adding elements to new partitions,
    they are deleted from the source in order to ease data transfer.
    :param iterator:
    :param partition_num:
    :return:
    """
    ret = list()
    chunk_size = max(1, len(iterator) // partition_num)
    for index, key in enumerate(iterator.keys()):
        ret.extend(iterator[key])
        del iterator[key]
        if index == chunk_size-1:
            break

    return ret


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


@task(returns=list)
def task_load(data):
    """

    :param data:
    :return:
    """
    ret = list()
    if isinstance(data, list):
        ret.extend(data)
    else:
        ret.append(data)
    return ret


@task(iterator=IN, returns=list)
def get_next_partition(iterable, start, end):
    """
    Divide and retrieve the next partition.
    :return:
    """
    ret = list()

    # Strings are immutable, just add them to a list
    if isinstance(iterable, basestring):
        yield iterable[start:end]
    # If it's a dict
    elif isinstance(iterable, dict):
        sorted_keys = sorted(iterable.keys())
        for key in sorted_keys[start:end]:
            yield key
    # elif hasattr(iterator, "__len__") and hasattr(iterator, "__getslice__"):
    #     # Todo: Test it!
    #     # Python 2
    #     print("____________________________PYTHON 2")
    #     ret = iterator[:chunk_size]
    #     iterator = iterator[chunk_size:]
    #     return ret
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


@task(returns=list)
def load_partition_from_file(file_path, start, chunk_size):
    """

    :param file_path:
    :param start:
    :param chunk_size:
    :return:
    """
    fp = open(file_path)
    fp.seek(start)
    temp = fp.read(chunk_size)
    fp.close()
    return [temp]

