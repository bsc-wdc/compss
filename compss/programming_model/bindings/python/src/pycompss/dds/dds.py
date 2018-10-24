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
import sys, os
from collections import defaultdict, deque

from pycompss.api.api import compss_wait_on, compss_barrier, compss_open
from tasks import *


class DDS(object):
    """
    Distributed Data Handler.
    Should distribute the data and run tasks for each partition.
    """

    def __init__(self, iterator=None, num_of_parts=3):
        super(DDS, self).__init__()
        self.partitions = list()
        if iterator:
            self.load(iterator, num_of_parts)

    def load(self, iterator, num_of_parts=3):
        """
        Use the iterator and create the partitions of this DDS.
        :param iterator:
        :param num_of_parts:
        :return:

        >>> DDS().load(range(10), 5).num_of_partitions()
        5
        """
        if isinstance(iterator, xrange) or not hasattr(iterator, "__len__"):
            iterator = list(iterator)

        self.create_partitions(iterator, num_of_parts)
        return self

    def load_file(self, file_path, chunk_size=1024, worker_read=False):
        """
        Read file in chunks and save it onto partitions.
        :param file_path: a path to a file to be loaded
        :param chunk_size: size of chunks in bytes
        :param worker_read:
        :return:

        >>> with open("test.file", "w") as testFile:
        ...    _ = testFile.write("Hello world!")
        >>> DDS().load_file("test.file", 6).collect()
        ['Hello ', 'world!']
        """
        if worker_read:
            fp = open(file_path)
            fp.seek(0, 2)
            total = fp.tell()
            parsed = 0
            while parsed < total:
                self.partitions.append(load_partition_from_file(
                    file_path, parsed, chunk_size))
                parsed += chunk_size
        else:
            f = compss_open(file_path, 'r')
            chunk = f.read(chunk_size)
            while chunk:
                self.partitions.append(task_load(chunk))
                chunk = f.read(chunk_size)

        return self

    def load_text_file(self, file_name, chunk_size=1024):
        """
        Load a text file into partitions with 'chunk_size' lines on each.
        :param file_name: a path to a file to be loaded
        :param chunk_size: size of chunks in bytes
        :return:

        >>> with open("test.txt", "w") as testFile:
        ...    _ = testFile.write("First Line! \\n")
        ...    _ = testFile.write("Second Line! \\n")
        >>> DDS().load_text_file("test.txt").collect()
        ['First Line! ', 'Second Line! ']
        """

        self.partitions.extend(map(task_load, read_in_chunks(file_name,
                                                             chunk_size)))
        return self

    def load_and_map_partitions(self, file_name, map_func, chunk_size=1024,
                                *args, **kwargs):
        """

        :param file_name:
        :param map_func: a function that takes a partition as an argument
        :param chunk_size: chunk size in BYTES... After collecting each line,
                           size of the partition will be compared with this.
        :param args: If your function takes more arguments than just the
                     partition, feel free to send them here.
        :return:
        """

        for chunk in read_in_chunks(file_name, chunk_size):
            self.partitions.append(task_load_and_map(chunk, map_func, *args, **kwargs))

        return self

    def load_files_from_dir(self, dir_path, num_of_parts=3):
        """

        :param dir_path:
        :param num_of_parts:
        :return:
        """
        files = os.listdir(dir_path)
        total = len(files)
        partition_sizes = [(total // num_of_parts)] * num_of_parts
        extras = total % num_of_parts
        for i in range(extras):
            partition_sizes[i] += 1

        start = 0
        for size in partition_sizes:
            end = start + size
            partition_files = list()
            for file_name in files[start:end]:
                file_path = os.path.join(dir_path, file_name)
                partition_files.append(file_path)

            self.partitions.append(task_read_files(partition_files))
            start = end

        return self

    def create_partitions(self, iterator, num_of_parts):
        """
        Saves 'List of future objects' as the partitions. So once called, this
        data set will always contain only future objects.
        :param iterator:
        :param num_of_parts: Number of partitions to be created
                            Should be -1 (minus 1) if iterator is already a list
                            of future objects
        :return:

        >>> DDS().load(range(10), 2).collect(True)
        [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
        """
        if self.partitions:
            raise Exception("Partitions have already been created, cannot load "
                            "new data.")

        if num_of_parts == -1:
            self.partitions = iterator
            return

        total = len(iterator)
        if not total:
            return

        chunk_sizes = [(total // num_of_parts)] * num_of_parts
        extras = total % num_of_parts
        for i in range(extras):
            chunk_sizes[i] += 1

        if isinstance(iterator, basestring):
            start, end = 0, 0
            for size in chunk_sizes:
                end = start + size
                temp = task_load(iterator[start:end])
                start = end
                self.partitions.append(temp)
            return

        start = 0
        for size in chunk_sizes:
            end = start + size
            temp = get_next_partition(iterator, start, end)
            self.partitions.append(temp)
            start = end
        return

    def collect(self, keep_partitions=False, future_objects=False):
        """
        Returns all elements from all partitions. Elements can be grouped by
        partitions by setting keep_partitions value as True.
        :param keep_partitions: Keep Partitions?
        :param future_objects:
        :return:

        >>> dds = DDS().load(range(10), 2)
        >>> dds.collect(True)
        [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
        >>> DDS().load(range(10), 2).collect()
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        res = list()
        # Future objects cannot be extended for now...
        if future_objects:
            return self.partitions

        self.partitions = compss_wait_on(self.partitions)
        if not keep_partitions:
            for p in self.partitions:
                p = compss_wait_on(p)
                res.extend(p)
        else:
            for p in self.partitions:
                res.append(list(p))
        return res

    def partition_by(self, partition_func=default_hash, num_of_partitions=-1):
        """
        Create partitions by a Partition Func.
        :param partition_func: A Function distribute data on partitions based on
                For example, hash function.
        :param num_of_partitions: number of partitions to be created
        :return:
        >>> dds = DDS([0, 0, 1, 1, 2, 3]).map(lambda x: (x*x, x))
        >>> dds.partition_by(num_of_partitions=5).collect(True)
        [[(0, 0), (0, 0)], [(1, 1), (1, 1)], [(4, 2), (9, 3)], [], []]
        """
        nop = len(self.partitions) if num_of_partitions == -1 \
            else num_of_partitions

        future_partitions = defaultdict(list)

        for p in self.partitions:
            distribute(p, partition_func, future_partitions, nop)

        ret = []
        for i in range(nop, 0, -1):
            ret.append(task_next_bucket(future_partitions, i))
        self.partitions = ret
        return self

    def num_of_partitions(self):
        """
        Get the total amount of partitions
        :return: int

        >>> DDS(range(10), 5).num_of_partitions()
        5
        """
        return len(self.partitions)

    def map(self, f):
        """
        Apply a function to each element of this data set.
        :param f: A function that will take each element of this data set as a
                  parameter
        :return:

        >>> dds = DDS(range(10), 5).map(lambda x: x * 2).map(lambda x: x+1)
        >>> sorted(dds.collect())
        [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
        """

        def dummy(_):
            return map(f, _)

        return self.map_partitions(dummy)

    def map_partitions(self, f):
        """
        Apply a function to each partition of this data set.
        :param f: A function that takes an iterable as a parameter
        :return:

        >>> DDS(range(10), 5).map_partitions(lambda x: [sum(x)]).collect(True)
        [[1], [5], [9], [13], [17]]
        """
        future_objects = []
        for p in self.partitions:
            future_objects.append(task_map_partition(f, p))
        self.partitions = future_objects
        return self

    def map_and_flatten(self, f):
        """
        Just because flat_map is an ugly name.
        Apply a function to each element and extend the derived element(s) if
        possible.
        :param f: A function that should return a list, tuple or another kind of
                  iterable
        :return:

        >>> dds = DDS([2, 3, 4])
        >>> sorted(dds.map_and_flatten(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        """
        from itertools import chain

        def dummy(iterator):
            return list(chain.from_iterable(map(f, iterator)))

        return self.map_partitions(dummy)

    def filter(self, f):
        """
        Filter elements of this data set.
        :param f: A filtering function
        :return:

        >>> DDS(range(10), 5).filter(lambda x: x % 2).count()
        5
        """
        def dummy(iterator):
            return filter(f, iterator)

        return self.map_partitions(dummy)

    def distinct(self):
        """
        Get the distinct elements of this data set.
        :return:

        >>> test = list(range(10))
        >>> test.extend(list(range(5)))
        >>> len(test)
        15
        >>> DDS(test, 5).distinct().count()
        10
        """
        return self.map(lambda x: (x, None))\
            .reduce_by_key(lambda x, _: x).map(lambda x: x[0])

    def reduce(self, f, initial=marker, arity=-1, collect=True):
        """
        Reduce the whole data set.
        :param f: A reduce function which should take two parameters as inputs
                  and return a single result which will be sent to itself again.
        :param initial: Initial value for reducer which will be used to reduce
                the first element with.
        :param collect: if return value should be the result or a DDS containing
                        the result
        :param arity: tree depth
        :return: reduced result (inside a DDS if necessary).

        >>> DDS(range(10), 5).reduce((lambda b, a: b + a) , 100)
        145
        """
        local_results = deque()
        for part in self.partitions:
            local_results.append(task_reduce_partition(f, part))

        # If initial value is set, add it to the list as well
        if initial != marker:
            local_results.append(initial)

        arity = arity if arity > 0 else len(self.partitions)
        branch = list()
        while local_results:
            while local_results and len(branch) < arity:
                temp = local_results.popleft()
                branch.append(temp)

            if len(branch) == 1:
                break
            # branch = compss_wait_on(branch)
            temp = task_reduce(f, as_list=False, *branch)
            local_results.append(temp)
            branch = []

        if collect:
            return compss_wait_on(branch[0])

        # Create only one partition as a future object, and return new DDS
        future_partition = list()
        future_partition.append(task_reduce(f, as_list=True, *branch))
        self.partitions = future_partition
        return self

    def union(self, *args):
        """
        Combine this data set with some other DDS data.
        :param args: Arbitrary amount of DDS objects.
        :return:

        >>> first = DDS([0, 1, 2, 3, 4], 2)
        >>> second = DDS([5, 6, 7, 8, 9], 3)
        >>> first.union(second).count()
        10
        """

        for dds in args:
            self.partitions.extend(dds.partitions)

        return self

    def key_by(self, f):
        """
        Create a (key,value) pair for each element where the 'key' is f(value)
        :param f: A Key Creator function which takes the element as a parameter
                  and returns the key.
        :return: list of (key, value) pairs

        >>> dds = DDS(range(3), 2)
        >>> dds.key_by(lambda x: str(x)).collect()
        [('0', 0), ('1', 1), ('2', 2)]
        """
        return self.map(lambda x: (f(x), x))

    def sum(self):
        """
        Sum everything up

        >>> DDS(range(3), 2).sum()
        3

        """
        return sum(self.map_partitions(lambda x: [sum(x)]).collect())

    def count(self):
        """
        :return: total number of elements

        >>> DDS(range(3), 2).count()
        3
        """
        return self.map(lambda x: 1).sum()

    def count_by_value(self, as_dict=False):
        """
        Amount of each element on this data set.
        :return: list of tuples (element, number)

        >>> first = DDS([0, 1, 2], 2)
        >>> second = DDS([2, 3, 4], 3)
        >>> first.union(second).count_by_value(as_dict=True)
        {0: 1, 1: 1, 2: 2, 3: 1, 4: 1}
        """

        def count_partition(iterator):
            counts = defaultdict(int)
            for obj in iterator:
                counts[obj] += 1
            return counts

        # Count locally and create dictionary partitions
        self.map_partitions(count_partition)

        # Create a deque from partitions and start reduce
        future_objects = deque(self.partitions)
        ret = []
        while future_objects:
            first = future_objects.popleft()
            if future_objects:
                second = future_objects.popleft()
                reduce_dicts(first, second)
                future_objects.append(first)
            else:
                # If it's the last item in the queue, retrieve it:
                if as_dict:
                    # As a dict if necessary
                    first = compss_wait_on(first)
                    return dict(first)

                # As a list of future objects
                # TODO: Optimizations required!
                length = len(self.partitions)
                for i in range(length):
                    ret.append(task_dict_to_list(first, length, i))

        self.partitions = ret
        return self

    def max(self, key=None):
        """
        :return: the highest value on this data set.

        >>> DDS(range(10)).max()
        9
        """
        m = -sys.maxint
        if key is None:
            return self.reduce(max, m)

        def dummy(a, b):
            """
            To avoid failures when the inout of the key function is not an int
            :param a:
            :param b:
            :return:
            """
            if a == -sys.maxint:
                return b
            return max(a, b, key=key)

        return self.reduce(dummy, m)

    def min(self, key=None):
        """
        :param key: The lowest value on this data set.
        :return:

        >>> DDS(range(10)).min()
        0
        """
        m = sys.maxint
        if key is None:
            return self.reduce(min, m)

        def dummy(a, b):
            """
            To avoid failures when the inout of the key function is not an int
            :param a:
            :param b:
            :return:
            """
            if a == sys.maxint:
                return b
            return min(a, b, key=key)

        return self.reduce(dummy, m)

    def foreach(self, f):
        """
        Apply a function to each element of this data set without returning
        anything.
        :param f: a void function
        :return: null

        >>> def dummy(x): print(x)
        >>> DDS([1, 2], 2).foreach(dummy)
        1
        2
        """
        self.map(f)
        # Wait for all the tasks to finish
        compss_barrier()
        return

    """
    Functions for (Key, Value) pairs.
    """

    def collect_as_dict(self):
        """
        Get (key,value) as { key: value }.
        :return: dict

        >>> DDS().load([("a", 1), ("b", 1)]).collect_as_dict()
        {'a': 1, 'b': 1}
        """
        return dict(self.collect())

    def keys(self):
        """
        :return: list of keys

        >>> DDS().load([("a", 1), ("b", 1)]).keys().collect()
        ['a', 'b']
        """
        return self.map(lambda x: x[0])

    def values(self):
        """
        :return: list of values

        >>> DDS().load([("a", 1), ("b", 2)]).values().collect()
        [1, 2]
        """
        return self.map(lambda x: x[1])

    def map_values(self, f):
        """
        Apply a function to each value of (key, value) element of this data set.
        :param f: a function which takes 'value's as parameter.
        :return: new DDS

        >>> DDS().load([("a", 1), ("b", 1)]).map_values(lambda x: x+1).collect()
        [('a', 2), ('b', 2)]
        """
        def dummy(pair):
            return pair[0], f(pair[1])

        return self.map(dummy)

    # TODO: Review/Test, and Decide!
    def combine_by_key(self, creator_func, combiner_func, merger_function,
                       partitions=-1, as_dict=False):
        """
        Should combine different values of the same key.
        Works as a tree combiner. First, runs combiner function for each
        partition, then merger function for each pair of partitions and so on.

        :param creator_func: a function to be applied to the first value of each
                             key. The function should take only one argument.
                             For example, element->list converter.

        :param combiner_func: a function to combine values after the second
                            occurrence. It must take two parameters: the first
                            one is the result of 'creator_function', the second
                            one is new coming value.

        :param merger_function: a function to merge two combined values. It is
                                necessary for tree combiner, because sometimes
                                combiner function cannot be applied to 'combined
                                results'. For example, if combiner function
                                appends new values, merger function should
                                behave as an 'extender'.

        :param partitions: number of partitions to be created in the end.
        :param as_dict: True- results will be a dictionary
                        False-results will be distributed on new partitions
                                and retrieved as a new DDS
        :return:
        """
        partitions = len(self.partitions) if partitions < 0 else partitions
        ret = []
        future_objects = deque()
        for p in self.partitions:
            future_objects.append(task_combine(p, creator_func, combiner_func))

        while future_objects:
            first = future_objects.popleft()
            if future_objects:
                second = future_objects.popleft()
                temp = task_merge(first, second, merger_function)
                future_objects.append(temp)
            else:
                # If it's the last item in the queue, retrieve it:
                if as_dict:
                    # As a dict if necessary
                    ret = compss_wait_on(first)
                    return ret
                # As a list of future objects
                ret = []
                for i in range(partitions):
                    ret.append(task_dict_to_list(first, partitions, i))

        self.partitions = ret
        return self

    def another_combine_by_key(self, creator_func, combiner_func,
                               as_dict=False):
        """
        Combine values by grouping them by key on each partition and then
        apply a combiner function. Here we do not need a merger function since
        partitions don't contain any element with the same key. So, combining
        them locally is enough.
        :param as_dict: True- results will be a dictionary
                        False-results will be distributed on new partitions
                                and retrieved as a new DDS
        :param creator_func: a function to be applied to values in their key's
                            first occurrence.
        :param combiner_func: a function to combine new values.
        :return:
        """
        partitioned = self.partition_by()
        local_dicts = []
        for p in partitioned.partitions:
            local_dicts.append(task_combine(p, creator_func, combiner_func))

        if as_dict:
            ret = dict()
            for l in local_dicts:
                ret.update(compss_wait_on(l))
            return ret

        ret = []
        for d in local_dicts:
            ret.append(another_task_dict_to_list(d))
        self.partitions = ret
        return self

    def reduce_by_key(self, f, as_dict=False):
        """
        Reduce values for each key.
        :param f: a reducer function which takes two parameters and returns one.
        :param as_dict: see 'as_dict' argument of 'combine_by_key'
        :return:

        >>> DDS([("a",1), ("a",2)]).reduce_by_key((lambda a, b: a+b)).collect()
        [('a', 3)]
        """
        return self.combine_by_key((lambda x: x), f, f, as_dict=as_dict)

    def count_by_key(self, as_dict=False):
        """
        :return: a new DDS with data set of list of tuples (element, occurrence)
        :param as_dict: see 'as_dict' argument of 'combine_by_key'

        >>> DDS([("a", 100), ("a", 200)]).count_by_key().collect_as_dict()
        {'a': 2}
        """
        return self.map(lambda x: x[0]).count_by_value(as_dict=as_dict)

    def flatten_by_key(self, f):
        """
        Reverse of combine by key.Flat (key, values) as (key, value1),
        (key, value2) etc.
        :param f: a function to parse values
        :return:

        >>> DDS([('a',[1, 2]), ('b',[1])]).flatten_by_key(lambda x: x).collect()
        [('a', 1), ('a', 2), ('b', 1)]
        """
        def dummy(key_value):
            return ((key_value[0], x) for x in f(key_value[1]))

        return self.map_and_flatten(dummy)


def read_in_chunks(file_name, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    partition = list()
    f = compss_open(file_name)
    collected = 0
    for line in f:
        partition.append(line.rstrip("\n"))
        collected += sys.getsizeof(line)
        if collected > chunk_size:
            yield partition
            partition = []
            collected = 0

    if partition:
        yield partition


def _run_tests():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    _run_tests()
