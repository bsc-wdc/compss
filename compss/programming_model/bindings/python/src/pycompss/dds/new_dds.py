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
import bisect
import itertools
import os
from collections import deque, defaultdict
from itertools import chain

from pycompss.api.api import compss_wait_on, compss_barrier

from pycompss.dds.new_tasks import *
from pycompss.dds.partition_generators import *
from pycompss.dds import heapq3
from operator import add


@task(returns="NUMBER_OF_SUB_PARTITIONS")
def distribute_partition(partition, filter_func, nop):
    """
    """
    global NUMBER_OF_SUB_PARTITIONS

    from collections import defaultdict
    buckets = defaultdict(list)

    for _i in range(nop):
        buckets[_i] = list()

    for k, v in partition:
        buckets[filter_func(k) % nop].append((k, v))

    return buckets.items()


def default_hash(x):
    """
    :param x:
    :return: hash value
    """
    return hash(x)


class DDS(object):
    """

    """

    def __init__(self):
        """
        """
        super(DDS, self).__init__()
        self.partitions = list()
        self.func = None

    def load(self, iterator, num_of_parts=10):
        """
        """
        if num_of_parts == -1:
            self.partitions = iterator
            return self

        total = len(iterator)
        if not total:
            return self

        chunk_sizes = [(total // num_of_parts)] * num_of_parts
        extras = total % num_of_parts
        for i in range(extras):
            chunk_sizes[i] += 1

        start = 0
        for size in chunk_sizes:
            end = start + size
            _partition_loader = IteratorLoader(iterator, start, end)
            self.partitions.append(_partition_loader)
            start = end

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
                _partition_loader = WorkerFileLoader(
                    [file_path], single_file=True,
                    start=parsed, chunk_size=chunk_size)

                self.partitions.append(_partition_loader)
                parsed += chunk_size
        else:
            f = open(file_path, 'r')
            chunk = f.read(chunk_size)
            while chunk:
                _partition_loader = BasicDataLoader(chunk)
                self.partitions.append(_partition_loader)
                chunk = f.read(chunk_size)

        return self

    def load_text_file(self, file_name, chunk_size=1024, in_bytes=True,
                       strip=True):
        """
        Load a text file into partitions with 'chunk_size' lines on each.
        :param file_name: a path to a file to be loaded
        :param chunk_size: size of chunks in bytes
        :param in_bytes: if chunk size is in bytes or in number of lines.
        :param strip: if line separators should be stripped from lines
        :return:

        >>> with open("test.txt", "w") as testFile:
        ...    _ = testFile.write("First Line! \\n")
        ...    _ = testFile.write("Second Line! \\n")
        >>> DDS().load_text_file("test.txt").collect()
        ['First Line! ', 'Second Line! ']
        """
        func = read_in_chunks if in_bytes else read_lines

        for _p in func(file_name, chunk_size, strip=strip):
            partition_loader = BasicDataLoader(_p)
            self.partitions.append(partition_loader)

        return self

    def load_files_from_dir(self, dir_path, num_of_parts=-1):
        """
        Read multiple files from a given directory. Each file and its content
        is saved in a tuple in ('file_path', 'file_content') format.
        :param dir_path: A directory that all files will be loaded from
        :param num_of_parts: can be set to -1 to create one partition per file
        :return:
        """
        files = os.listdir(dir_path)
        total = len(files)
        num_of_parts = total if num_of_parts < 0 else num_of_parts
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

            _partition_loader = WorkerFileLoader(partition_files)
            self.partitions.append(_partition_loader)
            start = end

        return self

    def union(self, *args):
        """
        Combine this data set with some other DDS data.
        :param args: Arbitrary amount of DDS objects.
        :return:

        >>> first = DDS().load([0, 1, 2, 3, 4], 2)
        >>> second = DDS().load([5, 6, 7, 8, 9], 3)
        >>> first.union(second).count()
        10
        """

        for dds in args:
            self.partitions.extend(dds.partitions)

        return self

    def num_of_partitions(self):
        """ Get the total amount of partitions
        :return: int

        >>> DDS().load(range(10), 5).num_of_partitions()
        5
        """
        return len(self.partitions)

    def map(self, func, *args, **kwargs):
        """ Apply the given function to each element of the dataset.

        >>> dds = DDS().load(range(10), 5).map(lambda x: x * 2)
        >>> sorted(dds.collect())
        [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        """

        def mapper(iterator):
            results = list()
            for item in iterator:
                results.append(func(item, *args, **kwargs))
            return results

        return self.map_partitions(mapper)

    def map_partitions(self, func):
        """ Apply a function to each partition of this data set.

        >>> DDS().load(range(10), 5).map_partitions(lambda x: [sum(x)]).collect(True)
        [[1], [5], [9], [13], [17]]
        """
        return ChildDDS(self, func)

    def map_and_flatten(self, f, *args, **kwargs):
        """ Apply a function to each element of the dataset, and extend the
        derived element(s) if possible.
        :param f: A function that should return a list, tuple or another kind of
                  iterable

        >>> dds = DDS().load([2, 3, 4])
        >>> sorted(dds.map_and_flatten(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        """
        def mapper(iterator):
            res = list()
            for item in iterator:
                res.extend(f(item, *args, **kwargs))
            return res

        return self.map_partitions(mapper)

    def filter(self, f):
        """ Filter elements of this data set by applying a given function.


        >>> DDS().load(range(10), 5).filter(lambda x: x % 2).count()
        5
        """
        def _filter(iterator):
            return filter(f, iterator)

        return self.map_partitions(_filter)

    def reduce(self, f, initial=marker, arity=-1):
        """
        Reduce the whole data set.
        :param f: A reduce function which should take two parameters as inputs
                  and return a single result which will be sent to itself again.
        :param initial: Initial value for reducer which will be used to reduce
                the first element with.
        :param arity: tree depth
        :return: reduced result (inside a DDS if necessary).

        >>> DDS().load(range(10), 5).reduce((lambda b, a: b + a) , 100)
        145
        """
        def local_reducer(partition):
            """
            A function to reduce a partition and retrieve it as a partition
            containing one element.
            :param partition:
            :return:
            """
            iterator = iter(partition)
            try:
                init = next(iterator)
            except StopIteration:
                return []

            return [reduce(f, iterator, init)]

        local_results = self.map_partitions(local_reducer)\
            .collect(future_objects=True)

        local_results = deque(local_results)

        # If initial value is set, add it to the list as well
        if initial != marker:
            local_results.append([initial])

        arity = arity if arity > 0 else len(self.partitions)
        branch = list()

        while local_results:
            while local_results and len(branch) < arity:
                temp = local_results.popleft()
                branch.append(temp)

            if len(branch) == 1:
                branch = compss_wait_on(branch[0])
                break

            temp = reduce_multiple(f, *branch)
            local_results.append(temp)
            branch = []

        return branch[0]

    def distinct(self):
        """
        Get the distinct elements of this data set.
        :return:

        >>> test = list(range(10))
        >>> test.extend(list(range(5)))
        >>> len(test)
        15
        >>> DDS().load(test, 5).distinct().count()
        10
        """
        return self.map(lambda x: (x, None))\
            .reduce_by_key(lambda x, _: x).map(lambda x: x[0])

    def count_by_value(self, arity=2, as_dict=False):
        """
        Amount of each element on this data set.
        :return: list of tuples (element, number)

        >>> first = DDS().load([0, 1, 2], 2)
        >>> second = DDS().load([2, 3, 4], 3)
        >>> first.union(second).count_by_value(as_dict=True)
        {0: 1, 1: 1, 2: 2, 3: 1, 4: 1}
        """

        def count_partition(iterator):
            counts = defaultdict(int)
            for obj in iterator:
                counts[obj] += 1
            return counts

        # Count locally and create dictionary partitions
        local_results = self.map_partitions(count_partition) \
            .collect(future_objects=True)

        # Create a deque from partitions and start reduce
        future_objects = deque(local_results)

        branch = list()
        while future_objects:
            branch = []
            while future_objects and len(branch) < arity:
                temp = future_objects.popleft()
                branch.append(temp)

            if len(branch) == 1:
                break
            reduce_dicts(*branch)
            future_objects.append(branch[0])

        if as_dict:
            branch[0] = compss_wait_on(branch[0])
            return dict(branch[0])

        length = self.num_of_partitions()
        new_partitions = list()
        for i in range(length):
            new_partitions.append(task_dict_to_list(branch[0], length, i))

        return DDS().load(new_partitions, -1)

    def key_by(self, f):
        """
        Create a (key,value) pair for each element where the 'key' is f(value)
        :param f: A Key Creator function which takes the element as a parameter
                  and returns the key.
        :return: list of (key, value) pairs

        >>> dds = DDS().load(range(3), 2)
        >>> dds.key_by(lambda x: str(x)).collect()
        [('0', 0), ('1', 1), ('2', 2)]
        """
        return self.map(lambda x: (f(x), x))

    def sum(self):
        """
        Sum everything up

        >>> DDS().load(range(3), 2).sum()
        3

        """
        return sum(self.map_partitions(lambda x: [sum(x)]).collect())

    def count(self):
        """
        :return: total number of elements

        >>> DDS().load(range(3), 2).count()
        3
        """
        return self.map(lambda x: 1).sum()

    def foreach(self, f):
        """
        Apply a function to each element of this data set without returning
        anything.
        :param f: a void function
        """
        self.map(f)
        # Wait for all the tasks to finish
        compss_barrier()
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

        processed = list()
        if self.func:
            for _p in self.partitions:
                processed.append(map_partition(self.func, _p))
            # Reset the function!
            self.func = None
        else:
            for _p in self.partitions:
                if isinstance(_p, IPartitionGenerator):
                    processed.append(_p.retrieve_data())
                else:
                    processed.append(_p)

        # Future objects cannot be extended for now...
        if future_objects:
            return processed

        processed = compss_wait_on(processed)

        ret = list()
        if not keep_partitions:
            for _pp in processed:
                ret.extend(_pp)
        else:
            for _pp in processed:
                ret.append(list(_pp))
        return ret

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

    def partition_by(self, partition_func=default_hash, num_of_partitions=-1):
        """
        Create partitions by a Partition Func.
        :param partition_func: A Function distribute data on partitions based on
                For example, hash function.
        :param num_of_partitions: number of partitions to be created
        :return:
        >>> dds = DDS().load(range(6)).map(lambda x: (x, x))
        >>> dds.partition_by(num_of_partitions=3).collect(True)
        [[(0, 0), (3, 3)], [(1, 1), (4, 4)], [(2, 2), (5, 5)]]
        """

        nop = len(self.partitions) if num_of_partitions == -1 \
            else num_of_partitions
        collected = self.collect(keep_partitions=True, future_objects=True)

        grouped = defaultdict(list)
        global NUMBER_OF_SUB_PARTITIONS
        NUMBER_OF_SUB_PARTITIONS = nop

        for partition in collected:
            temp = distribute_partition(partition, partition_func, nop)
            for _i in range(nop):
                grouped[_i].append(temp[_i])

        future_partitions = list()
        for bucket in grouped.values():
            future_partitions.append(combine_lists(*bucket))

        return DDS().load(future_partitions, -1)

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

    def flatten_by_key(self, f):
        """
        Reverse of combine by key.Flat (key, values) as (key, value1),
        (key, value2) etc.
        :param f: a function to parse values
        :return:

        >>> DDS().load([('a',[1, 2]), ('b',[1])]).flatten_by_key(lambda x: x).collect()
        [('a', 1), ('a', 2), ('b', 1)]
        """
        def dummy(key_value):
            return ((key_value[0], x) for x in f(key_value[1]))

        return self.map_and_flatten(dummy)

    def combine_by_key(self, creator_func, combiner_func, merger_function,
                       total_parts=-1, collect=False):
        """
        """
        total_parts = len(self.partitions) if total_parts < 0 else total_parts

        def combine_partition(partition):
            """
            """
            res = dict()
            for key, val in partition:
                res[key] = combiner_func(res[key], val) if key in res \
                                                        else creator_func(val)
            return res

        locally_combined = self.map_partitions(combine_partition)\
            .collect(future_objects=True)

        future_objects = deque(locally_combined)
        while future_objects:
            first = future_objects.popleft()
            if future_objects:
                second = future_objects.popleft()
                merge_dicts(first, second, merger_function)
                future_objects.append(first)
            else:
                # If it's the last item in the queue, retrieve it:
                if collect:
                    # As a dict if necessary
                    ret = compss_wait_on(first)
                    return ret

                # As a list of future objects
                # TODO: Implement 'dict' --> 'lists on nodes'
                new_partitions = list()
                for i in range(total_parts):
                    new_partitions.append(task_dict_to_list(first, total_parts, i))

                return DDS().load(new_partitions, -1)

    def reduce_by_key(self, f, collect=False):
        """ Reduce values for each key.
        :param f: a reducer function which takes two parameters and returns one.
        :param collect: if should retrieve results

        >>> DDS().load([("a",1), ("a",2)]).reduce_by_key((lambda a, b: a+b), collect=True)
        {'a': 3}
        """
        return self.combine_by_key((lambda x: x), f, f, collect=collect)

    def count_by_key(self, as_dict=False):
        """
        :return: a new DDS with data set of list of tuples (element, occurrence)
        :param as_dict: see 'as_dict' argument of 'combine_by_key'

        >>> DDS().load([("a", 100), ("a", 200)]).count_by_key(True)
        {'a': 2}
        """
        return self.map(lambda x: x[0]).count_by_value(as_dict=as_dict)

    def sort_by_key(self, ascending=True, num_of_parts=None,
                    key_func=lambda x: x):
        """

        :type key_func:
        :param num_of_parts:
        :param ascending:
        :return:
        """

        def range_partitioner(key):
            p = bisect.bisect_left(bounds, key_func(key))
            if ascending:
                return p
            else:
                return num_of_parts - 1 - p

        def sort_partition(iterator):
            """
            Sort a partition locally.
            :param iterator:
            :return:
            """
            chunk_size = 500
            iterator = iter(iterator)
            chunks = list()
            while True:
                chunk = list(itertools.islice(iterator, chunk_size))
                chunk.sort(key=lambda kv: key_func(kv[0]), reverse=not ascending)
                chunks.append(chunk)
                if len(chunk) < chunk_size:
                    break
            else:
                chunks.append(chunk.sort(key=lambda kv: key_func(kv[0]),
                                         reverse=not ascending))

            return heapq3.merge(chunks, key=lambda kv: key_func(kv[0]),
                                reverse=not ascending)

        partitioned = DDS().load(col_parts, -1).partition_by(range_partitioner)
        return partitioned.map_partitions(sort_partition)


class ChildDDS(DDS):
    """

    """
    def __init__(self, parent, func):
        """

        :param parent:
        :param func:
        """
        super(ChildDDS, self).__init__()
        if not isinstance(parent, ChildDDS):
            self.func = func
            if isinstance(parent, DDS):
                self.partitions = parent.partitions
        else:
            self.partitions = parent.partitions
            par_func = parent.func

            def wrap_parent_func(partition):
                return func(par_func(partition))

            self.func = wrap_parent_func


def tree_reduce_dicts(initial, reduce_function, collect, total_parts=-1):
    """
    """
    future_objects = deque(initial)

    while future_objects:
        first = future_objects.popleft()
        if future_objects:
            second = future_objects.popleft()
            merge_dicts(first, second, reduce_function)
            future_objects.append(first)
        else:
            # If it's the last item in the queue, retrieve it:
            if collect:
                # As a dict if necessary
                ret = compss_wait_on(first)
                return ret
            # As a list of future objects
            # TODO: Implement 'dict' --> 'lists on nodes'
            ret = list()
            for i in range(total_parts):
                ret.append(task_dict_to_list(first, total_parts, i))
            return ret


def _run_tests():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    _run_tests()
