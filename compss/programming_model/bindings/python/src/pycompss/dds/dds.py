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
import bisect
import itertools
import os
import pickle
from collections import deque, defaultdict

from pycompss.api.api import compss_wait_on as cwo, compss_delete_object as cdo
from pycompss.api.api import compss_barrier
from pycompss.dds import heapq3
from pycompss.dds.partition_generators import *
from pycompss.dds.tasks import *
from pycompss.util.tracing.helpers import event


def default_hash(x):
    """
    :param x:
    :return: hash value
    """
    return hash(x)


class DDS(object):
    """ Distributed Data Set object.

    """

    def __init__(self):
        """
        """
        super(DDS, self).__init__()
        self.partitions = list()
        self.func = None

        # Partition As A Collection
        # True if partitions are not Future Objects but list of Future Objects
        self.paac = False

    def load(self, iterator, num_of_parts=10, paac=False):
        """ Load and distribute the iterator on partitions.
        """
        self.paac = paac
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
        files = sorted(os.listdir(dir_path))
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

    def load_pickle_files(self, dir_path):
        """
        Load serialized partitions from pickle files.
        :param dir_path: path to serialized partitions.
        :return:
        """

        files = sorted(os.listdir(dir_path))
        for _f in files:
            file_name = os.path.join(dir_path, _f)
            _partition_loader = PickleLoader(file_name)
            self.partitions.append(_partition_loader)

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
        current = list(self.collect(future_objects=True))
        for dds in args:
            temp = list(dds.collect(future_objects=True))
            current.extend(temp)

        return DDS().load(current, num_of_parts=-1)

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

        def mapper(partition):
            results = list()
            for element in partition:
                results.append(func(element, *args, **kwargs))
            return results

        return ChildDDS(self, mapper)

    def map_partitions(self, func):
        """ Apply a function to each partition of this data set.

        >>> DDS().load(range(10), 5).map_partitions(lambda x: [sum(x)]).collect(True)
        [[1], [5], [9], [13], [17]]
        """
        return ChildDDS(self, func)

    def flat_map(self, f, *args, **kwargs):
        """ Apply a function to each element of the dataset, and extend the
        derived element(s) if possible.
        :param f: A function that should return a list, tuple or another kind of
                  iterable

        >>> dds = DDS().load([2, 3, 4])
        >>> sorted(dds.flat_map(lambda x: range(1, x)).collect())
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
                branch = cwo(branch[0])
                break

            temp = reduce_multiple(f, branch)
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

    def count_by_value(self, arity=2, as_dict=True, as_fo=False):
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

            first, branch = branch[0], branch[1:]
            reduce_dicts(first, branch)
            future_objects.append(first)

        if as_dict:
            if as_fo:
                return branch[0]
            branch[0] = cwo(branch[0])
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
        return self.map_partitions(lambda i: [sum(1 for _ in i)]).sum()
        # return self.map(lambda x: 1).sum()

    def foreach(self, f):
        """
        Apply a function to each element of this data set without returning
        anything.
        :param f: a void function
        """
        self.map(f)
        # Wait for all the tasks to finish
        compss_barrier()

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
            if self.paac:
                for col in self.partitions:
                    processed.append(map_partition(self.func, None, col))
            else:
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

        processed = cwo(processed)

        ret = list()
        if not keep_partitions:
            for _pp in processed:
                ret.extend(_pp)
        else:
            for _pp in processed:
                ret.append(list(_pp))
        return ret

    def save_as_text_file(self, path):
        """
        Save string representations of DDS elements as text files (one file per
        partition).
        :param path:
        :return:
        """
        if self.paac:
            for i, _p in enumerate(self.partitions):
                map_and_save_text_file(self.func, i, path, None, _p)
                cdo(_p)
        else:
            for i, _p in enumerate(self.partitions):
                map_and_save_text_file(self.func, i, path, _p)
        return None

    def save_as_pickle(self, path):
        """
        Save partitions of this DDS as pickle files. Each partition is saved as
        a separate file for the sake of parallelism.
        :param path:
        :return:
        """
        if self.paac:
            for i, _p in enumerate(self.partitions):
                map_and_save_pickle(self.func, i, path, None, _p)
        else:
            for i, _p in enumerate(self.partitions):
                map_and_save_pickle(self.func, i, path, _p)
        return None

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

    def partition_by(self, partitioner_func=default_hash, num_of_partitions=-1):
        """
        Create partitions by a Partition Func.
        :param partitioner_func: A Function distribute data on partitions based on
                For example, hash function.
        :param num_of_partitions: number of partitions to be created
        :return:
        >>> dds = DDS().load(range(6)).map(lambda x: (x, x))
        >>> dds.partition_by(num_of_partitions=3).collect(True)
        [[(0, 0), (3, 3)], [(1, 1), (4, 4)], [(2, 2), (5, 5)]]
        """

        def combine_lists(_partition):
            # Elements of the partition are grouped by their previous partitions
            ret = list()
            for _li in _partition:
                ret.extend(_li)
            return ret

        nop = len(self.partitions) if num_of_partitions == -1 \
            else num_of_partitions

        grouped = defaultdict(list)

        if self.paac:
            for collection in self.partitions:
                col = [[] for _ in range(nop)]
                with event(3002, master=True):
                    distribute_partition(col, self.func, partitioner_func, None,
                                         collection)
                cdo(collection)
                for _i in range(nop):
                    grouped[_i].append(col[_i])
        else:
            for _part in self.partitions:
                col = [[] for _ in range(nop)]
                with event(3002, master=True):
                    distribute_partition(col, self.func, partitioner_func, _part)
                for _i in range(nop):
                    grouped[_i].append(col[_i])

        future_partitions = list()
        for key in sorted(grouped.keys()):
            future_partitions.append(grouped[key])

        return DDS().load(future_partitions, -1, True)\
            .map_partitions(combine_lists)

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

        return self.flat_map(dummy)

    def join(self, other, num_of_partitions=-1):
        """
        :param other: another dds object
        :param num_of_partitions:
        :return:

        >>> x = DDS().load([("a", 1), ("b", 3)])
        >>> y = DDS().load([("a", 2), ("b", 4)])
        >>> sorted(x.join(y).collect())
        [('a', (1, 2)), ('b', (3, 4))]
        """

        def dispatch(seq):
            buf_1, buf_2 = [], []
            for (n, v) in seq:
                if n == 1:
                    buf_1.append(v)
                elif n == 2:
                    buf_2.append(v)
            return [(v, w) for v in buf_1 for w in buf_2]

        nop = len(self.partitions) if num_of_partitions == -1 \
            else num_of_partitions

        buf_a = self.map_values(lambda v: (1, v))
        buf_b = other.map_values(lambda y: (2, y))

        return buf_a.union(buf_b).group_by_key(num_of_parts=nop)\
            .flatten_by_key(lambda x: dispatch(x.__iter__()))

    def combine_by_key(self, creator_func, combiner_func, merger_function,
                       total_parts=-1):
        """
        Combine elements of each key.
        :param creator_func: to apply to the first element of the key. Takes
                             only one argument which is the value from (k, v)
                             pair. (e.g: v = list(v))
        :param combiner_func: to apply when a new element with the same 'key' is
                              found. It is used to combine partitions locally.
                              Takes 2 arguments; first one is the result of
                              'creator_func' where the second one is a 'value'
                              of the same 'key' from the same partition.
                              (e.g: v1.append(v2))
        :param merger_function: to merge local results. Basically takes two
                                arguments- both are results of 'combiner_func'.
                                (e.g: list_1.extend(list_2) )

        :param total_parts: number of partitions after combinations
        :return:
        """

        def combine_partition(partition):
            """
            """
            res = dict()
            for key, val in partition:
                res[key] = combiner_func(res[key], val) if key in res \
                                                        else creator_func(val)
            return list(res.items())

        def merge_partition(partition):
            """
            """
            res = dict()
            for key, val in partition:
                res[key] = merger_function(res[key], val) if key in res \
                                                        else val
            return list(res.items())

        ret = self.map_partitions(combine_partition)\
            .partition_by(num_of_partitions=total_parts)\
            .map_partitions(merge_partition)

        return ret

    def reduce_by_key(self, f):
        """ Reduce values for each key.
        :param f: a reducer function which takes two parameters and returns one.

        >>> DDS().load([("a",1), ("a",2)]).reduce_by_key((lambda a, b: a+b)).collect()
        [('a', 3)]
        """
        return self.combine_by_key((lambda x: x), f, f)

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

        if num_of_parts is None:
            num_of_parts = len(self.partitions)

        # Collect everything to take samples
        col_parts = self.collect(future_objects=True)
        samples = list()
        for _part in col_parts:
            samples.append(task_collect_samples(_part, 20, key_func))

        samples = sorted(list(
            itertools.chain.from_iterable(cwo(samples))))

        bounds = [samples[int(len(samples) * (i + 1) / num_of_parts)]
                  for i in range(0, num_of_parts - 1)]

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

    def group_by_key(self, num_of_parts=-1):
        """
        Group values of each key in a single list. A special and most used case
        of 'combine_by_key'


        >>> x = DDS().load([("a", 1), ("b", 2), ("a", 2), ("b", 4)])
        >>> sorted(x.group_by_key().collect())
        [('a', [1, 2]), ('b', [2, 4])]
        """
        def _create(x):
            return [x]

        def _merge(xs, x):
            xs.append(x)
            return xs

        def _combine(a, b):
            a.extend(b)
            return a

        return self.combine_by_key(_create, _merge, _combine,
                                   total_parts=num_of_parts)

    def take(self, num):
        """
        The first num elements of DDS.
        :param num: number of elements to be retrieved.
        :return:

        """
        items = []
        partitions = self.collect(future_objects=True)
        taken = 0

        for part in partitions:
            _p = iter(cwo(part))
            while taken < num:
                try:
                    items.append(next(_p))
                    taken += 1
                except StopIteration:
                    break
            if taken >= num:
                break

        return items[:num]


class ChildDDS(DDS):
    """ Similar as DDS objects, with the only difference that ChildDDS objects
    inherit the partitions from their parents, and have functions to be mapped
    to the their partitions.
    """
    def __init__(self, parent, func):
        """

        :param parent:
        :param func:
        """
        super(ChildDDS, self).__init__()
        self.paac = parent.paac

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


def _run_tests():
    import doctest
    doctest.testmod()
    os.remove("test.file")
    os.remove("test.txt")


if __name__ == "__main__":
    _run_tests()
