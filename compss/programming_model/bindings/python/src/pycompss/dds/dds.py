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

"""
PyCOMPSs DDS - API.

This file contains the DDS interface.
"""

import heapq
import bisect
import itertools
import functools
import os
from collections import defaultdict
from collections import deque

from pycompss.api.api import compss_wait_on
from pycompss.api.api import compss_delete_object
from pycompss.api.api import compss_barrier
from pycompss.dds.core.partition_generators import IPartitionGenerator
from pycompss.dds.core.partition_generators import BasicDataLoader
from pycompss.dds.core.partition_generators import IteratorLoader
from pycompss.dds.core.partition_generators import WorkerFileLoader
from pycompss.dds.core.partition_generators import PickleLoader
from pycompss.dds.core.partition_generators import read_in_chunks
from pycompss.dds.core.partition_generators import read_lines
from pycompss.dds.core.tasks import map_partition
from pycompss.dds.core.tasks import distribute_partition
from pycompss.dds.core.tasks import reduce_dicts
from pycompss.dds.core.tasks import task_dict_to_list
from pycompss.dds.core.tasks import reduce_multiple
from pycompss.dds.core.tasks import task_collect_samples
from pycompss.dds.core.tasks import map_and_save_text_file
from pycompss.dds.core.tasks import map_and_save_pickle
from pycompss.dds.core.tasks import MARKER
from pycompss.dds.core.utils import default_hash
from pycompss.util.tracing.helpers import EventMaster


class DDS:  # pylint: disable=too-many-public-methods
    """Distributed Data Set object."""

    def __init__(self):
        """Create a new DDS object."""
        super().__init__()
        self.partitions = []
        self.func = None

        # Partition As A Collection
        # True if partitions are not Future Objects but list of Future Objects
        self.paac = False

    def load(self, iterator, num_of_parts=10, paac=False):
        """Load and distribute the iterator on partitions.

        :param iterator: Partitions iterator.
        :param num_of_parts: Number of parts.
        :param paac: Partition as a collection.
        :returns: Self.
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
        """Read file in chunks and save it onto partitions.

        Usage sample:
            >>> with open("test.file", "w") as testFile:
            ...     _ = testFile.write("Hello world!")
            >>> DDS().load_file("test.file", 6).collect()
            ['Hello ', 'world!']

        :param file_path: A path to a file to be loaded.
        :param chunk_size: Size of chunks in bytes.
        :param worker_read: If reading the file in the worker
                            (skips first bytes).
        :return: Self.
        """
        if worker_read:
            with open(file_path) as file_path_fd:  # pylint: disable=W1514
                file_path_fd.seek(0, 2)
                total = file_path_fd.tell()
            parsed = 0
            while parsed < total:
                _partition_loader = WorkerFileLoader(
                    [file_path],
                    single_file=True,
                    start=parsed,
                    chunk_size=chunk_size,
                )
                self.partitions.append(_partition_loader)
                parsed += chunk_size
        else:
            with open(file_path, "r") as file_path_fd:  # pylint: disable=W1514
                chunk = file_path_fd.read(chunk_size)
                while chunk:
                    _partition_loader = BasicDataLoader(chunk)
                    self.partitions.append(_partition_loader)
                    chunk = file_path_fd.read(chunk_size)

        return self

    def load_text_file(
        self, file_name, chunk_size=1024, in_bytes=True, strip=True
    ):
        r"""Load a text file into partitions with 'chunk_size' lines on each.

        Usage sample:
            >>> with open("test.txt", "w") as testFile:
            ...     _ = testFile.write("First Line! \n")
            ...     _ = testFile.write("Second Line! \n")
            >>> DDS().load_text_file("test.txt").collect()
            ['First Line! ', 'Second Line! ']

        :param file_name: A path to a file to be loaded.
        :param chunk_size: Size of chunks in bytes.
        :param in_bytes: If chunk size is in bytes or in number of lines.
        :param strip: If line separators should be stripped from lines.
        :return: Self.
        """
        func = read_in_chunks if in_bytes else read_lines

        for _p in func(file_name, chunk_size, strip=strip):
            partition_loader = BasicDataLoader(_p)
            self.partitions.append(partition_loader)

        return self

    def load_files_from_dir(self, dir_path, num_of_parts=-1):
        """Read multiple files from a given directory.

        Each file and its content is saved in a tuple in ('file_path',
        'file_content') format.

        :param dir_path: A directory that all files will be loaded from.
        :param num_of_parts: Can be set to -1 to create one partition per file.
        :return: Self.
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
            partition_files = []
            for file_name in files[start:end]:
                file_path = os.path.join(dir_path, file_name)
                partition_files.append(file_path)

            _partition_loader = WorkerFileLoader(partition_files)
            self.partitions.append(_partition_loader)
            start = end

        return self

    def load_pickle_files(self, dir_path):
        """Load serialized partitions from pickle files.

        :param dir_path: Path to serialized partitions.
        :return: Self.
        """
        files = sorted(os.listdir(dir_path))
        for _f in files:
            file_name = os.path.join(dir_path, _f)
            _partition_loader = PickleLoader(file_name)
            self.partitions.append(_partition_loader)

        return self

    def union(self, *args):
        """Combine this data set with some other DDS data.

        Usage sample:
            >>> first = DDS().load([0, 1, 2, 3, 4], 2)
            >>> second = DDS().load([5, 6, 7, 8, 9], 3)
            >>> first.union(second).count()
            10

        :param args: Arbitrary amount of DDS objects.
        :return: New DDS object combining two DDS objects.
        """
        current = list(self.collect(future_objects=True))
        for dds in args:
            temp = list(dds.collect(future_objects=True))
            current.extend(temp)

        return DDS().load(current, num_of_parts=-1)

    def num_of_partitions(self):
        """Get the total amount of partitions.

        Usage sample:
            >>> DDS().load(range(10), 5).num_of_partitions()
            5

        :return: Number of partitions.
        """
        return len(self.partitions)

    def map(self, func, *args, **kwargs):
        """Apply the given function to each element of the dataset.

        Usage sample:
            >>> dds = DDS().load(range(10), 5).map(lambda x: x * 2)
            >>> sorted(dds.collect())
            [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

        :param func: Function to apply.
        :param args: Arguments.
        :param kwargs: Keyword arguments.
        :returns: New child DDS object.
        """

        def mapper(partition):
            results = []
            for element in partition:
                results.append(func(element, *args, **kwargs))
            return results

        return _ChildDDS(self, mapper)

    def map_partitions(self, func):
        """Apply a function to each partition of this data set.

        Usage sample:
            >>> DDS().load(range(10), 5).map_partitions(
            ...     lambda x: [sum(x)]
            ... ).collect(True)
            [[1], [5], [9], [13], [17]]

        :param func: Function to apply.
        :returns: New child DDS object.
        """
        return _ChildDDS(self, func)

    def flat_map(self, func, *args, **kwargs):
        """Apply a function to each element of the dataset.

        NOTE: Extends the derived element(s) if possible.

        Usage sample:
            >>> dds = DDS().load([2, 3, 4])
            >>> sorted(dds.flat_map(lambda x: range(1, x)).collect())
            [1, 1, 1, 2, 2, 3]

        :param func: A function that should return a list, tuple or
                     another kind of iterable.
        :param args: Arguments.
        :param kwargs: Keyword arguments.
        :returns: New child DDS object.
        """

        def mapper(iterator):
            res = []
            for item in iterator:
                res.extend(func(item, *args, **kwargs))
            return res

        return self.map_partitions(mapper)

    def filter(self, func):
        """Filter elements of this data set by applying a given function.

        Usage sample:
            >>> DDS().load(range(10), 5).filter(lambda x: x % 2).count()
            5

        :param func: Filtering function.
        :returns: New child DDS object filtered.
        """

        def _filter(iterator):
            return filter(func, iterator)

        return self.map_partitions(_filter)

    def reduce(self, func, initial=MARKER, arity=-1):
        """Reduce the whole data set.

        Usage sample:
            >>> DDS().load(range(10), 5).reduce((lambda b, a: b + a) , 100)
            145

        :param func: A reduce function which should take two parameters as
                     inputs and return a single result which will be sent to
                     itself again.
        :param initial: Initial value for reducer which will be used to reduce
                        the first element with.
        :param arity: Tree depth.
        :return: Reduced result (inside a DDS if necessary).
        """

        def local_reducer(partition):
            """Reduce a partition and retrieve it as a one element partition.

            :param partition: Partition.
            :return: One element partition.
            """
            iterator = iter(partition)
            try:
                init = next(iterator)
            except StopIteration:
                return []

            return [functools.reduce(func, iterator, init)]

        local_results = self.map_partitions(local_reducer).collect(
            future_objects=True
        )

        local_results = deque(local_results)

        # If initial value is set, add it to the list as well
        if initial != MARKER:
            local_results.append([initial])

        arity = arity if arity > 0 else len(self.partitions)
        branch = []

        while local_results:
            while local_results and len(branch) < arity:
                temp = local_results.popleft()
                branch.append(temp)

            if len(branch) == 1:
                branch = compss_wait_on(branch[0])
                break

            temp = reduce_multiple(func, branch)
            local_results.append(temp)
            branch = []

        return branch[0]

    def distinct(self):
        """Get the distinct elements of this data set.

        Usage sample:
            >>> test = list(range(10))
            >>> test.extend(list(range(5)))
            >>> len(test)
            15
            >>> DDS().load(test, 5).distinct().count()
            10

        :returns: New child DDS object with distinct elements.
        """
        return (
            self.map(lambda x: (x, None))
            .reduce_by_key(lambda x, _: x)
            .map(lambda x: x[0])
        )

    def count_by_value(self, arity=2, as_dict=True, as_fo=False):
        """Amount of each element on this data set.

        Usage sample:
            >>> first = DDS().load([0, 1, 2], 2)
            >>> second = DDS().load([2, 3, 4], 3)
            >>> dict(sorted(
            ...     first.union(second).count_by_value(as_dict=True).items()
            ... ))
            {0: 1, 1: 1, 2: 2, 3: 1, 4: 1}

        :param arity: Tree depth.
        :param as_dict: As dictionary.
        :param as_fo: As future object.
        :return: List of tuples (element, number).
        """

        def count_partition(iterator):
            counts = defaultdict(int)
            for obj in iterator:
                counts[obj] += 1
            return counts

        # Count locally and create dictionary partitions
        local_results = self.map_partitions(count_partition).collect(
            future_objects=True
        )

        # Create a deque from partitions and start reduce
        future_objects = deque(local_results)

        branch = []
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
            branch[0] = compss_wait_on(branch[0])
            return dict(branch[0])

        length = self.num_of_partitions()
        new_partitions = []
        for i in range(length):
            new_partitions.append(task_dict_to_list(branch[0], length, i))

        return DDS().load(new_partitions, -1)

    def key_by(self, func):
        """Create a (key,value) pair for each element where 'key' is f(value).

        Usage sample:
            >>> dds = DDS().load(range(3), 2)
            >>> dds.key_by(lambda x: str(x)).collect()
            [('0', 0), ('1', 1), ('2', 2)]

        :param func: A Key Creator function which takes the element as a
                     parameter and returns the key.
        :return: List of (key, value) pairs.
        """
        return self.map(lambda x: (func(x), x))

    def sum(self):
        """Sum everything up.

        Usage sample:
            >>> DDS().load(range(3), 2).sum()
            3

        :returns: The sum of everything.
        """
        return sum(self.map_partitions(lambda x: [sum(x)]).collect())

    def count(self):
        """Count everything up.

        Usage sample:
            >>> DDS().load(range(3), 2).count()
            3

        :return: Total number of elements.
        """
        return self.map_partitions(lambda i: [sum(1 for _ in i)]).sum()

    def foreach(self, func):
        """Apply a function to each element of this data set.

        CAUTION: Does not return anything.

        :param func: A void function.
        :returns: None
        """
        self.map(func)
        # Wait for all the tasks to finish
        compss_barrier()

    def collect(
        self,  # pylint: disable=R0912
        keep_partitions=False,
        future_objects=False,
    ):
        """Return all elements from all partitions.

        Elements can be grouped by partitions by setting keep_partitions value
        as True.

        Usage sample:
            >>> dds = DDS().load(range(10), 2)
            >>> dds.collect(True)
            [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
            >>> DDS().load(range(10), 2).collect()
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        :param keep_partitions: Keep Partitions?
        :param future_objects: Future objects?
        :return: All elements from all partitions.
        """
        processed = []
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

        processed = compss_wait_on(processed)

        ret = []
        if not keep_partitions:
            for _pp in processed:
                ret.extend(_pp)
        else:
            for _pp in processed:
                ret.append(list(_pp))
        return ret

    def save_as_text_file(self, path):
        """Save string representations of DDS elements as text files.

        This saving creates one file per partition.

        :param path: Destination file path.
        :return: None.
        """
        if self.paac:
            for i, _p in enumerate(self.partitions):
                map_and_save_text_file(self.func, i, path, None, _p)
                compss_delete_object(_p)
        else:
            for i, _p in enumerate(self.partitions):
                map_and_save_text_file(self.func, i, path, _p)

    def save_as_pickle(self, path):
        """Save partitions of this DDS as pickle files.

        Each partition is saved as a separate file for the sake of parallelism.

        :param path:Destination file path.
        :return: None.
        """
        if self.paac:
            for i, _p in enumerate(self.partitions):
                map_and_save_pickle(self.func, i, path, None, _p)
        else:
            for i, _p in enumerate(self.partitions):
                map_and_save_pickle(self.func, i, path, _p)

    # ################################################################ #
    # ############## Functions for (Key, Value) pairs. ############### #
    # ################################################################ #

    def collect_as_dict(self):
        """Get (key,value) as { key: value }.

        Usage sample:
            >>> DDS().load([("a", 1), ("b", 1)]).collect_as_dict()
            {'a': 1, 'b': 1}

        :return: Dict.
        """
        return dict(self.collect())

    def keys(self):
        """Get keys.

        Usage sample:
            >>> DDS().load([("a", 1), ("b", 1)]).keys().collect()
            ['a', 'b']

        :return: List of keys.
        """
        return self.map(lambda x: x[0])

    def values(self):
        """Get values.

        Usage sample:
            >>> DDS().load([("a", 1), ("b", 2)]).values().collect()
            [1, 2]

        :return: List of values.
        """
        return self.map(lambda x: x[1])

    def partition_by(
        self, partitioner_func=default_hash, num_of_partitions=-1
    ):
        """Create partitions by a Partition Func.

        Usage sample:
            >>> dds = DDS().load(range(6)).map(lambda x: (x, x))
            >>> dds.partition_by(num_of_partitions=3).collect(True)
            [[(0, 0), (3, 3)], [(1, 1), (4, 4)], [(2, 2), (5, 5)]]

        :param partitioner_func: A Function distribute data on partitions based
                                 on for example, hash function.
        :param num_of_partitions: Number of partitions to be created.
        :return: Partitions.
        """

        def combine_lists(_partition):
            # Elements of the partition are grouped by their
            # previous partitions
            ret = []
            for _li in _partition:
                ret.extend(_li)
            return ret

        nop = (
            len(self.partitions)
            if num_of_partitions == -1
            else num_of_partitions
        )

        grouped = defaultdict(list)

        if self.paac:
            for collection in self.partitions:
                col = [[] for _ in range(nop)]
                with EventMaster(3002):
                    distribute_partition(
                        col, self.func, partitioner_func, None, collection
                    )
                compss_delete_object(collection)
                for _i in range(nop):
                    grouped[_i].append(col[_i])
        else:
            for _part in self.partitions:
                col = [[] for _ in range(nop)]
                with EventMaster(3002):
                    distribute_partition(
                        col, self.func, partitioner_func, _part
                    )
                for _i in range(nop):
                    grouped[_i].append(col[_i])

        future_partitions = []
        for key in sorted(grouped.keys()):
            future_partitions.append(grouped[key])

        return (
            DDS()
            .load(future_partitions, -1, True)
            .map_partitions(combine_lists)
        )

    def map_values(self, func):
        """Apply a function to each value of (k, v) element of this data set.

        Usage sample:
            >>> DDS().load([("a", 1), ("b", 1)]).map_values(
            ...     lambda x: x+1
            ... ).collect()
            [('a', 2), ('b', 2)]

        :param func: A function which takes 'value's as parameter.
        :return: New DDS.
        """

        def dummy(pair):
            return pair[0], func(pair[1])

        return self.map(dummy)

    def flatten_by_key(self, func):
        """Reverse of combine by key.Flat (k, v) as (k, v1), (k, v2) etc.

        In detail: (key, values) as (key, value1), (key, value2) ...

        Usage sample:
            >>> DDS().load([('a',[1, 2]), ('b',[1])]).flatten_by_key(
            ...     lambda x: x
            ... ).collect()
            [('a', 1), ('a', 2), ('b', 1)]

        :param func: A function to parse values.
        :return: Flattened by key.
        """

        def dummy(key_value):
            return ((key_value[0], x) for x in func(key_value[1]))

        return self.flat_map(dummy)

    def join(self, other, num_of_partitions=-1):
        """Join DDS objects.

        Usage sample:
            >>> x = DDS().load([("a", 1), ("b", 3)])
            >>> y = DDS().load([("a", 2), ("b", 4)])
            >>> sorted(x.join(y).collect())
            [('a', (1, 2)), ('b', (3, 4))]

        :param other: Another DDS object.
        :param num_of_partitions: Number of partitions.
        :return: Joined DDS objects.
        """

        def dispatch(seq):
            buf_1, buf_2 = [], []
            for num, value in seq:
                if num == 1:
                    buf_1.append(value)
                elif num == 2:
                    buf_2.append(value)
            return [(v, w) for v in buf_1 for w in buf_2]

        nop = (
            len(self.partitions)
            if num_of_partitions == -1
            else num_of_partitions
        )

        buf_a = self.map_values(lambda v: (1, v))
        buf_b = other.map_values(lambda y: (2, y))

        return (
            buf_a.union(buf_b)
            .group_by_key(num_of_parts=nop)
            .flatten_by_key(lambda x: dispatch(iter(x)))
        )

    def combine_by_key(
        self, creator_func, combiner_func, merger_function, total_parts=-1
    ):
        """Combine elements of each key.

        :param creator_func: To apply to the first element of the key. Takes
                             only one argument which is the value from (k, v)
                             pair. (e.g: v = list(v)).
        :param combiner_func: To apply when a new element with the same 'key'
                              is found. It is used to combine partitions
                              locally. Takes 2 arguments; first one is the
                              result of 'creator_func' where the second one
                              is a 'value' of the same 'key' from the same
                              partition. (e.g: v1.append(v2)).
        :param merger_function: To merge local results. Basically takes two
                                arguments -both are results of 'combiner_func'.
                                (e.g: list_1.extend(list_2)).
        :param total_parts: Number of partitions after combinations.
        :return: Combined by key DDS object.
        """

        def combine_partition(partition):
            """Combine partitions.

            :param partition: Dictionary of partitions.
            :returns: List of combined partitions.
            """
            res = {}
            for key, val in partition:
                res[key] = (
                    combiner_func(res[key], val)
                    if key in res
                    else creator_func(val)
                )
            return list(res.items())

        def merge_partition(partition):
            """Merge partitions.

            :param partition: Dictionary of partitions.
            :returns: List of merged partitions.
            """
            res = {}
            for key, val in partition:
                res[key] = (
                    merger_function(res[key], val) if key in res else val
                )
            return list(res.items())

        ret = (
            self.map_partitions(combine_partition)
            .partition_by(num_of_partitions=total_parts)
            .map_partitions(merge_partition)
        )

        return ret

    def reduce_by_key(self, func):
        """Reduce values for each key.

        Usage sample:
            >>> DDS().load([("a",1), ("a",2)]).reduce_by_key(
            ...     (lambda a, b: a+b)
            ... ).collect()
            [('a', 3)]

        :param func: a reducer function which takes two parameters and
                     returns one.
        :returns: Reduced values.
        """
        return self.combine_by_key((lambda x: x), func, func)

    def count_by_key(self, as_dict=False):
        """Count by key.

        Usage sample:
            >>> DDS().load([("a", 100), ("a", 200)]).count_by_key(True)
            {'a': 2}

        :param as_dict: See 'as_dict' argument of 'combine_by_key'.
        :return: A new DDS with data set of list of tuples
                 (element, occurrence).
        """
        return self.map(lambda x: x[0]).count_by_value(as_dict=as_dict)

    def sort_by_key(
        self, ascending=True, num_of_parts=None, key_func=lambda x: x
    ):
        """Sort by key.

        :param ascending: Ascending.
        :param num_of_parts: Number of parts.
        :param key_func: Key function.
        :return: Sorted by key DDS object.
        """
        if num_of_parts is None:
            num_of_parts = len(self.partitions)

        # Collect everything to take samples
        col_parts = self.collect(future_objects=True)
        samples = []
        for _part in col_parts:
            samples.append(task_collect_samples(_part, 20, key_func))

        samples = sorted(
            list(itertools.chain.from_iterable(compss_wait_on(samples)))
        )

        bounds = [
            samples[int(len(samples) * (i + 1) / num_of_parts)]
            for i in range(0, num_of_parts - 1)
        ]

        def range_partitioner(key):
            """Partition a range.

            :param key: Partition key.
            :return: Partitioned range.
            """
            part = bisect.bisect_left(bounds, key_func(key))
            if ascending:
                return part
            return num_of_parts - 1 - part

        def sort_partition(iterator):
            """Sort a partition locally.

            :param iterator: List iterator.
            :return: Sorted partition.
            """
            chunk_size = 500
            iterator = iter(iterator)
            chunks = []
            while True:
                chunk = list(itertools.islice(iterator, chunk_size))
                chunk.sort(
                    key=lambda kv: key_func(kv[0]), reverse=not ascending
                )
                if len(chunk) > 0:
                    chunks.append(chunk)
                if len(chunk) < chunk_size:
                    break
            else:
                chunks.append(
                    chunk.sort(
                        key=lambda kv: key_func(kv[0]), reverse=not ascending
                    )
                )

            if len(chunks) == 1:
                return chunks[0]
            else:
                return heapq.merge(
                    *chunks,
                    key=lambda kv: key_func(kv[0]),
                    reverse=not ascending
                )

        partitioned = DDS().load(col_parts, -1).partition_by(range_partitioner)
        return partitioned.map_partitions(sort_partition)

    def group_by_key(self, num_of_parts=-1):
        """Group values of each key in a single list.

        A special and most used case of 'combine_by_key'.

        Usage sample:
            >>> x = DDS().load([("a", 1), ("b", 2), ("a", 2), ("b", 4)])
            >>> sorted(x.group_by_key().collect())
            [('a', [1, 2]), ('b', [2, 4])]

        :param num_of_parts: Number of parts.
        :returns: Grouped by key DDS object.
        """

        def _create(value):
            return [value]

        def _merge(container, value):
            container.append(value)
            return container

        def _combine(container_a, container_b):
            container_a.extend(container_b)
            return container_a

        return self.combine_by_key(
            _create, _merge, _combine, total_parts=num_of_parts
        )

    def take(self, num):
        """Take the first num elements of DDS.

        :param num: Number of elements to be retrieved.
        :return: First elements of DDS.
        """
        items = []
        partitions = self.collect(future_objects=True)
        taken = 0

        for part in partitions:
            _p = iter(compss_wait_on(part))
            while taken < num:
                try:
                    items.append(next(_p))
                    taken += 1
                except StopIteration:
                    break
            if taken >= num:
                break

        return items[:num]


class _ChildDDS(DDS):
    """_ChildDDS class.

    Similar as DDS objects, with the only difference that _ChildDDS objects
    inherit the partitions from their parents, and have functions to be mapped
    to their partitions.
    """

    def __init__(self, parent, func):
        """Create a new _ChildDDS object.

        :param parent: Parent DDS object.
        :param func: Function.
        """
        super().__init__()
        self.paac = parent.paac

        if not isinstance(parent, _ChildDDS):
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
    """Run tests.

    :returns: None.
    """
    import doctest  # pylint: disable=C0415

    doctest.testmod()

    # Clean after testing
    to_be_removed = ["test.file", "test.txt"]
    for file_name in to_be_removed:
        try:
            os.remove(file_name)
        except OSError:
            pass


if __name__ == "__main__":
    _run_tests()
