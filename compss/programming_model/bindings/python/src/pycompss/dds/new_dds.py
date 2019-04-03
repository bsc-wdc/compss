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
import os
from collections import deque, defaultdict
from itertools import chain

from pycompss.api.api import compss_wait_on, compss_barrier

from pycompss.dds.new_tasks import *
from operator import add


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
            return

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
            temp = get_next_partition(iterator, start, end)
            self.partitions.append(temp)
            start = end

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

            self.partitions.append(task_read_files(partition_files))
            start = end

        return self

    def map(self, func):

        def _map(iterator):
            return map(func, iterator)

        return self.map_partitions(_map)

    def map_partitions(self, func):
        return ChildDDS(self, func)

    def map_and_flatten(self, f, *args, **kwargs):
        """
        Just because flat_map is an ugly name.
        Apply a function to each element and extend the derived element(s) if
        possible.
        :param f: A function that should return a list, tuple or another kind of
                  iterable
        :return:

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

        def _filter(iterator):
            return filter(f, iterator)

        return self.map_partitions(_filter)

    def reduce(self, f, initial, arity=-1):
        """

        :param f:
        :param initial:
        :param arity:
        :return:
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

    def count_by_value(self, arity=2, as_dict=False):
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
        local_results = self.map_partitions(count_partition)

        # Create a deque from partitions and start reduce
        future_objects = deque(local_results.collect(True))
        ret = []
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

        length = len(self.partitions)
        for i in range(length):
            ret.append(task_dict_to_list(branch[0], length, i))

        self.partitions = ret
        return self

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

    def foreach(self, f):
        """
        Apply a function to each element of this data set without returning
        anything.
        :param f: a void function
        :return: null

        >>> def dummy(x): print(x)
        >>> DDS().load(range(2), 2).foreach(dummy)
        1
        2
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
        res = list()
        # Future objects cannot be extended for now...
        if future_objects:
            return self.partitions

        self.partitions = compss_wait_on(self.partitions)
        if not keep_partitions:
            for p in self.partitions:
                # p = compss_wait_on(p)
                res.extend(p)
        else:
            for p in self.partitions:
                res.append(list(p))
        return res


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

        return tree_reduce_dicts(locally_combined, merger_function, collect,
                                 total_parts=total_parts)

    def reduce_by_key(self, f, collect=False):
        """
        """
        return self.combine_by_key((lambda x: x), f, f, collect=collect)


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
            print(total_parts)
            return []


def _run_tests():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    _run_tests()
