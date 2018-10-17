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
from collections import deque, defaultdict
from itertools import chain

from pycompss.api.api import compss_wait_on, compss_barrier

from new_tasks import *
from tasks import get_next_partition, marker
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

    def map(self, func):

        def _map(iterator):
            return map(func, iterator)

        return self.map_partitions(_map)

    def map_partitions(self, func):
        return ChildDDS(self, func)

    def map_and_flatten(self, f):
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
        from itertools import chain

        def dummy(partition):
            return list(chain.from_iterable(map(f, partition)))

        return self.map_partitions(dummy)

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

    def count_by_value(self, collect=True):
        """
        """

        def count_partition(partition):
            counts = defaultdict(int)
            for obj in partition:
                counts[obj] += 1
            return counts

        local_results = self.map_partitions(count_partition).\
            collect(future_objects=True)

        return tree_reduce_dicts(local_results, add, True)

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

    def collect(self, future_objects=False):
        ret = list()
        res = list()
        if not callable(self.func):
            return chain.from_iterable(self.partitions)

        for part in self.partitions:
            temp = map_partition(self.func, part)
            ret.append(temp)

        if future_objects:
            return ret

        for item in ret:
            res.extend(compss_wait_on(item))

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
