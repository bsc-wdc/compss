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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Functions: Reduce
===================================
    This file defines the common reduce fs.
"""

from pycompss.runtime.commons import IS_PYTHON3


def merge_reduce(f, data):
    """
    Apply f cumulatively to the items of data,
    from left to right in binary tree structure, so as to
    reduce the data to a single value.

    :param f: function to apply to reduce data
    :param data: List of items to be reduced
    :return: result of reduce the data to a single value
    """

    from collections import deque
    q = deque(range(len(data)))
    new_data = data[:]
    while len(q):
        x = q.popleft()
        if len(q):
            y = q.popleft()
            new_data[x] = f(new_data[x], new_data[y])
            q.append(x)
        else:
            return new_data[x]


def merge_n_reduce(f, data):
    """
    Apply f cumulatively to the items of data,
    from left to right in n-tree structure, so as to
    reduce the data.

    :param f: function to apply to reduce data
    :param data: List of items to be reduced
    :return: List of results
    """

    from collections import deque
    q = deque(range(len(data)))
    num_args = f.func_code.co_argcount
    while len(q) >= num_args:
        args = [q.popleft() for _ in range(num_args)]
        data[args[0]] = f(*[data[i] for i in args])
        q.append(args[0])
    else:
        args = [q.popleft() for _ in range(len(q))]
        return [data[i] for i in args]


def simple_reduce(f, data):
    """
    Apply f of two arguments cumulatively to the items
    of data, from left to right, so as to reduce the iterable
    to a single value.

    :param f: function to apply to reduce data
    :param data: List of items to be reduced
    :return: result of reduce the data to a single value
    """

    try:
        if IS_PYTHON3:
            import functools
            return functools.reduce(f, data)
        else:
            return reduce(f, data)
    except Exception as e:
        raise e
