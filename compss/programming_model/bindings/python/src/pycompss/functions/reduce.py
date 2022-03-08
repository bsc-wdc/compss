#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
==========================
    This file defines the common reduce functions.
"""

from pycompss.util.typing_helper import typing
from collections import deque


def merge_reduce(f: typing.Callable, data: list) -> typing.Any:
    """Apply function cumulatively to the items of data,
    from left to right in binary tree structure, so as to
    reduce the data to a single value.

    :param f: function to apply to reduce data
    :param data: List of items to be reduced
    :return: result of reduce the data to a single value
    """
    q = deque(range(len(data)))
    while len(q):
        x = q.popleft()
        if len(q):
            y = q.popleft()
            data[x] = f(data[x], data[y])
            q.append(x)
        else:
            return data[x]


def merge_n_reduce(f: typing.Callable, arity: int, data, list) -> typing.Any:
    """Apply f cumulatively to the items of data,
    from left to right in n-tree structure, so as to
    reduce the data.

    :param f: function to apply to reduce data
    :param arity: Number of elements in group
    :param data: List of items to be reduced
    :return: List of results
    """
    while len(data) > 1:
        data_chunk = data[:arity]
        data = data[arity:]
        data.append(f(*data_chunk))
    return data[0]
