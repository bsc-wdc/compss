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
PyCOMPSs Functions: Reduce.

This file defines the common reduction functions.
"""

from collections import deque

from pycompss.util.typing_helper import typing


def merge_reduce(function: typing.Callable, data: list) -> typing.Any:
    """Reduce the given data applying f as an inverted binary tree.

    Apply function cumulatively to the items of data, from left to right in
    binary tree structure, so as to reduce the data to a single value.

    :param function: function to apply to reduce data
    :param data: List of items to be reduced
    :return: result of reduce the data to a single value
    """
    queue = deque(range(len(data)))
    while len(queue):
        x_value = queue.popleft()
        if len(queue):
            y_value = queue.popleft()
            data[x_value] = function(data[x_value], data[y_value])
            queue.append(x_value)
        else:
            return data[x_value]


def merge_n_reduce(
    function: typing.Callable, arity: int, data: list
) -> typing.Any:
    """Reduce the given data applying f as an inverted N-ary tree.

    Apply f cumulatively to the items of data, from left to right in n-tree
    structure, to reduce the data.

    :param function: function to apply to reduce data
    :param arity: Number of elements in group
    :param data: List of items to be reduced
    :return: List of results
    """
    while len(data) > 1:
        data_chunk = data[:arity]
        data = data[arity:]
        data.append(function(*data_chunk))
    return data[0]
