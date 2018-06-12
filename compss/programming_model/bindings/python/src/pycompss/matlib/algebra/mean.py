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
PyCOMPSs Mathematical Library: Algebra: Mean
============================================
    This file contains the arithmetic mean algorithm.
"""

from pycompss.api.task import task
from pycompss.functions.reduce import merge_reduce


def _list_lenght(l):
    """
    Recursive function to get the size of any list

    :return: List length
    """

    if l:
        if not isinstance(l[0], list):
            return 1 + _list_lenght(l[1:])
        else:
            return _list_lenght(l[0]) + _list_lenght(l[1:])
    return 0


@task(returns=float)
def _mean(data, n):
    """
    Calculate the mean of a list,

    :param data: List of elements
    :param n: Number of elements
    :return: Mean
    """

    return sum(data) / float(n)


def mean(data, wait=False):
    """
    Arithmetic mean.

    :param data: chunked data
    :param wait: if we want to wait for result. Default False
    :return: mean of data.
    """

    n = _list_lenght(data)
    result = merge_reduce(reduce_add, [_mean(x, n) for x in data])
    if wait:
        from pycompss.api.api import compss_wait_on
        result = compss_wait_on(result)
    return result
