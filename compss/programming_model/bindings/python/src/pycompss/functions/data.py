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
PyCOMPSs Functions: Data generators
===================================
    This file defines the common data producing functions.
"""

import typing

from pycompss.functions.data_tasks import gen_random as _gen_random
from pycompss.functions.data_tasks import gen_normal as _gen_normal
from pycompss.functions.data_tasks import gen_uniform as _gen_uniform


def generator(size, num_frag, seed=0, distribution="random", wait=False):
    # type: (typing.Tuple[int, int], int, int, str, bool) -> typing.Any
    """ Data generator.

    Generates a list of fragments.

    :param size: (numElements, dim)
    :param num_frag: dataset number of fragments
    :param seed: random seed. Default None, system time is used-
    :param distribution: random, normal, uniform
    :param wait: if we want to wait for result. Default False
    :return: random dataset
    """
    data = None
    frag_size = int(size[0] / num_frag)
    if distribution == "random":
        data = [_gen_random(size[1], frag_size, seed)
                for _ in range(num_frag)]
    elif distribution == "normal":
        data = [_gen_normal(size[1], frag_size, seed)
                for _ in range(num_frag)]
    elif distribution == "uniform":
        data = [_gen_uniform(size[1], frag_size, seed)
                for _ in range(num_frag)]
    if wait:
        from pycompss.api.api import compss_wait_on
        data = compss_wait_on(data)
    return data


def chunks(lst, n, balanced=False):
    # type: (list, int, bool) -> typing.Iterator[typing.Any]
    """ List splitter into fragments.

    WARNING: Not tested!

    :param lst: List of data to be chunked
    :param n: length of the fragments
    :param balanced: True to generate balanced fragments
    :return: yield fragments of size n from lst
    """
    if not balanced or not len(lst) % n:
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    else:
        rest = len(lst) % n
        start = 0
        while rest:
            yield lst[start: start + n + 1]
            rest -= 1
            start += n + 1
        for i in range(start, len(lst), n):
            yield lst[i:i + n]
