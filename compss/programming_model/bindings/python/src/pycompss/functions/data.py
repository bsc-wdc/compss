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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Functions: Data generators
===================================
    This file defines the common data producing functions.
"""

import random
from pycompss.api.task import task


def chunks(lst, n, balanced=False):
    """
    Generator to chunk data.

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


@task(returns=list)
def _gen_random(size, frag_size, seed):
    """
    Random generator.

    :param size: Size
    :param frag_size: Fragment size
    :param seed: Random seed
    :return: a fragment of elements
    """

    random.seed(seed)
    return [[random.random() for _ in range(size)] for _ in range(frag_size)]


@task(returns=list)
def _gen_normal(size, frag_size, seed):
    """
    Normal generator.

    :param size: Size
    :param frag_size: Fragment size
    :param seed: Random seed
    :return: a fragment of elements
    """

    random.seed(seed)
    return [[random.gauss(mu=0.0, sigma=1.0) for _ in range(size)]
            for _ in range(frag_size)]


@task(returns=list)
def _gen_uniform(size, frag_size, seed):
    """
    Uniform generator.

    :param size: Size
    :param frag_size: Fragment size
    :param seed: Random seed
    :return: a fragment of elements
    """

    random.seed(seed)
    return [[random.uniform(-1.0, 1.0) for _ in range(size)]
            for _ in range(frag_size)]


def generator(size, num_frag, seed=None, distribution='random', wait=False):
    """
    Data generator.

    :param size: (numElements,dim)
    :param num_frag: dataset's number of fragments
    :param seed: random seed. Default None, system time is used-
    :param distribution: random, normal, uniform
    :param wait: if we want to wait for result. Default False
    :return: random dataset
    """

    data = None
    frag_size = size[0] / num_frag
    if distribution == 'random':
        data = [_gen_random(size[1], frag_size, seed, frag_size * i)
                for i in range(num_frag)]
    elif distribution == 'normal':
        data = [_gen_normal(size[1], frag_size, seed, frag_size * i)
                for i in range(num_frag)]
    elif distribution == 'uniform':
        data = [_gen_uniform(size[1], frag_size, seed, frag_size * i)
                for i in range(num_frag)]
    if wait:
        from pycompss.api.api import compss_wait_on
        data = compss_wait_on(data)
    return data
