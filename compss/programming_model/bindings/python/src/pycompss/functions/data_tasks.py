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
PyCOMPSs Functions: Data generators.

This file defines the common data producing functions.

WARNING: This file can not be compiled with mypyc since contains tasks.
"""

import random

from pycompss.api.task import task
from pycompss.util.typing_helper import typing


@task(returns=list)
def gen_random(
    size: int, frag_size: int, seed: int
) -> typing.List[typing.List[float]]:
    """Generate random distribution fragment.

    :param size: Size
    :param frag_size: Fragment size
    :param seed: Random seed
    :return: a fragment of elements
    """
    random.seed(seed)
    return [[random.random() for _ in range(size)] for _ in range(frag_size)]


@task(returns=list)
def gen_normal(
    size: int, frag_size: int, seed: int
) -> typing.List[typing.List[float]]:
    """Generate normal distribution fragment.

    :param size: Size
    :param frag_size: Fragment size
    :param seed: Random seed
    :return: a fragment of elements
    """
    random.seed(seed)
    return [
        [random.gauss(mu=0.0, sigma=1.0) for _ in range(size)]
        for _ in range(frag_size)
    ]


@task(returns=list)
def gen_uniform(
    size: int, frag_size: int, seed: int
) -> typing.List[typing.List[float]]:
    """Generate uniform distribution fragment.

    :param size: Size
    :param frag_size: Fragment size
    :param seed: Random seed
    :return: a fragment of elements
    """
    random.seed(seed)
    return [
        [random.uniform(-1.0, 1.0) for _ in range(size)]
        for _ in range(frag_size)
    ]
