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
"""

from pycompss.functions.data_tasks import gen_normal as _gen_normal
from pycompss.functions.data_tasks import gen_random as _gen_random
from pycompss.functions.data_tasks import gen_uniform as _gen_uniform
from pycompss.util.typing_helper import typing


def generator(
    size: typing.Tuple[int, int],
    num_frag: int,
    seed: int = 0,
    distribution: str = "random",
    wait: bool = False,
) -> typing.Optional[typing.List[typing.Any]]:
    """Generate a list of fragments with random data.

    :param size: Size (numElements, dim)
    :param num_frag: Dataset number of fragments
    :param seed: Random seed. Default None, system time is used-
    :param distribution: Random, normal, uniform
    :param wait: If we want to wait for result. Default False
    :return: Random dataset
    """
    data = None
    frag_size = int(size[0] / num_frag)
    if distribution == "random":
        data = [_gen_random(size[1], frag_size, seed) for _ in range(num_frag)]
    elif distribution == "normal":
        data = [_gen_normal(size[1], frag_size, seed) for _ in range(num_frag)]
    elif distribution == "uniform":
        data = [
            _gen_uniform(size[1], frag_size, seed) for _ in range(num_frag)
        ]
    if wait:
        from pycompss.api.api import (  # pylint: disable=C0415
            # disable=import-outside-toplevel
            compss_wait_on,
        )

        data = compss_wait_on(data)
    return data


def chunks(
    lst: list, number: int, balanced: bool = False
) -> typing.Iterator[typing.List[int]]:
    """List splitter into fragments.

    WARNING: Not tested!

    :param lst: List of data to be chunked
    :param number: length of the fragments
    :param balanced: True to generate balanced fragments
    :return: yield fragments of size n from lst
    """
    if not balanced or not len(lst) % number:
        for i in range(0, len(lst), number):
            yield lst[i : i + number]  # noqa: E203
    else:
        rest = len(lst) % number
        start = 0
        while rest:
            yield lst[start : start + number + 1]  # noqa: E203
            rest -= 1
            start += number + 1
        for i in range(start, len(lst), number):
            yield lst[i : i + number]  # noqa: E203
