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
PyCOMPSs Util - Objects - Utils.

This file contains the utilities that could be needed for the objects.
"""

from pycompss.util.typing_helper import typing


def group_iterable(
    iterable: typing.Iterable, number: int
) -> typing.Iterator[typing.Any]:
    """Return a list of lists containing n elements.

    s -> [(s0, s1, s2, ..., sn-1),
          (sn, sn+1, sn+2, ..., s2n-1),
          (s2n, s2n+1, s2n+2, ..., s3n-1),
          ...,
          (sNn, sNn+1, sNn+2, ..., sMn-1)]"
    :param iterable: Iterable to group.
    :param number: Number of elements per group.
    :return: A list of lists where the inner contain n elements.
    """
    return zip(*[iter(iterable)] * number)
