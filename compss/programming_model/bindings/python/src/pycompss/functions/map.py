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
PyCOMPSs Functions: Map.

This file defines the common map functions.
"""

from pycompss.util.typing_helper import typing


def __get_chunks(
    iterable: typing.Union[list, tuple], chunksize: int
) -> typing.Iterator[typing.Union[list, tuple]]:
    """Yield n-sized chunks from iterable.

    :param iterable: Iterable of items to be reduced.
    :param chunksize: Elements to consider in a chunk.
    :return: List of lists that contain chunksize elements.
    """
    for i in range(0, len(list(iterable)), chunksize):
        yield iterable[i : i + chunksize]  # noqa: E203


def map(  # pylint: disable=redefined-builtin
    func: typing.Callable,
    iterable: typing.Union[list, tuple],
    chunksize: typing.Optional[int] = None,
) -> typing.Union[list, tuple]:
    """Apply function cumulatively to the items of data.

    Provides the multiprocessing pool map interface.

    :param func: function to apply to reduce data.
    :param iterable: Iterable of items to be reduced.
    :param chunksize: Elements to consider in a chunk.
    :return: result of applying func to all iterable elements.
    """
    from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
        compss_wait_on,
    )

    result = []
    if chunksize:
        for i in __get_chunks(iterable, chunksize):
            result.append(func(i))
    else:
        for i in iterable:
            result.append(func(i))
    result = compss_wait_on(result)
    return result
