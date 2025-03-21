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
PyCOMPSs Util - Objects - Sizing algorithms.

This file contains the object sizing algorithm method.
"""

from __future__ import print_function

import reprlib
from collections import deque
from itertools import chain
from sys import getsizeof
from sys import stderr

from pycompss.util.typing_helper import typing

from collections.abc import Iterator


def _dict_handler(given_dict: dict) -> Iterator:
    """Convert dictionary to dictionary handler.

    :param given_dict: Dictionary.
    :return: Dictionary handler.
    """
    return chain.from_iterable(given_dict.items())


def _user_object_handler(user_object: typing.Any) -> Iterator:
    """Convert user object to dictionary handler.

    :param user_object: User object.
    :return: Dictionary handler.
    """
    return chain.from_iterable(user_object.__dict__.items())


def total_sizeof(
    given_object: typing.Any,
    handlers: typing.Optional[Iterator] = None,
    verbose: bool = False,
) -> int:
    """Calculate the size of an object.

    Returns the approximate memory footprint an object and all of its contents.
    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:
        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    :param given_object: Object to get its size.
    :param handlers: Handlers.
    :param verbose: Verbose mode [ True | False ] (default: False).
    :return: Total size of the object.
    """
    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: _dict_handler,
        set: iter,
        frozenset: iter,
    }  # type: typing.Dict[typing.Any, typing.Any]
    if type(given_object) not in all_handlers and hasattr(
        given_object, "__dict__"
    ):
        # It is something else include its __dict__
        all_handlers[type(given_object)] = _user_object_handler
    if handlers is not None:
        all_handlers.update(handlers)  # user handlers take precedence
    seen = set()  # track which object id's have already been seen
    default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(obj: typing.Any) -> int:
        """Calculate the size o the given object in bytes.

        :param obj: Object to measure
        :return: The object size in bytes
        """
        if id(obj) in seen:  # do not double count the same object
            return 0
        seen.add(id(obj))
        new_size = getsizeof(obj, default_size)

        if verbose:
            print(new_size, type(obj), reprlib.repr(obj), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(obj, typ):
                new_size += sum(map(sizeof, handler(obj)))
                break
        return new_size

    return sizeof(given_object)
