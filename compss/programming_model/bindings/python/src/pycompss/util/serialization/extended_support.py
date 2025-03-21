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
PyCOMPSs Util - Serialization - Extended support.

This file contains the serialization extensions.
"""
import types

from pycompss.util.typing_helper import typing


class GeneratorIndicator:
    """GeneratorIndicator class."""


def pickle_generator(
    f_gen: typing.Generator,
    handler: typing.BinaryIO,
    serializer: types.ModuleType,
) -> None:
    """Pickle a generator and store the serialization result in a file.

    :param f_gen: Generator object.
    :param handler: Destination file for pickling generator.
    :param serializer: Serializer to use.
    """
    # Convert generator to list and pickle (less efficient but more reliable)
    # The tuple will be useful to determine when to call unplickle generator.
    # Using a key is weak, but otherwise, How can we difference a list from a
    # generator when receiving it?
    # At least, the key is complicated.
    gen_snapshot = (GeneratorIndicator(), list(f_gen))
    serializer.dump(gen_snapshot, handler)


def convert_to_generator(lst: list) -> typing.Generator:
    """Convert a list into a generator.

    :param lst: List to be converted.
    :return: the generator from the list.
    """
    return (n for n in lst)
