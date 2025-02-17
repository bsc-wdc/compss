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
PyCOMPSs API - Parameter definitions.

This file contains the classes needed for the parameter definition.
1. DIRECTION.
    - IN
    - OUT
    - INOUT
    - CONCURRENT
    - COMMUTATIVE
    - IN_DELETE
2. TYPE.
    - FILE
    - BOOLEAN
    - STRING
    - INT
    - LONG
    - FLOAT
    - OBJECT
    - COLLECTION
    - DICTIONARY
    - EXTERNAL_PSCO
    - EXTERNAL_STREAM
    - ... (check _DataType generated during installation)
3. IOSTREAM.
    - STDIN
    - STDOUT
    - STDERR
    - UNSPECIFIED
4. PREFIX.
5. ALIASES.
"""

from pycompss.api.commons.data_type import DataType as _DataType
from pycompss.runtime.mpi.keys import MPI_LAYOUT_KEYS as _MPI_LAYOUT_KEYS
from pycompss.runtime.task.keys import PARAM_ALIAS_KEYS as _PARAM_ALIAS_KEYS
from pycompss.runtime.task.keys import PARAM_DICT_KEYS as _PARAM_DICT_KEYS

# Type definitions -> Numbers match both C and Java enums and are generated
#                     during the installation.
TYPE = _DataType


# Numbers match both C and Java enums
class SupportedDirections:  # pylint: disable=too-few-public-methods
    """Used as enum for direction types."""

    IN = 0
    OUT = 1
    INOUT = 2
    CONCURRENT = 3
    COMMUTATIVE = 4
    IN_DELETE = 5


# Numbers match both C and Java enums
class SupportedIoStreams:  # pylint: disable=too-few-public-methods
    """Used as enum for stream types."""

    STDIN = 0
    STDOUT = 1
    STDERR = 2
    UNSPECIFIED = 3


# String that identifies the prefix
class SupportedPrefixes:  # pylint: disable=too-few-public-methods
    """Used as enum for prefix."""

    PREFIX = "null"  # NOSONAR


DIRECTION = SupportedDirections()
IOSTREAM = SupportedIoStreams()
PREFIX = SupportedPrefixes()


class _Param:  # pylint: disable=too-few-public-methods
    """Private class which hides the parameter key to be used."""

    __slots__ = ["key"]

    def __init__(self, key: str) -> None:
        self.key = key


# Aliases for objects (just direction)
IN = _Param(_PARAM_ALIAS_KEYS.IN)
OUT = _Param(_PARAM_ALIAS_KEYS.OUT)
INOUT = _Param(_PARAM_ALIAS_KEYS.INOUT)
CONCURRENT = _Param(_PARAM_ALIAS_KEYS.CONCURRENT)
COMMUTATIVE = _Param(_PARAM_ALIAS_KEYS.COMMUTATIVE)
IN_DELETE = _Param(_PARAM_ALIAS_KEYS.IN_DELETE)

# Aliases for files with direction
FILE = _Param(_PARAM_ALIAS_KEYS.FILE)
FILE_IN = _Param(_PARAM_ALIAS_KEYS.FILE_IN)
FILE_OUT = _Param(_PARAM_ALIAS_KEYS.FILE_OUT)
FILE_INOUT = _Param(_PARAM_ALIAS_KEYS.FILE_INOUT)
FILE_CONCURRENT = _Param(_PARAM_ALIAS_KEYS.FILE_CONCURRENT)
FILE_COMMUTATIVE = _Param(_PARAM_ALIAS_KEYS.FILE_COMMUTATIVE)

# Aliases for files with stream
FILE_STDIN = _Param(_PARAM_ALIAS_KEYS.FILE_STDIN)
FILE_STDERR = _Param(_PARAM_ALIAS_KEYS.FILE_STDERR)
FILE_STDOUT = _Param(_PARAM_ALIAS_KEYS.FILE_STDOUT)

# Aliases for files with direction and stream
FILE_IN_STDIN = _Param(_PARAM_ALIAS_KEYS.FILE_IN_STDIN)
FILE_IN_STDERR = _Param(_PARAM_ALIAS_KEYS.FILE_IN_STDERR)
FILE_IN_STDOUT = _Param(_PARAM_ALIAS_KEYS.FILE_IN_STDOUT)
FILE_OUT_STDIN = _Param(_PARAM_ALIAS_KEYS.FILE_OUT_STDIN)
FILE_OUT_STDERR = _Param(_PARAM_ALIAS_KEYS.FILE_OUT_STDERR)
FILE_OUT_STDOUT = _Param(_PARAM_ALIAS_KEYS.FILE_OUT_STDOUT)
FILE_INOUT_STDIN = _Param(_PARAM_ALIAS_KEYS.FILE_INOUT_STDIN)
FILE_INOUT_STDERR = _Param(_PARAM_ALIAS_KEYS.FILE_INOUT_STDERR)
FILE_INOUT_STDOUT = _Param(_PARAM_ALIAS_KEYS.FILE_INOUT_STDOUT)
FILE_CONCURRENT_STDIN = _Param(_PARAM_ALIAS_KEYS.FILE_CONCURRENT_STDIN)
FILE_CONCURRENT_STDERR = _Param(_PARAM_ALIAS_KEYS.FILE_CONCURRENT_STDERR)
FILE_CONCURRENT_STDOUT = _Param(_PARAM_ALIAS_KEYS.FILE_CONCURRENT_STDOUT)
FILE_COMMUTATIVE_STDIN = _Param(_PARAM_ALIAS_KEYS.FILE_COMMUTATIVE_STDIN)
FILE_COMMUTATIVE_STDERR = _Param(_PARAM_ALIAS_KEYS.FILE_COMMUTATIVE_STDERR)
FILE_COMMUTATIVE_STDOUT = _Param(_PARAM_ALIAS_KEYS.FILE_COMMUTATIVE_STDOUT)

# Aliases for dirs
DIRECTORY = _Param(_PARAM_ALIAS_KEYS.DIRECTORY)
DIRECTORY_IN = _Param(_PARAM_ALIAS_KEYS.DIRECTORY_IN)
DIRECTORY_OUT = _Param(_PARAM_ALIAS_KEYS.DIRECTORY_OUT)
DIRECTORY_INOUT = _Param(_PARAM_ALIAS_KEYS.DIRECTORY_INOUT)

# Aliases for collections
COLLECTION = _Param(_PARAM_ALIAS_KEYS.COLLECTION)
COLLECTION_IN = _Param(_PARAM_ALIAS_KEYS.COLLECTION_IN)
COLLECTION_INOUT = _Param(_PARAM_ALIAS_KEYS.COLLECTION_INOUT)
COLLECTION_OUT = _Param(_PARAM_ALIAS_KEYS.COLLECTION_OUT)
COLLECTION_IN_DELETE = _Param(_PARAM_ALIAS_KEYS.COLLECTION_IN_DELETE)
COLLECTION_FILE = _Param(_PARAM_ALIAS_KEYS.COLLECTION_FILE)
COLLECTION_FILE_IN = _Param(_PARAM_ALIAS_KEYS.COLLECTION_FILE_IN)
COLLECTION_FILE_INOUT = _Param(_PARAM_ALIAS_KEYS.COLLECTION_FILE_INOUT)
COLLECTION_FILE_OUT = _Param(_PARAM_ALIAS_KEYS.COLLECTION_FILE_OUT)

# Aliases for dictionary collections
DICTIONARY = _Param(_PARAM_ALIAS_KEYS.DICT_COLLECTION)
DICTIONARY_IN = _Param(_PARAM_ALIAS_KEYS.DICT_COLLECTION_IN)
DICTIONARY_INOUT = _Param(_PARAM_ALIAS_KEYS.DICT_COLLECTION_INOUT)
DICTIONARY_OUT = _Param(_PARAM_ALIAS_KEYS.DICT_COLLECTION_OUT)
DICTIONARY_IN_DELETE = _Param(_PARAM_ALIAS_KEYS.DICT_COLLECTION_IN_DELETE)

# Aliases for streams
STREAM_IN = _Param(_PARAM_ALIAS_KEYS.STREAM_IN)
STREAM_OUT = _Param(_PARAM_ALIAS_KEYS.STREAM_OUT)

# Aliases for std IO streams (just stream direction)
STDIN = IOSTREAM.STDIN
STDOUT = IOSTREAM.STDOUT
STDERR = IOSTREAM.STDERR

# Aliases for parameter definition as dictionary
# - Parameter type
Type = _PARAM_DICT_KEYS.TYPE  # pylint: disable=invalid-name
# - Parameter direction
Direction = _PARAM_DICT_KEYS.DIRECTION  # pylint: disable=invalid-name
# - Parameter stream
StdIOStream = _PARAM_DICT_KEYS.STDIOSTREAM  # pylint: disable=invalid-name
# - Parameter prefix
Prefix = _PARAM_DICT_KEYS.PREFIX  # pylint: disable=invalid-name
# - Collection recursive depth
Depth = _PARAM_DICT_KEYS.DEPTH  # pylint: disable=invalid-name
# - Parameter weight
Weight = _PARAM_DICT_KEYS.WEIGHT  # pylint: disable=invalid-name
# - Parameter keep rename property
Keep_rename = _PARAM_DICT_KEYS.KEEP_RENAME  # pylint: disable=invalid-name
# - Enable/disable store in cache
Cache = _PARAM_DICT_KEYS.CACHE  # pylint: disable=invalid-name

# Aliases for collection layout for native mpi tasks
block_count = _MPI_LAYOUT_KEYS.block_count  # pylint: disable=invalid-name
block_length = _MPI_LAYOUT_KEYS.block_length  # pylint: disable=invalid-name
stride = _MPI_LAYOUT_KEYS.stride  # pylint: disable=invalid-name
