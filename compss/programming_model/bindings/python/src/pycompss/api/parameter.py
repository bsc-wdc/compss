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
PyCOMPSs API - Parameter
========================
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
from pycompss.runtime.task.keys import ParamAliasKeys as _ParamAliasKeys
from pycompss.runtime.task.keys import ParamDictKeys as _ParamDictKeys
from pycompss.runtime.mpi.keys import MPILayoutKeys as _MPILayoutKeys

# Type definitions -> Numbers match both C and Java enums and are generated
#                     during the installation.
TYPE = _DataType


# Numbers match both C and Java enums
class DIRECTION(object):
    """
    Used as enum for direction types
    """
    IN = 0
    OUT = 1
    INOUT = 2
    CONCURRENT = 3
    COMMUTATIVE = 4
    IN_DELETE = 5


# Numbers match both C and Java enums
class IOSTREAM(object):
    """
    Used as enum for stream types
    """
    STDIN = 0
    STDOUT = 1
    STDERR = 2
    UNSPECIFIED = 3


# String that identifies the prefix
class PREFIX(object):
    """
    Used as enum for prefix
    """
    PREFIX = 'null'  # NOSONAR


class _Param(object):
    """
    Private class which hides the parameter key to be used.
    """
    def __init__(self, key):
        self.key = key


# Aliases for objects (just direction)
IN = _Param(_ParamAliasKeys.IN)
OUT = _Param(_ParamAliasKeys.OUT)
INOUT = _Param(_ParamAliasKeys.INOUT)
CONCURRENT = _Param(_ParamAliasKeys.CONCURRENT)
COMMUTATIVE = _Param(_ParamAliasKeys.COMMUTATIVE)
IN_DELETE = _Param(_ParamAliasKeys.IN_DELETE)

# Aliases for files with direction
FILE = _Param(_ParamAliasKeys.FILE)
FILE_IN = _Param(_ParamAliasKeys.FILE_IN)
FILE_OUT = _Param(_ParamAliasKeys.FILE_OUT)
FILE_INOUT = _Param(_ParamAliasKeys.FILE_INOUT)
FILE_CONCURRENT = _Param(_ParamAliasKeys.FILE_CONCURRENT)
FILE_COMMUTATIVE = _Param(_ParamAliasKeys.FILE_COMMUTATIVE)

# Aliases for files with stream
FILE_STDIN = _Param(_ParamAliasKeys.FILE_STDIN)
FILE_STDERR = _Param(_ParamAliasKeys.FILE_STDERR)
FILE_STDOUT = _Param(_ParamAliasKeys.FILE_STDOUT)

# Aliases for files with direction and stream
FILE_IN_STDIN = _Param(_ParamAliasKeys.FILE_IN_STDIN)
FILE_IN_STDERR = _Param(_ParamAliasKeys.FILE_IN_STDERR)
FILE_IN_STDOUT = _Param(_ParamAliasKeys.FILE_IN_STDOUT)
FILE_OUT_STDIN = _Param(_ParamAliasKeys.FILE_OUT_STDIN)
FILE_OUT_STDERR = _Param(_ParamAliasKeys.FILE_OUT_STDERR)
FILE_OUT_STDOUT = _Param(_ParamAliasKeys.FILE_OUT_STDOUT)
FILE_INOUT_STDIN = _Param(_ParamAliasKeys.FILE_INOUT_STDIN)
FILE_INOUT_STDERR = _Param(_ParamAliasKeys.FILE_INOUT_STDERR)
FILE_INOUT_STDOUT = _Param(_ParamAliasKeys.FILE_INOUT_STDOUT)
FILE_CONCURRENT_STDIN = _Param(_ParamAliasKeys.FILE_CONCURRENT_STDIN)
FILE_CONCURRENT_STDERR = _Param(_ParamAliasKeys.FILE_CONCURRENT_STDERR)
FILE_CONCURRENT_STDOUT = _Param(_ParamAliasKeys.FILE_CONCURRENT_STDOUT)
FILE_COMMUTATIVE_STDIN = _Param(_ParamAliasKeys.FILE_COMMUTATIVE_STDIN)
FILE_COMMUTATIVE_STDERR = _Param(_ParamAliasKeys.FILE_COMMUTATIVE_STDERR)
FILE_COMMUTATIVE_STDOUT = _Param(_ParamAliasKeys.FILE_COMMUTATIVE_STDOUT)

# Aliases for dirs
DIRECTORY = _Param(_ParamAliasKeys.DIRECTORY)
DIRECTORY_IN = _Param(_ParamAliasKeys.DIRECTORY_IN)
DIRECTORY_OUT = _Param(_ParamAliasKeys.DIRECTORY_OUT)
DIRECTORY_INOUT = _Param(_ParamAliasKeys.DIRECTORY_INOUT)

# Aliases for collections
COLLECTION = _Param(_ParamAliasKeys.COLLECTION)
COLLECTION_IN = _Param(_ParamAliasKeys.COLLECTION_IN)
COLLECTION_INOUT = _Param(_ParamAliasKeys.COLLECTION_INOUT)
COLLECTION_OUT = _Param(_ParamAliasKeys.COLLECTION_OUT)
COLLECTION_IN_DELETE = _Param(_ParamAliasKeys.COLLECTION_IN_DELETE)
COLLECTION_FILE = _Param(_ParamAliasKeys.COLLECTION_FILE)
COLLECTION_FILE_IN = _Param(_ParamAliasKeys.COLLECTION_FILE_IN)
COLLECTION_FILE_INOUT = _Param(_ParamAliasKeys.COLLECTION_FILE_INOUT)
COLLECTION_FILE_OUT = _Param(_ParamAliasKeys.COLLECTION_FILE_OUT)

# Aliases for dictionary collections
DICTIONARY = _Param(_ParamAliasKeys.DICT_COLLECTION)
DICTIONARY_IN = _Param(_ParamAliasKeys.DICT_COLLECTION_IN)
DICTIONARY_INOUT = _Param(_ParamAliasKeys.DICT_COLLECTION_INOUT)
DICTIONARY_OUT = _Param(_ParamAliasKeys.DICT_COLLECTION_OUT)
DICTIONARY_IN_DELETE = _Param(_ParamAliasKeys.DICT_COLLECTION_IN_DELETE)

# Aliases for streams
STREAM_IN = _Param(_ParamAliasKeys.STREAM_IN)
STREAM_OUT = _Param(_ParamAliasKeys.STREAM_OUT)

# Aliases for std IO streams (just stream direction)
STDIN = IOSTREAM.STDIN
STDOUT = IOSTREAM.STDOUT
STDERR = IOSTREAM.STDERR

# Aliases for parameter definition as dictionary
Type = _ParamDictKeys.TYPE                # parameter type
Direction = _ParamDictKeys.DIRECTION      # parameter direction
StdIOStream = _ParamDictKeys.STDIOSTREAM  # parameter stream
Prefix = _ParamDictKeys.PREFIX            # parameter prefix
Depth = _ParamDictKeys.DEPTH              # collection recursive depth
Weight = _ParamDictKeys.WEIGHT            # parameter weight
Keep_rename = _ParamDictKeys.KEEP_RENAME  # parameter keep rename property
Cache = _ParamDictKeys.CACHE              # enable/disable store in cache

# Aliases for collection layout for native mpi tasks
block_count = _MPILayoutKeys.block_count
block_length = _MPILayoutKeys.block_length
stride = _MPILayoutKeys.stride
