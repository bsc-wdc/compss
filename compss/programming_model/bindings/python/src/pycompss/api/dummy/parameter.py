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
    This file contains the clases needed for the parameter definition.
    1. DIRECTION.
        - IN
        - OUT
        - INOUT
        - CONCURRENT
    2. TYPE.
        - FILE
        - BOOLEAN
        - STRING
        - INT
        - LONG
        - FLOAT
        - OBJECT
        - PSCO
        - EXTERNAL_PSCO
    3. STREAM.
        - STDIN
        - STDOUT
        - STDERR
        - UNSPECIFIED
    4. PREFIX.
    5. Parameter.
    6. Aliases.
"""


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
    PREFIX = "null"


# CAUTION! This is automatically generated in the data_type file.
# Please, keep updated.
class TYPE(object):
    BOOLEAN = 0
    CHAR = 1
    BYTE = 2
    SHORT = 3
    INT = 4
    LONG = 5
    FLOAT = 6
    DOUBLE = 7
    STRING = 8
    FILE = 9
    OBJECT = 10
    PSCO = 11
    EXTERNAL_PSCO = 12
    BINDING_OBJECT = 13
    WCHAR = 14
    WSTRING = 15
    LONGLONG = 16
    VOID = 17
    ANY = 18
    ARRAY_CHAR = 19
    ARRAY_BYTE = 20
    ARRAY_SHORT = 21
    ARRAY_INT = 22
    ARRAY_LONG = 23
    ARRAY_FLOAT = 24
    ARRAY_DOUBLE = 25
    COLLECTION = 26
    STREAM = 27
    EXTERNAL_STREAM = 28
    ENUM = 29
    NULL = 30


class Parameter(object):
    """
    Parameter class
    Used to group the type, direction and value of a parameter
    """

    def __init__(self,
                 p_type=None,
                 p_direction=DIRECTION.IN,
                 p_stream=IOSTREAM.UNSPECIFIED,
                 p_prefix=PREFIX.PREFIX,
                 p_object=None,
                 file_name=None,
                 is_future=False,
                 depth=1):
        self.type = p_type
        self.direction = p_direction
        self.stream = p_stream
        self.prefix = p_prefix
        self.object = p_object      # placeholder for parameter object
        self.file_name = file_name  # placeholder for object's serialized file
        self.is_future = is_future
        self.depth = depth          # Recursive depth for collections


class TaskParameter(object):
    """
    An internal wrapper for parameters. It makes it easier for the task
    decorator to know any aspect of the parameters (should they be updated
    or can changes be discarded, should they be deserialized or read from
    some storage, etc.)
    """

    def __init__(self,
                 name=None,
                 p_type=None,
                 file_name=None,
                 key=None,
                 content=None,
                 stream=None,
                 prefix=None):
        self.name = name
        self.type = p_type
        self.file_name = file_name
        self.key = key
        self.content = content
        self.stream = stream
        self.prefix = prefix


class _Param(object):
    """
    Private class which hides the parameter key to be used.
    """

    def __init__(self, key):
        self.key = key


# Aliases for objects (just direction)
IN = _Param('IN')
OUT = _Param('OUT')
INOUT = _Param('INOUT')
CONCURRENT = _Param('CONCURRENT')
COMMUTATIVE = _Param('COMMUTATIVE')

# Aliases for files with direction
FILE = _Param('FILE')
FILE_IN = _Param('FILE_IN')
FILE_OUT = _Param('FILE_OUT')
FILE_INOUT = _Param('FILE_INOUT')
FILE_CONCURRENT = _Param('FILE_CONCURRENT')
FILE_COMMUTATIVE = _Param('FILE_COMMUTATIVE')

# Aliases for files with stream
FILE_STDIN = _Param('FILE_STDIN')
FILE_STDERR = _Param('FILE_STDERR')
FILE_STDOUT = _Param('FILE_STDOUT')

# Aliases for files with direction and stream
FILE_IN_STDIN = _Param('FILE_IN_STDIN')
FILE_IN_STDERR = _Param('FILE_IN_STDERR')
FILE_IN_STDOUT = _Param('FILE_IN_STDOUT')
FILE_OUT_STDIN = _Param('FILE_OUT_STDIN')
FILE_OUT_STDERR = _Param('FILE_OUT_STDERR')
FILE_OUT_STDOUT = _Param('FILE_OUT_STDOUT')
FILE_INOUT_STDIN = _Param('FILE_INOUT_STDIN')
FILE_INOUT_STDERR = _Param('FILE_INOUT_STDERR')
FILE_INOUT_STDOUT = _Param('FILE_INOUT_STDOUT')
FILE_CONCURRENT_STDIN = _Param('FILE_CONCURRENT_STDIN')
FILE_CONCURRENT_STDERR = _Param('FILE_CONCURRENT_STDERR')
FILE_CONCURRENT_STDOUT = _Param('FILE_CONCURRENT_STDOUT')
FILE_COMMUTATIVE_STDIN = _Param('FILE_COMMUTATIVE_STDIN')
FILE_COMMUTATIVE_STDERR = _Param('FILE_COMMUTATIVE_STDERR')
FILE_COMMUTATIVE_STDOUT = _Param('FILE_COMMUTATIVE_STDOUT')

# Aliases for collections
COLLECTION = _Param('COLLECTION')
COLLECTION_IN = _Param('COLLECTION_IN')
COLLECTION_INOUT = _Param('COLLECTION_INOUT')

# Aliases for streams
STREAM_IN = _Param("STREAM_IN")
STREAM_OUT = _Param("STREAM_OUT")

# Aliases for std IO streams (just stream direction)
STDIN = IOSTREAM.STDIN
STDOUT = IOSTREAM.STDOUT
STDERR = IOSTREAM.STDERR

# Aliases for parameter definition as dictionary
Type = 'type'  # parameter type
Direction = 'direction'  # parameter type
StdIOStream = 'stream'  # parameter stream
Prefix = 'prefix'  # parameter prefix
Depth = 'depth'  # collection recursive depth

# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT = 9223372036854775807
JAVA_MIN_LONG = PYTHON_MIN_INT = -9223372036854775808
