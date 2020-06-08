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
        - COMMUTATIVE
    2. TYPE.
        - FILE
        - BOOLEAN
        - STRING
        - INT
        - LONG
        - FLOAT
        - OBJECT
        - EXTERNAL_PSCO
        - EXTERNAL_STREAM
    3. IOSTREAM.
        - STDIN
        - STDOUT
        - STDERR
        - UNSPECIFIED
    4. PREFIX.
    5. Parameter.
    6. Aliases.
"""

# Content type format is <module_path>:<class_name>, separated by colon (':')
UNDEFINED_CONTENT_TYPE = "#UNDEFINED#:#UNDEFINED#"


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
    PREFIX = 'null'


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
                 depth=1,
                 is_file_collection=False,
                 content_type=UNDEFINED_CONTENT_TYPE,
                 weight="1.0",
                 keep_rename=False):
        self.type = p_type
        self.direction = p_direction
        self.stream = p_stream
        self.prefix = p_prefix
        self.object = p_object      # placeholder for parameter object
        self.file_name = file_name  # placeholder for object's serialized file
        self.is_future = is_future
        self.depth = depth          # Recursive depth for collections
        self.is_file_collection = is_file_collection
        self.content_type = content_type
        self.weight = weight
        self.keep_rename = keep_rename

    def __repr__(self):
        return 'Parameter(type=%s, direction=%s, stream=%s, prefix=%s\n' \
               '          object=%s\n' \
               '          content_type=%s\n' \
               '          weight=%s\n' \
               '          keep_rename=%s\n' \
               '          file_name=%s\n' \
               '          is_future=%s)' % (str(self.type),
                                            str(self.direction),
                                            str(self.stream),
                                            str(self.prefix),
                                            str(self.object),
                                            str(self.content_type),
                                            str(self.weight),
                                            str(self.keep_rename),
                                            str(self.file_name),
                                            str(self.is_future))


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
                 prefix=None,
                 content_type=UNDEFINED_CONTENT_TYPE,
                 weight="1.0",
                 keep_rename=False):
        self.name = name
        self.type = p_type
        self.file_name = file_name
        self.key = key
        self.content = content
        self.stream = stream
        self.prefix = prefix
        self.content_type = content_type
        self.weight = weight
        self.keep_rename = keep_rename

    def __repr__(self):
        return '\nParameter %s' % self.name + '\n' + \
               '\tType %s' % str(self.type) + '\n' + \
               '\tFile Name %s' % self.file_name + '\n' + \
               '\tKey %s' % str(self.key) + '\n' + \
               '\tContent %s' % str(self.content) + '\n' + \
               '\tStream %s' % str(self.stream) + '\n' + \
               '\tPrefix %s' % str(self.prefix) + '\n' + \
               '\tContent Type %s' % str(self.content_type) + '\n' + \
               '\tWeight %s' % str(self.weight) + '\n' + \
               '\tKeep Rename %s' % str(self.keep_rename) + '\n' + \
               '-' * 20 + '\n'


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

# Aliases for dirs

DIRECTORY = _Param('DIRECTORY')
DIRECTORY_IN = _Param('DIRECTORY_IN')
DIRECTORY_OUT = _Param('DIRECTORY_OUT')
DIRECTORY_INOUT = _Param('DIRECTORY_INOUT')

# Aliases for collections
COLLECTION = _Param('COLLECTION')
COLLECTION_IN = _Param('COLLECTION_IN')
COLLECTION_INOUT = _Param('COLLECTION_INOUT')
COLLECTION_OUT = _Param('COLLECTION_OUT')
COLLECTION_FILE = _Param('COLLECTION_FILE')
COLLECTION_FILE_IN = _Param('COLLECTION_FILE_IN')
COLLECTION_FILE_INOUT = _Param('COLLECTION_FILE_INOUT')
COLLECTION_FILE_OUT = _Param('COLLECTION_FILE_OUT')

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
Weight = 'weight'  # parameter weight
Keep_rename = 'keep_rename'  # parameter keep rename property

# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT = 9223372036854775807
JAVA_MIN_LONG = PYTHON_MIN_INT = -9223372036854775808
