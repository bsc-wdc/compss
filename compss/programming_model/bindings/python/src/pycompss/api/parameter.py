#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
from pycompss.api.data_type import data_type
TYPE = data_type


# Numbers match both C and Java enums
class DIRECTION(object):
    """
    Used as enum for direction types
    """
    IN = 0
    OUT = 1
    INOUT = 2


# Numbers match both C and Java enums
class STREAM(object):
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


class Parameter(object):
    """
    Parameter class
    Used to group the type, direction and value of a parameter
    """

    def __init__(self, p_type=None, p_direction=DIRECTION.IN, p_stream=STREAM.UNSPECIFIED, p_prefix=PREFIX.PREFIX):
        self.type = p_type
        self.direction = p_direction
        self.stream = p_stream
        self.prefix = p_prefix
        self.object = None     # placeholder for parameter object
        self.file_name = None  # placeholder for object's serialized file
        self.is_future = False

    def __repr__(self):
        return "Parameter(type=%s, direction=%d, stream=%d, prefix=%s\n" \
               "          object=%s\n" \
               "          file_name=%s\n" \
               "          is_future=%s" % (str(self.type),
                                           self.direction,
                                           self.stream,
                                           self.prefix,
                                           str(self.object),
                                           str(self.file_name),
                                           str(self.is_future))


# Parameter conversion dictionary.
_param_conversion_dict_ = {
    'IN': {},
    'OUT': {
        'p_direction': DIRECTION.OUT
    },
    'INOUT': {
        'p_direction': DIRECTION.INOUT
    },
    'FILE': {
        'p_type': TYPE.FILE
    },
    'FILE_IN': {
        'p_type': TYPE.FILE
    },
    'FILE_OUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT
    },
    'FILE_INOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT
    },
    'FILE_STDIN': {
        'p_type': TYPE.FILE,
        'p_stream': STREAM.STDIN
    },
    'FILE_STDERR': {
        'p_type': TYPE.FILE,
        'p_stream': STREAM.STDERR
    },
    'FILE_STDOUT': {
        'p_type': TYPE.FILE,
        'p_stream': STREAM.STDOUT
    },
    'FILE_IN_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.IN,
        'p_stream': STREAM.STDIN
    },
    'FILE_IN_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.IN,
        'p_stream': STREAM.STDERR
    },
    'FILE_IN_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.IN,
        'p_stream': STREAM.STDOUT
    },
    'FILE_OUT_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT,
        'p_stream': STREAM.STDIN
    },
    'FILE_OUT_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT,
        'p_stream': STREAM.STDERR
    },
    'FILE_OUT_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT,
        'p_stream': STREAM.STDOUT
    },
    'FILE_INOUT_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT,
        'p_stream': STREAM.STDIN
    },
    'FILE_INOUT_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT,
        'p_stream': STREAM.STDERR
    },
    'FILE_INOUT_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT,
        'p_stream': STREAM.STDOUT
    }
}


def is_parameter(x):
    """
    Check if given object is a parameter.
    Avoids internal _param_ import.
    :param x: Object to check
    :return: Boolean
    """
    return isinstance(x, _param_)


def get_new_parameter(key):
    """
    Returns a brand new parameter (no copies!)
    :param key: Parameter key
    :return: Parameter object
    """
    return Parameter(**_param_conversion_dict_[key])


class _param_(object):
    """
    Private class which hides the parameter key to be used.
    """
    def __init__(self, key):
        self.key = key


# Aliases for objects (just direction)
IN = _param_('IN')
OUT = _param_('OUT')
INOUT = _param_('INOUT')

# Aliases for files with direction
FILE = _param_('FILE')
FILE_IN = _param_('FILE_IN')
FILE_OUT = _param_('FILE_OUT')
FILE_INOUT = _param_('FILE_INOUT')

# Aliases for files with stream
FILE_STDIN = _param_('FILE_STDIN')
FILE_STDERR = _param_('FILE_STDERR')
FILE_STDOUT = _param_('FILE_STDOUT')

# Aliases for files with direction and stream
FILE_IN_STDIN = _param_('FILE_IN_STDIN')
FILE_IN_STDERR = _param_('FILE_IN_STDERR')
FILE_IN_STDOUT = _param_('FILE_IN_STDOUT')
FILE_OUT_STDIN = _param_('FILE_OUT_STDIN')
FILE_OUT_STDERR = _param_('FILE_OUT_STDERR')
FILE_OUT_STDOUT = _param_('FILE_OUT_STDOUT')
FILE_INOUT_STDIN = _param_('FILE_INOUT_STDIN')
FILE_INOUT_STDERR = _param_('FILE_INOUT_STDERR')
FILE_INOUT_STDOUT = _param_('FILE_INOUT_STDOUT')

# Aliases for streams (just stream direction)
STDIN = STREAM.STDIN
STDOUT = STREAM.STDOUT
STDERR = STREAM.STDERR

# Aliases for parameter definition as dictionary
Type = 'type'            # parameter type
Direction = 'direction'  # parameter type
Stream = 'stream'        # parameter stream
Prefix = 'prefix'        # parameter prefix

# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT = 9223372036854775807
JAVA_MIN_LONG = PYTHON_MIN_INT = -9223372036854775808
