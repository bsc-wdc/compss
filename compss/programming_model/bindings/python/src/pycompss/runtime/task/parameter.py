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
PyCOMPSs runtime - Parameter
============================
    This file contains the classes needed for the parameter definition.
"""

import re
import copy

from pycompss.runtime.commons import IS_PYTHON3
from pycompss.api.parameter import TYPE
from pycompss.api.parameter import DIRECTION
from pycompss.api.parameter import IOSTREAM
from pycompss.api.parameter import PREFIX
from pycompss.api.parameter import Type
from pycompss.api.parameter import Direction
from pycompss.api.parameter import Prefix
from pycompss.api.parameter import StdIOStream
from pycompss.api.parameter import Depth
from pycompss.api.parameter import Weight
from pycompss.api.parameter import Keep_rename
from pycompss.api.parameter import _Param as Param  # noqa
from pycompss.util.objects.properties import is_basic_iterable
from pycompss.util.storages.persistent import has_id
from pycompss.util.storages.persistent import get_id

# Try to import numpy
try:
    import numpy as np
except ImportError:
    np = None

# Python3 has no ints and longs, only ints that are longs
PYCOMPSS_LONG = int if IS_PYTHON3 else long  # noqa

# Content type format is <module_path>:<class_name>, separated by colon (':')
UNDEFINED_CONTENT_TYPE = "#UNDEFINED#:#UNDEFINED#"


class Parameter(object):
    """
    Internal Parameter class
    Used to group all parameter variables.
    """

    def __init__(self,
                 name=None,
                 content=None,
                 content_type=None,
                 direction=DIRECTION.IN,
                 stream=IOSTREAM.UNSPECIFIED,
                 prefix=PREFIX.PREFIX,
                 file_name=None,
                 is_future=False,
                 is_file_collection=False,
                 depth=1,
                 extra_content_type=UNDEFINED_CONTENT_TYPE,
                 weight="1.0",
                 keep_rename=True):
        self.name = name
        self.content = content  # placeholder for parameter content
        self.content_type = content_type
        self.direction = direction
        self.stream = stream
        self.prefix = prefix
        self.file_name = file_name  # placeholder for object's serialized file
        self.is_future = is_future
        self.is_file_collection = is_file_collection
        self.depth = depth  # Recursive depth for collections
        self.extra_content_type = extra_content_type
        self.weight = weight
        self.keep_rename = keep_rename

    def __repr__(self):
        return 'Parameter(name=%s\n' \
               '          content=%s\n' \
               '          type=%s, direction=%s, stream=%s, prefix=%s\n' \
               '          file_name=%s\n' \
               '          is_future=%s\n' \
               '          is_file_collection=%s, depth=%s\n' \
               '          extra_content_type=%s\n' \
               '          weight=%s\n' \
               '          keep_rename=%s)' % (str(self.name),
                                              str(self.content),
                                              str(self.content_type),
                                              str(self.direction),
                                              str(self.stream),
                                              str(self.prefix),
                                              str(self.file_name),
                                              str(self.is_future),
                                              str(self.is_file_collection),
                                              str(self.depth),
                                              str(self.extra_content_type),
                                              str(self.weight),
                                              str(self.keep_rename))

    def is_object(self):
        """
        Determine if parameter is an object (not a FILE).

        :return: True if param represents an object (IN, INOUT, OUT)
        """
        return self.content_type is None

    def is_file(self):
        """
        Determine if parameter is a FILE.

        :return: True if param represents an FILE (IN, INOUT, OUT)
        """
        return self.content_type is TYPE.FILE

    def is_directory(self):
        """
        Determine if parameter is a DIRECTORY.

        :return: True if param represents an DIRECTORY
        """
        return self.content_type is TYPE.DIRECTORY


# Parameter conversion dictionary.
_param_conversion_dict_ = {
    'IN': {},
    'OUT': {
        'direction': DIRECTION.OUT,
    },
    'INOUT': {
        'direction': DIRECTION.INOUT,
    },
    'CONCURRENT': {
        'direction': DIRECTION.CONCURRENT,
    },
    'COMMUTATIVE': {
        'direction': DIRECTION.COMMUTATIVE,
    },
    'FILE': {
        'content_type': TYPE.FILE,
        'keep_rename': False
    },
    'FILE_IN': {
        'content_type': TYPE.FILE,
        'keep_rename': False
    },
    'FILE_OUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.OUT,
        'keep_rename': False
    },
    'FILE_INOUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.INOUT,
        'keep_rename': False
    },
    'DIRECTORY': {
        'content_type': TYPE.DIRECTORY,
        'keep_rename': False
    },
    'DIRECTORY_IN': {
        'content_type': TYPE.DIRECTORY,
        'keep_rename': False
    },
    'DIRECTORY_OUT': {
        'content_type': TYPE.DIRECTORY,
        'direction': DIRECTION.OUT,
        'keep_rename': False
    },
    'DIRECTORY_INOUT': {
        'content_type': TYPE.DIRECTORY,
        'direction': DIRECTION.INOUT,
        'keep_rename': False
    },
    'FILE_CONCURRENT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.CONCURRENT,
        'keep_rename': False
    },
    'FILE_COMMUTATIVE': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.COMMUTATIVE,
        'keep_rename': False
    },
    'FILE_STDIN': {
        'content_type': TYPE.FILE,
        'stream': IOSTREAM.STDIN,
        'keep_rename': False
    },
    'FILE_STDERR': {
        'content_type': TYPE.FILE,
        'stream': IOSTREAM.STDERR,
        'keep_rename': False
    },
    'FILE_STDOUT': {
        'content_type': TYPE.FILE,
        'stream': IOSTREAM.STDOUT,
        'keep_rename': False
    },
    'FILE_IN_STDIN': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.IN,
        'stream': IOSTREAM.STDIN,
        'keep_rename': False
    },
    'FILE_IN_STDERR': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.IN,
        'stream': IOSTREAM.STDERR,
        'keep_rename': False
    },
    'FILE_IN_STDOUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.IN,
        'stream': IOSTREAM.STDOUT,
        'keep_rename': False
    },
    'FILE_OUT_STDIN': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.OUT,
        'stream': IOSTREAM.STDIN,
        'keep_rename': False
    },
    'FILE_OUT_STDERR': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.OUT,
        'stream': IOSTREAM.STDERR,
        'keep_rename': False
    },
    'FILE_OUT_STDOUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.OUT,
        'stream': IOSTREAM.STDOUT
    },
    'FILE_INOUT_STDIN': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.INOUT,
        'stream': IOSTREAM.STDIN,
        'keep_rename': False
    },
    'FILE_INOUT_STDERR': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.INOUT,
        'stream': IOSTREAM.STDERR,
        'keep_rename': False
    },
    'FILE_INOUT_STDOUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.INOUT,
        'stream': IOSTREAM.STDOUT,
        'keep_rename': False
    },
    'FILE_CONCURRENT_STDIN': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.CONCURRENT,
        'stream': IOSTREAM.STDIN,
        'keep_rename': False
    },
    'FILE_CONCURRENT_STDERR': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.CONCURRENT,
        'stream': IOSTREAM.STDERR,
        'keep_rename': False
    },
    'FILE_CONCURRENT_STDOUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.CONCURRENT,
        'stream': IOSTREAM.STDOUT,
        'keep_rename': False
    },
    'FILE_COMMUTATIVE_STDIN': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.COMMUTATIVE,
        'stream': IOSTREAM.STDIN,
        'keep_rename': False
    },
    'FILE_COMMUTATIVE_STDERR': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.COMMUTATIVE,
        'stream': IOSTREAM.STDERR,
        'keep_rename': False
    },
    'FILE_COMMUTATIVE_STDOUT': {
        'content_type': TYPE.FILE,
        'direction': DIRECTION.COMMUTATIVE,
        'stream': IOSTREAM.STDOUT,
        'keep_rename': False
    },
    'COLLECTION': {
        'content_type': TYPE.COLLECTION,
    },
    'COLLECTION_IN': {
        'content_type': TYPE.COLLECTION,
        'direction': DIRECTION.IN,
    },
    'COLLECTION_INOUT': {
        'content_type': TYPE.COLLECTION,
        'direction': DIRECTION.INOUT,
    },
    'COLLECTION_OUT': {
        'content_type': TYPE.COLLECTION,
        'direction': DIRECTION.OUT,
    },
    'STREAM_IN': {
        'content_type': TYPE.EXTERNAL_STREAM,
        'direction': DIRECTION.IN
    },
    'STREAM_OUT': {
        'content_type': TYPE.EXTERNAL_STREAM,
        'direction': DIRECTION.OUT
    },
    'COLLECTION_FILE': {
        'content_type': TYPE.COLLECTION,
        'is_file_collection': True,
        'keep_rename': False
    },
    'COLLECTION_FILE_IN': {
        'content_type': TYPE.COLLECTION,
        'direction': DIRECTION.IN,
        'is_file_collection': True,
        'keep_rename': False
    },
    'COLLECTION_FILE_INOUT': {
        'content_type': TYPE.COLLECTION,
        'direction': DIRECTION.INOUT,
        'is_file_collection': True,
        'keep_rename': False
    },
    'COLLECTION_FILE_OUT': {
        'content_type': TYPE.COLLECTION,
        'direction': DIRECTION.OUT,
        'is_file_collection': True,
        'keep_rename': False
    }
}


def is_param(obj):
    """
    Check if given object is a parameter.
    Avoids internal _param_ import.

    :param obj: Object to check
    """
    return isinstance(obj, Param)


def is_parameter(obj):
    """
    Check if given object is a parameter.
    Avoids internal Parameter import.

    :param obj: Object to check
    """
    return isinstance(obj, Parameter)


def get_new_parameter(key):
    """
    Returns a brand new parameter (no copies!)

    :param key: A string that is a key of a valid Parameter template
    """
    return Parameter(**_param_conversion_dict_[key])


def get_parameter_copy(param):
    """
    Same as get_new_parameter but with param objects.

    :param param: Parameter object
    :return: An equivalent Parameter copy of this object (note that it will
             be equivalent, but not equal)
    """
    if is_param(param):
        return Parameter(**_param_conversion_dict_[param.key])
    assert is_parameter(param), \
        'Input parameter is neither a _Param nor a Parameter (is %s)' % \
        param.__class__.__name__
    return copy.deepcopy(param)


def is_dict_specifier(value):
    """
    Check if a parameter of the task decorator is a dictionary that specifies
    at least Type (and therefore can include things like Prefix, see binary
    decorator test for some examples).

    :param value: Decorator value to check
    :return: True if value is a dictionary that specifies at least the Type of
             the key
    """
    return isinstance(value, dict)


def get_parameter_from_dictionary(d):
    """
    Given a dictionary with fields like Type, Direction, etc.
    returns an actual Parameter object.

    :param d: Parameter description as dictionary
    :return: an actual Parameter object
    """
    if Type not in d:  # If no Type specified => IN
        d[Type] = Parameter()
    d[Type] = get_parameter_copy(d[Type])
    parameter = d[Type]
    if Direction in d:
        parameter.direction = d.get[Direction]
    if StdIOStream in d:
        parameter.stream = d[StdIOStream]
    if Prefix in d:
        parameter.prefix = d[Prefix]
    if Depth in d:
        parameter.depth = d[Depth]
    if Weight in d:
        parameter.weight = d[Weight]
    if Keep_rename in d:
        parameter.keep_rename = d[Keep_rename]
    return parameter


def get_compss_type(value, depth=0):
    """
    Retrieve the value type mapped to COMPSs types.

    :param value: Value to analyse
    :param depth: Collections depth.
    :return: The Type of the value
    """
    # If it is a numpy scalar, we manage it as all objects to avoid to
    # infer its type wrong. For instance isinstance(np.float64 object, float)
    # returns true
    if np and isinstance(value, np.generic):
        return TYPE.OBJECT

    if isinstance(value, bool):
        return TYPE.BOOLEAN
    elif isinstance(value, str):
        # Char does not exist as char, only strings.
        # Files will be detected as string, since it is a path.
        # The difference among them is defined by the parameter
        # decoration as FILE.
        return TYPE.STRING
    elif isinstance(value, int):
        if IS_PYTHON3:
            if value < PYTHON_MAX_INT:
                return TYPE.INT
            else:
                return TYPE.LONG
        else:
            return TYPE.INT
    elif isinstance(value, PYCOMPSS_LONG):
        return TYPE.LONG
    elif isinstance(value, float):
        return TYPE.DOUBLE
    elif has_id(value):
        # If has method getID maybe is a PSCO
        try:
            if get_id(value) not in [None, 'None']:
                # the 'getID' + id == criteria for persistent object
                return TYPE.EXTERNAL_PSCO
            else:
                return TYPE.OBJECT
        except TypeError:
            # A PSCO class has been used to check its type (when checking
            # the return). Since we still don't know if it is going to be
            # persistent inside, we assume that it is not. It will be checked
            # later on the worker side when the task finishes.
            return TYPE.OBJECT
    elif depth > 0 and is_basic_iterable(value):
        return TYPE.COLLECTION
    else:
        # Default type
        return TYPE.OBJECT


# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT = 9223372036854775807
JAVA_MIN_LONG = PYTHON_MIN_INT = -9223372036854775808
