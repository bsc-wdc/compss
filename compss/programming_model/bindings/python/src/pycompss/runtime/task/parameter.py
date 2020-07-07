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

import sys
import copy

from pycompss.runtime.commons import IS_PYTHON3
from pycompss.runtime.task.keys import Keys
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
        # type: () -> str
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
        # type: () -> bool
        """ Determine if parameter is an object (not a FILE).

        :return: True if param represents an object (IN, INOUT, OUT).
        """
        return self.content_type is None

    def is_file(self):
        # type: () -> bool
        """ Determine if parameter is a FILE.

        :return: True if param represents an FILE (IN, INOUT, OUT).
        """
        return self.content_type is TYPE.FILE

    def is_directory(self):
        # type: () -> bool
        """ Determine if parameter is a DIRECTORY.

        :return: True if param represents an DIRECTORY.
        """
        return self.content_type is TYPE.DIRECTORY


_DIRECTION = 'direction'
_KEEP_RENAME = 'keep_rename'
_CONTENT_TYPE = 'content_type'
_STREAM = 'stream'
_IS_FILE_COLLECTION = 'is_file_collection'
# Parameter conversion dictionary.
_param_conversion_dict_ = {
    Keys.IN: {},
    Keys.OUT: {
        _DIRECTION: DIRECTION.OUT,
    },
    Keys.INOUT: {
        _DIRECTION: DIRECTION.INOUT,
    },
    Keys.CONCURRENT: {
        _DIRECTION: DIRECTION.CONCURRENT,
    },
    Keys.COMMUTATIVE: {
        _DIRECTION: DIRECTION.COMMUTATIVE,
    },
    Keys.FILE: {
        _CONTENT_TYPE: TYPE.FILE,
        _KEEP_RENAME: False
    },
    Keys.FILE_IN: {
        _CONTENT_TYPE: TYPE.FILE,
        _KEEP_RENAME: False
    },
    Keys.FILE_OUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.OUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_INOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.INOUT,
        _KEEP_RENAME: False
    },
    Keys.DIRECTORY: {
        _CONTENT_TYPE: TYPE.DIRECTORY,
        _KEEP_RENAME: False
    },
    Keys.DIRECTORY_IN: {
        _CONTENT_TYPE: TYPE.DIRECTORY,
        _KEEP_RENAME: False
    },
    Keys.DIRECTORY_OUT: {
        _CONTENT_TYPE: TYPE.DIRECTORY,
        _DIRECTION: DIRECTION.OUT,
        _KEEP_RENAME: False
    },
    Keys.DIRECTORY_INOUT: {
        _CONTENT_TYPE: TYPE.DIRECTORY,
        _DIRECTION: DIRECTION.INOUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_CONCURRENT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.CONCURRENT,
        _KEEP_RENAME: False
    },
    Keys.FILE_COMMUTATIVE: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.COMMUTATIVE,
        _KEEP_RENAME: False
    },
    Keys.FILE_STDIN: {
        _CONTENT_TYPE: TYPE.FILE,
        _STREAM: IOSTREAM.STDIN,
        _KEEP_RENAME: False
    },
    Keys.FILE_STDERR: {
        _CONTENT_TYPE: TYPE.FILE,
        _STREAM: IOSTREAM.STDERR,
        _KEEP_RENAME: False
    },
    Keys.FILE_STDOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _STREAM: IOSTREAM.STDOUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_IN_STDIN: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.IN,
        _STREAM: IOSTREAM.STDIN,
        _KEEP_RENAME: False
    },
    Keys.FILE_IN_STDERR: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.IN,
        _STREAM: IOSTREAM.STDERR,
        _KEEP_RENAME: False
    },
    Keys.FILE_IN_STDOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.IN,
        _STREAM: IOSTREAM.STDOUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_OUT_STDIN: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.OUT,
        _STREAM: IOSTREAM.STDIN,
        _KEEP_RENAME: False
    },
    Keys.FILE_OUT_STDERR: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.OUT,
        _STREAM: IOSTREAM.STDERR,
        _KEEP_RENAME: False
    },
    Keys.FILE_OUT_STDOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.OUT,
        _STREAM: IOSTREAM.STDOUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_INOUT_STDIN: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.INOUT,
        _STREAM: IOSTREAM.STDIN,
        _KEEP_RENAME: False
    },
    Keys.FILE_INOUT_STDERR: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.INOUT,
        _STREAM: IOSTREAM.STDERR,
        _KEEP_RENAME: False
    },
    Keys.FILE_INOUT_STDOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.INOUT,
        _STREAM: IOSTREAM.STDOUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_CONCURRENT_STDIN: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.CONCURRENT,
        _STREAM: IOSTREAM.STDIN,
        _KEEP_RENAME: False
    },
    Keys.FILE_CONCURRENT_STDERR: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.CONCURRENT,
        _STREAM: IOSTREAM.STDERR,
        _KEEP_RENAME: False
    },
    Keys.FILE_CONCURRENT_STDOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.CONCURRENT,
        _STREAM: IOSTREAM.STDOUT,
        _KEEP_RENAME: False
    },
    Keys.FILE_COMMUTATIVE_STDIN: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.COMMUTATIVE,
        _STREAM: IOSTREAM.STDIN,
        _KEEP_RENAME: False
    },
    Keys.FILE_COMMUTATIVE_STDERR: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.COMMUTATIVE,
        _STREAM: IOSTREAM.STDERR,
        _KEEP_RENAME: False
    },
    Keys.FILE_COMMUTATIVE_STDOUT: {
        _CONTENT_TYPE: TYPE.FILE,
        _DIRECTION: DIRECTION.COMMUTATIVE,
        _STREAM: IOSTREAM.STDOUT,
        _KEEP_RENAME: False
    },
    Keys.COLLECTION: {
        _CONTENT_TYPE: TYPE.COLLECTION,
    },
    Keys.COLLECTION_IN: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _DIRECTION: DIRECTION.IN,
    },
    Keys.COLLECTION_INOUT: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _DIRECTION: DIRECTION.INOUT,
    },
    Keys.COLLECTION_OUT: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _DIRECTION: DIRECTION.OUT,
    },
    Keys.STREAM_IN: {
        _CONTENT_TYPE: TYPE.EXTERNAL_STREAM,
        _DIRECTION: DIRECTION.IN
    },
    Keys.STREAM_OUT: {
        _CONTENT_TYPE: TYPE.EXTERNAL_STREAM,
        _DIRECTION: DIRECTION.OUT
    },
    Keys.COLLECTION_FILE: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _IS_FILE_COLLECTION: True,
        _KEEP_RENAME: False
    },
    Keys.COLLECTION_FILE_IN: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _DIRECTION: DIRECTION.IN,
        _IS_FILE_COLLECTION: True,
        _KEEP_RENAME: False
    },
    Keys.COLLECTION_FILE_INOUT: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _DIRECTION: DIRECTION.INOUT,
        _IS_FILE_COLLECTION: True,
        _KEEP_RENAME: False
    },
    Keys.COLLECTION_FILE_OUT: {
        _CONTENT_TYPE: TYPE.COLLECTION,
        _DIRECTION: DIRECTION.OUT,
        _IS_FILE_COLLECTION: True,
        _KEEP_RENAME: False
    }
}


def is_param(obj):
    # type: (object) -> bool
    """ Check if given object is a parameter.
    Avoids internal _param_ import.

    :param obj: Object to check.
    :return: True if obj is instance of _Param.
    """
    return isinstance(obj, Param)


def is_parameter(obj):
    # type: (object) -> bool
    """ Check if given object is a parameter.
    Avoids internal Parameter import.

    :param obj: Object to check.
    :return: True if obj is instance of Parameter.
    """
    return isinstance(obj, Parameter)


def get_new_parameter(key):
    # type: (str) -> Parameter
    """ Returns a brand new parameter (no copies!)

    :param key: A string that is a key of a valid Parameter template.
    :return: A new Parameter from the given key.
    """
    return Parameter(**_param_conversion_dict_[key])


def get_parameter_copy(parameter):
    # type: (Parameter) -> Parameter
    """ Copies the given parameter into a new one.

    :param parameter: Parameter object.
    :return: An equivalent Parameter copy of this object (note that it will
             be equivalent, but not equal).
    """
    assert is_parameter(parameter), \
        'Input parameter is not Parameter (is %s)' % parameter.__class__.__name__  # noqa: E501
    return copy.deepcopy(parameter)


def is_dict_specifier(value):
    # type: (object) -> bool
    """ Check if value is a supported dictionary.
    Check if a parameter of the task decorator is a dictionary that specifies
    at least Type (and therefore can include things like Prefix, see binary
    decorator test for some examples).

    :param value: Decorator value to check.
    :return: True if value is a dictionary that specifies at least the Type of
             the key.
    """
    return isinstance(value, dict) and Type in value


def get_parameter_from_dictionary(d):
    # type: (dict) -> Parameter
    """ Convert a dictionary to Parameter
    Given a dictionary with fields like Type, Direction, etc.
    returns an actual Parameter object.

    :param d: Parameter description as dictionary.
    :return: an actual Parameter object.
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
    # type: (object, int) -> int
    """ Retrieve the value type mapped to COMPSs types.

    :param value: Value to analyse.
    :param depth: Collections depth.
    :return: The Type of the value.
    """
    # First check if it is a PSCO since a StorageNumpy can be detected
    # as a numpy object.
    if has_id(value):
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
    elif depth > 0 and is_basic_iterable(value):
        return TYPE.COLLECTION
    else:
        # Default type
        return TYPE.OBJECT


# Python max and min integer values
if IS_PYTHON3:
    PYTHON_MAX_INT = sys.maxsize
    PYTHON_MIN_INT = -sys.maxsize - 1
else:
    PYTHON_MAX_INT = sys.maxint       # noqa
    PYTHON_MIN_INT = -sys.maxint - 1  # noqa
# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT
JAVA_MIN_LONG = PYTHON_MIN_INT
