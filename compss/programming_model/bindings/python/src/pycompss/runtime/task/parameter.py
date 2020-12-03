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
from pycompss.runtime.task.keys import ParamAliasKeys
from pycompss.runtime.task.keys import ParamDictKeys
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
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.objects.properties import is_basic_iterable
from pycompss.util.objects.properties import is_dict
from pycompss.util.storages.persistent import has_id
from pycompss.util.storages.persistent import get_id

# Try to import numpy
try:
    import numpy as np
except ImportError:
    np = None

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

# Python3 has no ints and longs, only ints that are longs
PYCOMPSS_LONG = int if IS_PYTHON3 else long  # noqa

# Content type format is <module_path>:<class_name>, separated by colon (':')
UNDEFINED_CONTENT_TYPE = "#UNDEFINED#:#UNDEFINED#"


class Parameter(object):
    """
    Internal Parameter class
    Used to group all parameter variables.
    """

    __slots__ = ['name', 'content', 'content_type', 'direction', 'stream',
                 'prefix', 'file_name', 'is_future', 'is_file_collection',
                 'collection_content', 'dict_collection_content',
                 'depth', 'extra_content_type', 'weight', 'keep_rename']

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
                 collection_content=None,
                 dict_collection_content=None,
                 depth=1,
                 extra_content_type=UNDEFINED_CONTENT_TYPE,
                 weight="1.0",
                 keep_rename=True):  # NOSONAR
        self.name = name
        self.content = content  # placeholder for parameter content
        self.content_type = content_type
        self.direction = direction
        self.stream = stream
        self.prefix = prefix
        self.file_name = file_name  # placeholder for object's serialized file
        self.is_future = is_future
        self.is_file_collection = is_file_collection
        self.collection_content = collection_content
        self.dict_collection_content = dict_collection_content
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


# Parameter conversion dictionary.
_param_conversion_dict_ = {
    ParamAliasKeys.IN: {},
    ParamAliasKeys.OUT: {
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
    },
    ParamAliasKeys.INOUT: {
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
    },
    ParamAliasKeys.CONCURRENT: {
        ParamDictKeys.DIRECTION: DIRECTION.CONCURRENT,
    },
    ParamAliasKeys.COMMUTATIVE: {
        ParamDictKeys.DIRECTION: DIRECTION.COMMUTATIVE,
    },
    ParamAliasKeys.IN_DELETE: {
        ParamDictKeys.DIRECTION: DIRECTION.IN_DELETE,
    },
    ParamAliasKeys.FILE: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_IN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_OUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_INOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.DIRECTORY: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DIRECTORY,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.DIRECTORY_IN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DIRECTORY,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.DIRECTORY_OUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DIRECTORY,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.DIRECTORY_INOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DIRECTORY,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_CONCURRENT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.CONCURRENT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_COMMUTATIVE: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.COMMUTATIVE,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_STDIN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDIN,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_STDERR: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDERR,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_STDOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_IN_STDIN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.IN,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDIN,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_IN_STDERR: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.IN,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDERR,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_IN_STDOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.IN,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_OUT_STDIN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDIN,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_OUT_STDERR: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDERR,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_OUT_STDOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_INOUT_STDIN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDIN,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_INOUT_STDERR: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDERR,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_INOUT_STDOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_CONCURRENT_STDIN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.CONCURRENT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDIN,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_CONCURRENT_STDERR: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.CONCURRENT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDERR,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_CONCURRENT_STDOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.CONCURRENT,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_COMMUTATIVE_STDIN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.COMMUTATIVE,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDIN,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_COMMUTATIVE_STDERR: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.COMMUTATIVE,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDERR,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.FILE_COMMUTATIVE_STDOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.FILE,
        ParamDictKeys.DIRECTION: DIRECTION.COMMUTATIVE,
        ParamDictKeys.STDIOSTREAM: IOSTREAM.STDOUT,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.COLLECTION: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
    },
    ParamAliasKeys.COLLECTION_IN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.IN,
    },
    ParamAliasKeys.COLLECTION_INOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
    },
    ParamAliasKeys.COLLECTION_OUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
    },
    ParamAliasKeys.DICT_COLLECTION: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DICT_COLLECTION,
    },
    ParamAliasKeys.DICT_COLLECTION_IN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.IN,
    },
    ParamAliasKeys.DICT_COLLECTION_INOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
    },
    ParamAliasKeys.DICT_COLLECTION_OUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
    },
    ParamAliasKeys.COLLECTION_IN_DELETE: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.IN_DELETE,
    },
    ParamAliasKeys.STREAM_IN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.EXTERNAL_STREAM,
        ParamDictKeys.DIRECTION: DIRECTION.IN
    },
    ParamAliasKeys.STREAM_OUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.EXTERNAL_STREAM,
        ParamDictKeys.DIRECTION: DIRECTION.OUT
    },
    ParamAliasKeys.COLLECTION_FILE: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.IS_FILE_COLLECTION: True,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.COLLECTION_FILE_IN: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.IN,
        ParamDictKeys.IS_FILE_COLLECTION: True,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.COLLECTION_FILE_INOUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.INOUT,
        ParamDictKeys.IS_FILE_COLLECTION: True,
        ParamDictKeys.KEEP_RENAME: False
    },
    ParamAliasKeys.COLLECTION_FILE_OUT: {
        ParamDictKeys.CONTENT_TYPE: TYPE.COLLECTION,
        ParamDictKeys.DIRECTION: DIRECTION.OUT,
        ParamDictKeys.IS_FILE_COLLECTION: True,
        ParamDictKeys.KEEP_RENAME: False
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
    if not isinstance(d, dict):
        raise PyCOMPSsException("Unexpected type for parameter.")
    else:
        if Type not in d:  # If no Type specified => IN
            parameter = Parameter()
        else:
            parameter = get_new_parameter(d[Type].key)
        # Add other modifiers
        if Direction in d:
            parameter.direction = d[Direction]
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

    if isinstance(value, (bool, str, int, PYCOMPSS_LONG, float)):
        value_type = type(value)
        if value_type is bool:
            return TYPE.BOOLEAN
        elif value_type is str:
            # Char does not exist as char, only strings.
            # Files will be detected as string, since it is a path.
            # The difference among them is defined by the parameter
            # decoration as FILE.
            return TYPE.STRING
        elif value_type is int:
            if IS_PYTHON3:
                if value < PYTHON_MAX_INT:  # noqa
                    return TYPE.INT
                else:
                    return TYPE.LONG
            else:
                return TYPE.INT
        elif value_type is PYCOMPSS_LONG:
            return TYPE.LONG
        elif value_type is float:
            return TYPE.DOUBLE
    elif depth > 0 and is_basic_iterable(value):
        return TYPE.COLLECTION
    elif depth > 0 and is_dict(value):
        return TYPE.DICT_COLLECTION
    else:
        # Default type
        return TYPE.OBJECT
