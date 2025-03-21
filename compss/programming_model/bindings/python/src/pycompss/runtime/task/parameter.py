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
PyCOMPSs runtime - Task - Parameter.

This file contains the classes needed for the task parameter definition.
"""

import sys

from pycompss.api.parameter import Cache
from pycompss.api.parameter import DIRECTION
from pycompss.api.parameter import Depth
from pycompss.api.parameter import Direction
from pycompss.api.parameter import IOSTREAM
from pycompss.api.parameter import Keep_rename
from pycompss.api.parameter import PREFIX
from pycompss.api.parameter import Prefix
from pycompss.api.parameter import StdIOStream
from pycompss.api.parameter import TYPE
from pycompss.api.parameter import Type
from pycompss.api.parameter import Weight
from pycompss.api.parameter import _Param as Param  # noqa
from pycompss.runtime.task.keys import PARAM_ALIAS_KEYS
from pycompss.runtime.task.keys import PARAM_DICT_KEYS
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.objects.properties import is_basic_iterable
from pycompss.util.objects.properties import is_dict
from pycompss.util.storages.persistent import get_id
from pycompss.util.storages.persistent import has_id
from pycompss.util.typing_helper import typing

# Try to import numpy
NP = None  # type: typing.Union[None, typing.Any]
try:
    import numpy

    NP = numpy
except ImportError:
    pass

# Python max and min integer values
PYTHON_MAX_INT = sys.maxsize
PYTHON_MIN_INT = -sys.maxsize - 1
# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT
JAVA_MIN_LONG = PYTHON_MIN_INT

# Content type format is <module_path>:<class_name>, separated by colon (":")
UNDEFINED_CONTENT_TYPE = "#UNDEFINED#:#UNDEFINED#"


class COMPSsFile:
    """Class that represents a file in the worker."""

    __slots__ = [
        "source_path",
        "destination_name",
        "keep_source",
        "is_write_final",
        "original_path",
    ]

    def __init__(self, file_name: typing.Any = "None") -> None:
        """Compss File constructor.

        :param file_name: File name.
        """
        self.source_path = "None"  # type: str
        self.destination_name = "None"  # type: str
        self.keep_source = False  # type: bool
        self.is_write_final = False  # type: bool
        self.original_path = file_name  # type: typing.Any
        if (
            file_name is not None
            and isinstance(file_name, str)
            and ":" in file_name
        ):
            fields = file_name.split(":")
            self.source_path = fields[0]
            self.destination_name = fields[1]
            self.keep_source = fields[2] == "true"
            self.is_write_final = fields[3] == "true"
            self.original_path = fields[4]
        else:
            # Can be a collection wrapper or a stream
            pass

    def __repr__(self) -> str:
        """Compss File object representation.

        :returns: The COMPSsFile representation as string.
        """
        return (
            f"Source: {self.source_path}, "
            f"Destination: {self.destination_name}, "
            f"Keep source: {self.keep_source}, "
            f"Is write final: {self.is_write_final}, "
            f"Original path: {self.original_path}"
        )


class Parameter:
    """Class that represents a file in the worker.

    Internal Parameter class
    Used to group all parameter variables.
    """

    __slots__ = [
        "name",
        "content",
        "content_type",
        "direction",
        "stream",
        "prefix",
        "file_name",
        "is_future",
        "is_file_collection",
        "collection_content",
        "dict_collection_content",
        "depth",
        "extra_content_type",
        "weight",
        "keep_rename",
        "cache",
    ]

    def __init__(
        self,
        name: str = "None",
        content: typing.Any = "",
        content_type: int = -1,
        direction: int = DIRECTION.IN,
        stream: int = IOSTREAM.UNSPECIFIED,
        prefix: str = PREFIX.PREFIX,
        file_name: COMPSsFile = COMPSsFile(),
        is_future: bool = False,
        is_file_collection: bool = False,
        collection_content: typing.Any = "",
        dict_collection_content: typing.Optional[dict] = None,
        depth: int = PYTHON_MAX_INT,
        extra_content_type: str = UNDEFINED_CONTENT_TYPE,
        weight: str = "1.0",
        keep_rename: bool = True,
        cache: bool = True,
    ) -> None:
        """Parameter constructor.

        :param name: Parameter name.
        :param content: Object represented by the parameter.
        :param content_type: Object type.
        :param direction: Parameter direction.
        :param stream: Parameter stream type.
        :param prefix: Parameter prefix.
        :param file_name: Parameter associated file name.
        :param is_future: If is future or not.
        :param is_file_collection: If is a collection of files or not.
        :param collection_content: Collection contents (list of Parameters).
        :param dict_collection_content: Dictionary collection contents
                                        (dictionary of parameters).
        :param depth: Collection or dictionary collection depth.
        :param extra_content_type: Extra content types.
        :param weight: Parameter weight.
        :param keep_rename: Keep rename.
        :param cache: If cache the parameter.
        """
        if dict_collection_content is None:
            dict_collection_content = {}
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
        self.cache = cache

    def __repr__(self) -> str:
        """Parameter object representation.

        :returns: The parameter representation as string.
        """
        return (
            f"Parameter(name={str(self.name)}\n"
            f"          type={str(self.content_type)}, "
            f"direction={str(self.direction)}, "
            f"stream={str(self.stream)}, "
            f"prefix={str(self.prefix)}\n"
            f"          extra_content_type={str(self.extra_content_type)}\n"
            f"          file_name={str(self.file_name)}\n"
            f"          is_future={str(self.is_future)}\n"
            f"          is_file_collection={str(self.is_file_collection)}, "
            f"depth={str(self.depth)}\n"
            f"          weight={str(self.weight)}\n"
            f"          keep_rename={str(self.keep_rename)}\n"
            f"          cache={str(self.cache)}\n"
            f"          content={str(self.content)})"
        )

    def is_object(self) -> bool:
        """Determine if parameter is an object (not a FILE).

        :return: True if param represents an object (IN, INOUT, OUT).
        """
        return self.content_type == -1

    def is_file(self) -> bool:
        """Determine if parameter is a FILE.

        :return: True if param represents an FILE (IN, INOUT, OUT).
        """
        return self.content_type is TYPE.FILE

    def is_directory(self) -> bool:
        """Determine if parameter is a DIRECTORY.

        :return: True if param represents an DIRECTORY.
        """
        return self.content_type is TYPE.DIRECTORY

    def is_collection(self) -> bool:
        """Determine if parameter is a COLLECTION.

        :return: True if param represents an COLLECTION.
        """
        return self.content_type is TYPE.COLLECTION

    def is_dict_collection(self) -> bool:
        """Determine if parameter is a DICT_COLLECTION.

        :return: True if param represents an DICT_COLLECTION.
        """
        return self.content_type is TYPE.DICT_COLLECTION


# Parameter conversion dictionary.
_param_conversion_dict_ = {
    PARAM_ALIAS_KEYS.IN: {},
    PARAM_ALIAS_KEYS.OUT: {
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
    },
    PARAM_ALIAS_KEYS.INOUT: {
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
    },
    PARAM_ALIAS_KEYS.CONCURRENT: {
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.CONCURRENT,
    },
    PARAM_ALIAS_KEYS.COMMUTATIVE: {
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.COMMUTATIVE,
    },
    PARAM_ALIAS_KEYS.IN_DELETE: {
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN_DELETE,
    },
    PARAM_ALIAS_KEYS.FILE: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_IN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_OUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_INOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.DIRECTORY: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DIRECTORY,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
    },
    PARAM_ALIAS_KEYS.DIRECTORY_IN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DIRECTORY,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.DIRECTORY_OUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DIRECTORY,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.DIRECTORY_INOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DIRECTORY,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_CONCURRENT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.CONCURRENT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_COMMUTATIVE: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.COMMUTATIVE,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_STDIN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDIN,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_STDERR: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDERR,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_STDOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_IN_STDIN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDIN,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_IN_STDERR: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDERR,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_IN_STDOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_OUT_STDIN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDIN,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_OUT_STDERR: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDERR,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_OUT_STDOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_INOUT_STDIN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDIN,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_INOUT_STDERR: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDERR,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_INOUT_STDOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_CONCURRENT_STDIN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.CONCURRENT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDIN,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_CONCURRENT_STDERR: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.CONCURRENT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDERR,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_CONCURRENT_STDOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.CONCURRENT,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_COMMUTATIVE_STDIN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.COMMUTATIVE,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDIN,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_COMMUTATIVE_STDERR: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.COMMUTATIVE,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDERR,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.FILE_COMMUTATIVE_STDOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.FILE,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.COMMUTATIVE,
        PARAM_DICT_KEYS.STDIOSTREAM: IOSTREAM.STDOUT,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.COLLECTION: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
    },
    PARAM_ALIAS_KEYS.COLLECTION_IN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
    },
    PARAM_ALIAS_KEYS.COLLECTION_INOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
    },
    PARAM_ALIAS_KEYS.COLLECTION_OUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
    },
    PARAM_ALIAS_KEYS.DICT_COLLECTION: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DICT_COLLECTION,
    },
    PARAM_ALIAS_KEYS.DICT_COLLECTION_IN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
    },
    PARAM_ALIAS_KEYS.DICT_COLLECTION_INOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
    },
    PARAM_ALIAS_KEYS.DICT_COLLECTION_OUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
    },
    PARAM_ALIAS_KEYS.COLLECTION_IN_DELETE: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN_DELETE,
    },
    PARAM_ALIAS_KEYS.DICT_COLLECTION_IN_DELETE: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.DICT_COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN_DELETE,
    },
    PARAM_ALIAS_KEYS.STREAM_IN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.EXTERNAL_STREAM,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.STREAM_OUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.EXTERNAL_STREAM,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.COLLECTION_FILE: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.IS_FILE_COLLECTION: True,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.COLLECTION_FILE_IN: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.IN,
        PARAM_DICT_KEYS.IS_FILE_COLLECTION: True,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.COLLECTION_FILE_INOUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.INOUT,
        PARAM_DICT_KEYS.IS_FILE_COLLECTION: True,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
    PARAM_ALIAS_KEYS.COLLECTION_FILE_OUT: {
        PARAM_DICT_KEYS.CONTENT_TYPE: TYPE.COLLECTION,
        PARAM_DICT_KEYS.DIRECTION: DIRECTION.OUT,
        PARAM_DICT_KEYS.IS_FILE_COLLECTION: True,
        PARAM_DICT_KEYS.KEEP_RENAME: False,
        PARAM_DICT_KEYS.CACHE: False,
    },
}  # type: dict


def is_param(obj: typing.Any) -> bool:
    """Check if given object is a parameter.

    Avoids internal _param_ import.

    :param obj: Object to check.
    :return: True if obj is instance of _Param.
    """
    return isinstance(obj, Param)


def is_parameter(obj: typing.Any) -> bool:
    """Check if given object is a parameter.

    Avoids internal Parameter import.

    :param obj: Object to check.
    :return: True if obj is instance of Parameter.
    """
    return isinstance(obj, Parameter)


def get_new_parameter(key: str) -> Parameter:
    """Return a new parameter (no copies!).

    :param key: A string that is a key of a valid Parameter template.
    :return: A new Parameter from the given key.
    """
    return Parameter(**_param_conversion_dict_[key])


def is_dict_specifier(value: typing.Any) -> bool:
    """Check if value is a supported dictionary.

    Check if a parameter of the task decorator is a dictionary that specifies
    at least Type (and therefore can include things like Prefix, see binary
    decorator test for some examples).

    :param value: Decorator value to check.
    :return: True if value is a dictionary that specifies at least the Type of
             the key.
    """
    return isinstance(value, dict) and Type in value


def get_parameter_from_dictionary(dictionary: dict) -> Parameter:
    """Convert a dictionary to Parameter.

    Given a dictionary with fields like Type, Direction, etc.
    returns an actual Parameter object.

    :param dictionary: Parameter description as dictionary.
    :return: an actual Parameter object.
    """
    if not isinstance(dictionary, dict):
        raise PyCOMPSsException("Unexpected type for parameter.")
    if Type not in dictionary:  # If no Type specified => IN
        parameter = Parameter()
    else:
        parameter = get_new_parameter(dictionary[Type].key)
    # Add other modifiers
    if Direction in dictionary:
        parameter.direction = dictionary[Direction]
    if StdIOStream in dictionary:
        parameter.stream = dictionary[StdIOStream]
    if Prefix in dictionary:
        parameter.prefix = dictionary[Prefix]
    if Depth in dictionary:
        parameter.depth = dictionary[Depth]
    if Weight in dictionary:
        parameter.weight = dictionary[Weight]
    if Keep_rename in dictionary:
        parameter.keep_rename = dictionary[Keep_rename]
    if Cache in dictionary:
        parameter.cache = dictionary[Cache]
    return parameter


def get_direction_from_key(key: str) -> int:
    """Return the direction (as integer) for the given key.

    :param key: A direction key string (e.g. keys defined in Parameter).
    :return: The associated integer value for the given key.
    """
    if key == PARAM_ALIAS_KEYS.IN:
        return DIRECTION.IN
    elif key == PARAM_ALIAS_KEYS.OUT:
        return DIRECTION.OUT
    elif key == PARAM_ALIAS_KEYS.INOUT:
        return DIRECTION.INOUT
    elif key == PARAM_ALIAS_KEYS.CONCURRENT:
        return DIRECTION.CONCURRENT
    elif key == PARAM_ALIAS_KEYS.COMMUTATIVE:
        return DIRECTION.COMMUTATIVE
    elif key == PARAM_ALIAS_KEYS.IN_DELETE:
        return DIRECTION.IN_DELETE
    else:
        raise PyCOMPSsException(f"Wrong direction key: {key}")


def get_compss_type(
    value: typing.Any, depth: int = 0, code_strings=True, force_file=False
) -> int:
    """Retrieve the value type mapped to COMPSs types.

    :param value: Value to analyse.
    :param depth: Collections depth.
    :param code_strings: If strings will be encoded.
    :param force_file: If the default value is file (collections of files).
    :return: The Type of the value.
    """
    # First check if it is a PSCO since a StorageNumpy can be detected
    # as a numpy object.
    if has_id(value):
        # If has method getID maybe is a PSCO
        try:
            if get_id(value) not in [None, "None"]:
                # the "getID" + id == criteria for persistent object
                return TYPE.EXTERNAL_PSCO
            return TYPE.OBJECT
        except TypeError:
            # A PSCO class has been used to check its type (when checking
            # the return). Since we still don't know if it is going to be
            # persistent inside, we assume that it is not. It will be checked
            # later on the worker side when the task finishes.
            return TYPE.OBJECT

    if force_file:
        if depth > 0 and is_basic_iterable(value):
            return TYPE.COLLECTION
        elif depth > 0 and is_dict(value):
            return TYPE.DICT_COLLECTION
        else:
            return TYPE.FILE

    # If it is a numpy scalar, we manage it as all objects to avoid to
    # infer its type wrong. For instance isinstance(NP.float64 object, float)
    # returns true
    if NP and isinstance(value, NP.generic):
        return TYPE.OBJECT

    if isinstance(value, (bool, str, int, float)):
        value_type = type(value)
        if value_type is bool:
            return TYPE.BOOLEAN
        if value_type is str:
            # Char does not exist as char, only strings.
            # Files will be detected as string, since it is a path.
            # The difference among them is defined by the parameter
            # decoration as FILE.
            return TYPE.STRING if not code_strings else TYPE.STRING_64
        if value_type is int:
            if int(value) < PYTHON_MAX_INT:  # noqa
                return TYPE.INT
            return TYPE.LONG
        if value_type is float:
            return TYPE.DOUBLE
    elif depth > 0 and is_basic_iterable(value):
        return TYPE.COLLECTION
    elif depth > 0 and is_dict(value):
        return TYPE.DICT_COLLECTION
    else:
        # Default type
        return TYPE.OBJECT
    return -1
