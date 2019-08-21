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

# Python3 has no ints and longs, only ints that are longs
from pycompss.runtime.commons import IS_PYTHON3

# Numbers match both C and Java enums
from pycompss.api.data_type import data_type
from pycompss.util.objects.properties import is_basic_iterable
from pycompss.util.storages.persistent import has_id
from pycompss.util.storages.persistent import get_id

PYCOMPSS_LONG = int if IS_PYTHON3 else long

TYPE = data_type


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
                 depth=1):
        self.type = p_type
        self.direction = p_direction
        self.stream = p_stream
        self.prefix = p_prefix
        self.object = p_object      # placeholder for parameter object
        self.file_name = file_name  # placeholder for object's serialized file
        self.is_future = is_future
        self.depth = depth          # Recursive depth for collections

    def __repr__(self):
        return 'Parameter(type=%s, direction=%s, stream=%s, prefix=%s\n' \
               '          object=%s\n' \
               '          file_name=%s\n' \
               '          is_future=%s)' % (str(self.type),
                                            str(self.direction),
                                            str(self.stream),
                                            str(self.prefix),
                                            str(self.object),
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
                 prefix=None):
        self.name = name
        self.type = p_type
        self.file_name = file_name
        self.key = key
        self.content = content
        self.stream = stream
        self.prefix = prefix

    def __repr__(self):
        return '\nParameter %s' % self.name + '\n' + \
               '\tType %s' % str(self.type) + '\n' + \
               '\tFile Name %s' % self.file_name + '\n' + \
               '\tKey %s' % str(self.key) + '\n' + \
               '\tContent %s' % str(self.content) + '\n' + \
               '\tStream %s' % str(self.stream) + '\n' + \
               '\tPrefix %s' % str(self.prefix) + '\n' + \
               '-' * 20 + '\n'


# Parameter conversion dictionary.
_param_conversion_dict_ = {
    'IN': {},
    'OUT': {
        'p_direction': DIRECTION.OUT
    },
    'INOUT': {
        'p_direction': DIRECTION.INOUT
    },
    'CONCURRENT': {
        'p_direction': DIRECTION.CONCURRENT
    },
    'COMMUTATIVE': {
        'p_direction': DIRECTION.COMMUTATIVE
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
    'FILE_CONCURRENT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.CONCURRENT
    },
    'FILE_COMMUTATIVE': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.COMMUTATIVE
    },
    'FILE_STDIN': {
        'p_type': TYPE.FILE,
        'p_stream': IOSTREAM.STDIN
    },
    'FILE_STDERR': {
        'p_type': TYPE.FILE,
        'p_stream': IOSTREAM.STDERR
    },
    'FILE_STDOUT': {
        'p_type': TYPE.FILE,
        'p_stream': IOSTREAM.STDOUT
    },
    'FILE_IN_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.IN,
        'p_stream': IOSTREAM.STDIN
    },
    'FILE_IN_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.IN,
        'p_stream': IOSTREAM.STDERR
    },
    'FILE_IN_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.IN,
        'p_stream': IOSTREAM.STDOUT
    },
    'FILE_OUT_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT,
        'p_stream': IOSTREAM.STDIN
    },
    'FILE_OUT_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT,
        'p_stream': IOSTREAM.STDERR
    },
    'FILE_OUT_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.OUT,
        'p_stream': IOSTREAM.STDOUT
    },
    'FILE_INOUT_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT,
        'p_stream': IOSTREAM.STDIN
    },
    'FILE_INOUT_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT,
        'p_stream': IOSTREAM.STDERR
    },
    'FILE_INOUT_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.INOUT,
        'p_stream': IOSTREAM.STDOUT
    },
    'FILE_CONCURRENT_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.CONCURRENT,
        'p_stream': IOSTREAM.STDIN
    },
    'FILE_CONCURRENT_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.CONCURRENT,
        'p_stream': IOSTREAM.STDERR
    },
    'FILE_CONCURRENT_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.CONCURRENT,
        'p_stream': IOSTREAM.STDOUT
    },
    'FILE_COMMUTATIVE_STDIN': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.COMMUTATIVE,
        'p_stream': IOSTREAM.STDIN
    },
    'FILE_COMMUTATIVE_STDERR': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.COMMUTATIVE,
        'p_stream': IOSTREAM.STDERR
    },
    'FILE_COMMUTATIVE_STDOUT': {
        'p_type': TYPE.FILE,
        'p_direction': DIRECTION.COMMUTATIVE,
        'p_stream': IOSTREAM.STDOUT
    },
    'COLLECTION': {
        'p_type': TYPE.COLLECTION
    },
    'COLLECTION_IN': {
        'p_type': TYPE.COLLECTION,
        'p_direction': DIRECTION.IN
    },
    'COLLECTION_INOUT': {
        'p_type': TYPE.COLLECTION,
        'p_direction': DIRECTION.INOUT
    },
    'STREAM_IN': {
        'p_type': TYPE.EXTERNAL_STREAM,
        'p_direction': DIRECTION.IN
    },
    'STREAM_OUT': {
        'p_type': TYPE.EXTERNAL_STREAM,
        'p_direction': DIRECTION.OUT
    }
}


def is_parameter(x):
    """
    Check if given object is a parameter.
    Avoids internal _param_ import.

    :param x: Object to check
    """
    return isinstance(x, _Param)


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
    if is_parameter(param):
        return Parameter(**_param_conversion_dict_[param.key])
    assert isinstance(param, Parameter), \
        'Input parameter is neither a _param_ nor a Parameter (is %s)' % \
        param.__class__.__name__
    import copy
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
    p = d[Type]
    if Direction in d:
        p.direction = d.get[Direction]
    if StdIOStream in d:
        p.stream = d[StdIOStream]
    if Prefix in d:
        p.prefix = d[Prefix]
    if Depth in d:
        p.depth = d[Depth]
    return p


def is_vararg(param_name):
    """
    Determine if a parameter is named as a (internal) vararg.

    :param param_name: String with a parameter name
    :returns: True iff the name has the form of an internal vararg name
    """
    return param_name.startswith('*')


def get_varargs_name(full_name):
    """
    Extract the vararg name from the name given with full_name
    (part before "*").

    :param full_name: Complete name.
    :return: The vararg name
    """
    return full_name.split('*')[0]


def is_kwarg(param_name):
    """
    Determine if a parameter is named as a (internal) kwargs.

    :param param_name: String with a parameter name
    :return: True if the name has the form of an internal kwarg name
    """
    return param_name.startswith('#kwarg')


def is_return(param_name):
    """
    Determine if a parameter is named as a (internal) return.

    :param param_name: String with a parameter name
    :returns: True iff the name has the form of an internal return name
    """
    return param_name.startswith('$return')


def is_object(param):
    """
    Determine if a parameter is an object (not a FILE).

    :param param: Parameter to determine
    :return: True iff x represents an object (IN, INOUT, OUT)
    """
    return param.type is None


# Note that the given internal names to these parameters are
# impossible to be assigned by the user because they are invalid
# Python variable names, as they start with a star
def get_vararg_name(varargs_name, i):
    """
    Given some integer i, return the name of the ith vararg.

    :param varargs_name: Vararg names
    :param i: A nonnegative integer
    :return: The name of the ith vararg according to our internal naming
             convention
    """
    return '*%s*_%d' % (varargs_name, i)


def get_kwarg_name(var):
    """
    Given some variable name, get the kwarg identifier.

    :param var: A string with a variable name
    :return: The name of the kwarg according to our internal naming convention
    """
    return '#kwarg_%s' % var


def get_name_from_kwarg(var):
    """
    Given some kwarg name, return the original variable name.

    :param var: A string with a (internal) kwarg name
    :return: The original variable name
    """
    return var.replace('#kwarg_', '')


def get_return_name(i):
    """
    Given some integer i, return the name of the ith return.

    :param i: A nonnegative integer
    :return: The name of the return identifier according to our internal naming
    """
    return '$return_%d' % i


def get_original_name(x):
    """
    Given a name with some internal prefix, remove them and return
    the original name.

    :param x: Parameter name
    :return: The original name of the parameter (i.e the one given by the user)
    """
    return get_name_from_kwarg(x)


def build_command_line_parameter(name, value):
    """
    Some parameters are passed as command line arguments. In order to be able
    to recognize them they are passed following the expression below.
    Note that strings are always encoded to base64, so it is guaranteed that
    we will always have exactly two underscores on the parameter.

    :param name: Name of the parameter
    :param value: Value of the parameter
    :return: *PARAM_name_value. Example, variable y equals 3 => *PARAM_y_3
    """
    return '*INLINE_%s_%s' % (name, str(value))


def retrieve_command_line_parameter(cla):
    """
    Given a command line parameter, retrieve its name and its value.

    :param cla: Command line argument
    :return: name and value of the command line argument
    """
    _, name, value = cla.split('_')
    return name, value


def is_command_line_parameter(cla):
    """
    Check if a string is a command line parameter.

    :param cla: Command line argument
    :return: Boolean
    """
    import re
    return bool(re.match('\*PARAM_.+_.+', cla))


def stringify(object_name, object_type, object_content):
    """
    Given a stringifiable object, stringify it.

    :param object_name: Name of the object
    :param object_type: Type of the object
    :param object_content: The object itself
    :return: The stringified object
    """
    return '%s#%s#%s' % (object_name, object_type, str(object_content))


def destringify(stringified_object):
    """
    Given a stringified object, destringify it.

    :param stringified_object: Stringified object
    :return: actual object from the given stringified object
    """
    return stringified_object.split('#', 2)


def get_compss_type(value, depth=0):
    """
    Retrieve the value type mapped to COMPSs types.

    :param value: Value to analyse
    :param depth: Collections depth.
    :return: The Type of the value
    """
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
        # TODO: L4 less error-prone methods
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
