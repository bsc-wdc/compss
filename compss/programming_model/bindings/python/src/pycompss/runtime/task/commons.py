#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
This module includes all API commons
"""

from pycompss.util.typing_helper import typing

import pycompss.api.parameter as parameter
from pycompss.runtime.task.parameter import get_new_parameter
from pycompss.runtime.task.parameter import Parameter


class TaskCommons(object):
    """
    Code shared by the TaskMaster and TaskWorker.
    Both classes inherit from TaskCommons.
    """

    __slots__ = ["user_function", "decorator_arguments",
                 "param_args", "param_varargs",
                 "on_failure", "defaults"]

    def __init__(self,                 # type: typing.Any
                 decorator_arguments,  # type: typing.Dict[str, typing.Any]
                 user_function,        # type: typing.Any
                 on_failure,           # type: str
                 defaults              # type: dict
                 ):                    # type: (...) -> None
        self.user_function = user_function
        self.decorator_arguments = decorator_arguments
        self.param_args = []       # type: typing.List[typing.Any]
        self.param_varargs = None  # type: typing.Any
        self.on_failure = on_failure
        self.defaults = defaults


def get_varargs_direction(param_varargs, decorator_arguments):
    # type: (typing.Any, typing.Any) -> typing.Tuple[typing.Any, Parameter]
    """ Returns the direction of the varargs arguments.

    Can be defined in the decorator in two ways:
        args = dir, where args is the name of the variadic args tuple.
        varargs_type = dir (for legacy reasons).

    :param param_varargs: Parameter varargs.
    :param decorator_arguments: Decorator arguments.
    :return: Direction of the varargs arguments.
    """
    if param_varargs not in decorator_arguments:
        if "varargsType" in decorator_arguments:
            param_varargs = "varargsType"
            return param_varargs, decorator_arguments[param_varargs]
        return param_varargs, decorator_arguments["varargs_type"]
    return param_varargs, decorator_arguments[param_varargs]


def get_default_direction(var_name, decorator_arguments, param_args):
    # type: (str, typing.Dict[str, typing.Any], typing.List[typing.Any]) -> Parameter
    """ Returns the default direction for a given parameter.

    :param var_name: Variable name.
    :param decorator_arguments: Decorator arguments.
    :param param_args: Parameter args.
    :return: An identifier of the direction.
    """
    # We are the "self" or "cls" in an instance or class method that
    # modifies the given class, so we are an INOUT, CONCURRENT or
    # COMMUTATIVE
    self_dirs = [parameter.DIRECTION.INOUT,
                 parameter.DIRECTION.CONCURRENT,
                 parameter.DIRECTION.COMMUTATIVE]
    if "targetDirection" in decorator_arguments:
        target_label = "targetDirection"
    else:
        target_label = "target_direction"
    if decorator_arguments[target_label].direction in self_dirs and \
            var_name in ["self", "cls"] and \
            param_args and \
            param_args[0] == var_name:
        return decorator_arguments[target_label]
    return get_new_parameter("IN")
