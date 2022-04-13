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
PyCOMPSs runtime - Task - Arguments commons.

This file contains the task  arguments commons.
"""

import pycompss.api.parameter as parameter
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_new_parameter
from pycompss.util.typing_helper import typing


def get_varargs_direction(
    param_varargs: typing.Any, decorator_arguments: typing.Any
) -> typing.Tuple[typing.Any, Parameter]:
    """Return the direction of the varargs arguments.

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


def get_default_direction(
    var_name: str,
    decorator_arguments: typing.Dict[str, typing.Any],
    param_args: typing.List[typing.Any],
) -> Parameter:
    """Return the default direction for a given parameter.

    :param var_name: Variable name.
    :param decorator_arguments: Decorator arguments.
    :param param_args: Parameter args.
    :return: An identifier of the direction.
    """
    # We are the "self" or "cls" in an instance or class method that
    # modifies the given class, so we are an INOUT, CONCURRENT or
    # COMMUTATIVE
    self_dirs = [
        parameter.DIRECTION.INOUT,
        parameter.DIRECTION.CONCURRENT,
        parameter.DIRECTION.COMMUTATIVE,
    ]
    if "targetDirection" in decorator_arguments:
        target_label = "targetDirection"
    else:
        target_label = "target_direction"
    if (
        decorator_arguments[target_label].direction in self_dirs
        and var_name in ["self", "cls"]
        and param_args
        and param_args[0] == var_name
    ):
        return decorator_arguments[target_label]
    return get_new_parameter("IN")
