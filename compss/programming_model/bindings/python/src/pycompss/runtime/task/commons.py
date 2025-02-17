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
PyCOMPSs runtime - Task - Arguments commons.

This file contains the task  arguments commons.
"""

from pycompss.api import parameter
from pycompss.util.typing_helper import typing
from pycompss.runtime.task.definitions.arguments import TaskArguments


def get_default_direction(
    var_name: str,
    decorator_arguments: TaskArguments,
    param_args: typing.List[typing.Any],
) -> str:
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
        parameter.INOUT.key,
        parameter.CONCURRENT.key,
        parameter.COMMUTATIVE.key,
    ]
    if (
        decorator_arguments.target_direction in self_dirs
        and var_name in ["self", "cls"]
        and param_args
        and param_args[0] == var_name
    ):
        return decorator_arguments.target_direction
    return parameter.IN.key
