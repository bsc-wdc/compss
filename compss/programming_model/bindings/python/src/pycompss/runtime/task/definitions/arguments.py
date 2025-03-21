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
PyCOMPSs runtime - Task - Definitions - Arguments.

This file contains the task decorator arguments definition.
"""

from pycompss.api import parameter
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.util.typing_helper import typing
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_new_parameter
from pycompss.runtime.task.parameter import get_parameter_from_dictionary
from pycompss.runtime.task.parameter import is_param
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging

    LOGGER = logging.getLogger(__name__)


class TaskArguments:
    """Task decorator arguments class definition.

    This class represents the supported task decorator arguments.
    """

    __slots__ = [
        # Supported keywords
        "target_direction",
        "returns",
        "cache_returns",
        "priority",
        "defaults",
        "time_out",
        "is_replicated",
        "is_distributed",
        "computing_nodes",
        "processes_per_node",
        "is_reduce",
        "chunk_size",
        "on_failure",
        "tracing_hook",
        "numba",
        "numba_flags",
        "numba_signature",
        "numba_declaration",
        # varargs_type kept for legacy purposes:
        "varargs_type",
        # Function parameters:
        "parameters",
    ]

    def __init__(self) -> None:
        """Set the default task decorator arguments values."""
        # Supported arguments in the @task decorator
        self.target_direction = parameter.INOUT.key
        self.returns = False  # type: typing.Any
        self.cache_returns = True
        self.priority = False
        self.defaults = {}  # type: typing.Dict[str, typing.Any]
        self.time_out = 0
        self.is_replicated = False
        self.is_distributed = False
        self.computing_nodes = 1
        self.processes_per_node = 1
        self.is_reduce = False
        self.chunk_size = 0
        self.on_failure = "RETRY"
        self.tracing_hook = False
        # numba mode (jit, vectorize, guvectorize)
        self.numba = (
            False
        )  # type: typing.Union[bool, str, typing.Dict[str, bool]]
        # user defined extra numba flags
        self.numba_flags = (
            {}
        )  # type: typing.Dict[str, typing.Union[str, bool]]
        # vectorize and guvectorize signature
        self.numba_signature = (
            None
        )  # type: typing.Optional[typing.Union[str, typing.List[str]]]
        # guvectorize declaration
        self.numba_declaration = None  # type: typing.Optional[str]
        # varargs_type kept for legacy purposes:
        self.varargs_type = parameter.IN.key
        # Function parameters:
        self.parameters = (
            {}
        )  # type: typing.Dict[str, typing.Union[Parameter, typing.Any]]

    @staticmethod
    def __deprecation_warning__(old_keyword: str, new_keyword: str) -> None:
        """Log the deprecation warning message for the given keyword.

        :param old_keyword: Deprecated keyword.
        :param new_keyword: Keyword that should be used.
        :return: None
        """
        LOGGER.warning(
            "Detected deprecated %s. Please, change it to %s",
            old_keyword,
            new_keyword,
        )

    def update_arguments(self, kwargs: typing.Dict[str, typing.Any]) -> None:
        """Update the task arguments with the given kwargs.

        :param kwargs: Task keyword arguments.
        :returns: None
        """
        # Argument: target_direction
        if LABELS.target_direction in kwargs:
            target_direction = kwargs.pop(LABELS.target_direction)
            if is_param(target_direction):
                self.target_direction = target_direction.key
            else:
                raise PyCOMPSsException(
                    f"Unexpected {LABELS.target_direction} type. "
                    f"Must be a direction."
                )
        elif LEGACY_LABELS.target_direction in kwargs:
            target_direction = kwargs.pop(LEGACY_LABELS.target_direction)
            if is_param(target_direction):
                self.target_direction = target_direction.key
                self.__deprecation_warning__(
                    LEGACY_LABELS.target_direction, LABELS.target_direction
                )
            else:
                raise PyCOMPSsException(
                    f"Unexpected {LEGACY_LABELS.target_direction} type. "
                    f"Must be a direction."
                )
        # Argument: returns
        if LABELS.returns in kwargs:
            self.returns = kwargs.pop(LABELS.returns)
        # Argument: cache_returns
        if LABELS.cache_returns in kwargs:
            self.cache_returns = kwargs.pop(LABELS.cache_returns)
        # Argument: priority
        if LABELS.priority in kwargs:
            self.priority = kwargs.pop(LABELS.priority)
        # Argument: defaults
        if LABELS.defaults in kwargs:
            self.defaults = kwargs.pop(LABELS.defaults)
        # Argument: time_out
        if LABELS.time_out in kwargs:
            self.time_out = kwargs.pop(LABELS.time_out)
        elif LEGACY_LABELS.time_out in kwargs:
            self.time_out = kwargs.pop(LEGACY_LABELS.time_out)
            self.__deprecation_warning__(
                LEGACY_LABELS.time_out, LABELS.time_out
            )
        # Argument: is_replicated
        if LABELS.is_replicated in kwargs:
            self.is_replicated = kwargs.pop(LABELS.is_replicated)
        elif LEGACY_LABELS.is_replicated in kwargs:
            self.is_replicated = kwargs.pop(LEGACY_LABELS.is_replicated)
            self.__deprecation_warning__(
                LEGACY_LABELS.is_replicated, LABELS.is_replicated
            )
        # Argument: is_distributed
        if LABELS.is_distributed in kwargs:
            self.is_distributed = kwargs.pop(LABELS.is_distributed)
        elif LEGACY_LABELS.is_distributed in kwargs:
            self.is_distributed = kwargs.pop(LEGACY_LABELS.is_distributed)
            self.__deprecation_warning__(
                LEGACY_LABELS.is_distributed, LABELS.is_distributed
            )
        # Argument: computing_nodes
        if LABELS.computing_nodes in kwargs:
            self.computing_nodes = kwargs.pop(LABELS.computing_nodes)
        elif LEGACY_LABELS.computing_nodes in kwargs:
            self.computing_nodes = kwargs.pop(LEGACY_LABELS.computing_nodes)
            self.__deprecation_warning__(
                LEGACY_LABELS.computing_nodes, LABELS.computing_nodes
            )
        # Argument: processes_per_node
        if LABELS.processes_per_node in kwargs:
            self.processes_per_node = kwargs.pop(LABELS.processes_per_node)
        # Argument: is_reduce
        if LABELS.is_reduce in kwargs:
            self.is_reduce = kwargs.pop(LABELS.is_reduce)
        # Argument: chunk_size
        if LABELS.chunk_size in kwargs:
            self.chunk_size = kwargs.pop(LABELS.chunk_size)
        # Argument: on_failure
        if LABELS.on_failure in kwargs:
            self.on_failure = kwargs.pop(LABELS.on_failure)
        # Argument: tracing_hook
        if LABELS.tracing_hook in kwargs:
            self.tracing_hook = kwargs.pop(LABELS.tracing_hook)
        # Argument: numba
        if LABELS.numba in kwargs:
            self.numba = kwargs.pop(LABELS.numba)
        # Argument: numba_flags
        if LABELS.numba_flags in kwargs:
            self.numba_flags = kwargs.pop(LABELS.numba_flags)
        # Argument: numba_signature
        if LABELS.numba_signature in kwargs:
            self.numba_signature = kwargs.pop(LABELS.numba_signature)
        # Argument: numba_declaration
        if LABELS.numba_declaration in kwargs:
            self.numba_declaration = kwargs.pop(LABELS.numba_declaration)
        # Argument: varargs_type
        if LABELS.varargs_type in kwargs:
            varargs_type = kwargs.pop(LABELS.varargs_type)
            if is_param(varargs_type):
                self.varargs_type = varargs_type.key
            else:
                raise PyCOMPSsException(
                    "Unexpected varargs_type type. Must be a direction."
                )
        elif LEGACY_LABELS.varargs_type in kwargs:
            varargs_type = kwargs.pop(LEGACY_LABELS.varargs_type)
            if is_param(varargs_type):
                self.varargs_type = varargs_type.key
                self.__deprecation_warning__(
                    LEGACY_LABELS.varargs_type, LABELS.varargs_type
                )
            else:
                raise PyCOMPSsException(
                    "Unexpected varargsType type. Must be a direction."
                )
        # Argument: the rest (named function parameters).
        # The rest of the arguments are expected to be only the function
        # parameter related information
        for key, value in kwargs.items():
            if isinstance(value, dict):
                # It is a dictionary for the given parameter
                # (e.g. param={Direction: IN})
                self.parameters[key] = get_parameter_from_dictionary(value)
            else:
                # It is a keyword for the given parameter (e.g. param=IN)
                try:
                    self.parameters[key] = get_new_parameter(value.key)
                except AttributeError:  # no .key in value.
                    # Non defined parameters: usually mistaken parameters.
                    self.parameters[key] = value

    def __repr__(self) -> str:
        """Representation of the task arguments definition.

        :return: String representing the task arguments.
        """
        task_arguments = "Task arguments:\n"
        task_arguments += f"- Target direction: {self.target_direction}\n"
        task_arguments += f"- Returns: {self.returns}\n"
        task_arguments += f"- Cache returns: {self.cache_returns}\n"
        task_arguments += f"- Priority: {self.priority}\n"
        task_arguments += f"- Defaults: {self.defaults}\n"
        task_arguments += f"- Time out: {self.time_out}\n"
        task_arguments += f"- Is replicated: {self.is_replicated}\n"
        task_arguments += f"- Is distributed: {self.is_distributed}\n"
        task_arguments += f"- Computing nodes: {self.computing_nodes}\n"
        task_arguments += f"- Processes per node: {self.processes_per_node}\n"
        task_arguments += f"- Is reduce: {self.is_reduce}\n"
        task_arguments += f"- Chunk size: {self.chunk_size}\n"
        task_arguments += f"- On failure: {self.on_failure}\n"
        task_arguments += f"- Tracing hook: {self.tracing_hook}\n"
        task_arguments += f"- Numba: {self.numba}\n"
        task_arguments += f"- Numba flags: {self.numba_flags}\n"
        task_arguments += f"- Numba signature: {self.numba_signature}\n"
        task_arguments += f"- Numba declaration: {self.numba_declaration}\n"
        task_arguments += f"- Varargs type: {self.varargs_type}\n"
        task_arguments += f"- Parameters: {self.parameters}"
        return task_arguments

    def get_keys(self) -> typing.List[str]:  # TODO: avoid this function.
        """Return defined arguments.

        :return: List of strings
        """
        arguments = [
            "target_direction",
            "returns",
            "cache_returns",
            "priority",
            "defaults",
            "time_out",
            "is_replicated",
            "is_distributed",
            "computing_nodes",
            "processes_per_node",
            "is_reduce",
            "chunk_size",
            "on_failure",
            "tracing_hook",
            "numba",
            "numba_flags",
            "numba_signature",
            "numba_declaration",
            "varargs_type",
        ]
        return arguments

    def get_parameter(self, name: str, default_direction: str) -> Parameter:
        """Retrieve the Parameter object associated to name.

        If not found, retrieves a new Parameter object with the default
        direction.

        :param name: Parameter name.
        :param default_direction: Default direction if not found.
        :return: The Parameter object
        """
        if name in self.parameters:
            return self.parameters[name]
        # Not found:
        default_parameter = get_new_parameter(default_direction)
        return default_parameter

    def get_parameter_or_none(self, name: str) -> typing.Optional[Parameter]:
        """Retrieve the Parameter object associated to name.

        If not found, retrieves None.

        :param name: Parameter name.
        :return: The Parameter object or None
        """
        if name in self.parameters:
            return self.parameters[name]
        return None
