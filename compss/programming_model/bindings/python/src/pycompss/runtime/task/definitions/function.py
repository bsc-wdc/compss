#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs runtime - Task - Definitions - Function.

This file contains the task function definition.
"""

from pycompss.util.typing_helper import typing
from pycompss.util.typing_helper import dummy_function


class FunctionDefinition:
    """Task decorated function definition class.

    This class represents the decorated function definition attributes.
    It contains the function related attributes.
    """

    __slots__ = [
        "function",
        "registered",
        "signature",
        "interactive",
        "module",
        "function_name",
        "module_name",
        "function_type",
        "class_name",
        "code_strings",
    ]

    def __init__(self):
        """Set the default function definition values."""
        self.function = dummy_function  # type: typing.Callable
        # Global variables common for all tasks of this kind
        self.registered = False
        self.signature = ""
        # Saved from the initial task
        self.interactive = False
        self.module = None  # type: typing.Any
        self.function_name = ""
        self.module_name = ""
        self.function_type = -1
        self.class_name = ""
        self.code_strings = False

    def get_function(self) -> typing.Callable:
        """Get function.

        The function can be invoked or inspected.
        :returns: The function instance.
        """
        return self.function

    def set_function(self, function: typing.Callable) -> None:
        """Set function.

        :param function: Function to be set.
        :return: None
        """
        self.function = function

    def get_registered(self) -> bool:
        """Get registered.

        :returns: True if the function has been registered. False otherwise.
        """
        return self.registered

    def set_registered(self, registered: bool) -> None:
        """Set registered.

        :param registered: Registered value to be set.
        :return: None
        """
        self.registered = registered

    def get_signature(self) -> str:
        """Get signature.

        :returns: The function signature (e.g. module.function_name).
        """
        return self.signature

    def set_signature(self, signature: str) -> None:
        """Set function's signature.

        :param signature: Signature to be set.
        :return: None
        """
        self.signature = signature

    def get_interactive(self) -> bool:
        """Get interactive.

        :returns: True if the function is interactive. False otherwise.
        """
        return self.interactive

    def set_interactive(self, interactive: bool) -> None:
        """Set interactive.

        :param interactive: Interactive value to be set.
        :return: None
        """
        self.interactive = interactive

    def get_module(self) -> typing.Any:
        """Get the module that contains the function.

        :returns: The module that contains the function.
        """
        return self.module

    def set_module(self, module: typing.Any) -> None:
        """Set the module that contains the function.

        :param module: Module to be set.
        :return: None
        """
        self.module = module

    def get_function_name(self) -> str:
        """Get function name.

        :returns: The function name.
        """
        return self.function_name

    def set_function_name(self, function_name: str) -> None:
        """Set function name.

        :param function_name: Function name to be set.
        :return: None
        """
        self.function_name = function_name

    def get_module_name(self) -> str:
        """Get the module where the function is defined name.

        :returns: The function's module name.
        """
        return self.module_name

    def set_module_name(self, module_name: str) -> None:
        """Set the function's module name.

        :param module_name: Module name to be set.
        :return: None
        """
        self.module_name = module_name

    def get_function_type(self) -> int:
        """Get the function type.

        :returns: The function type.
        """
        return self.function_type

    def set_function_type(self, function_type: int) -> None:
        """Set the function type.

        :param function_type: Function type to be set.
        :return: None
        """
        self.function_type = function_type

    def get_class_name(self) -> str:
        """Get the name of the class where the function is defined.

        :returns: The function's class name.
        """
        return self.class_name

    def set_class_name(self, class_name: str) -> None:
        """Set the function's class name.

        :param class_name: Class name to be set.
        :return: None
        """
        self.class_name = class_name

    def get_code_strings(self) -> bool:
        """Get code strings.

        :returns: True if code string. False otherwise
        """
        return self.code_strings

    def set_code_strings(self, code_strings: bool) -> None:
        """Set code strings.

        :param code_strings: Code strings value to be set.
        :return: None
        """
        self.code_strings = code_strings
