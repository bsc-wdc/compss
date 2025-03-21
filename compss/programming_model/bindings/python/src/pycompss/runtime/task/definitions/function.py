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
PyCOMPSs runtime - Task - Definitions - Function.

This file contains the task function definition.
"""
import types

from pycompss.util.typing_helper import typing  # noqa: F401
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

    def __init__(self) -> None:
        """Set the default function definition values."""
        self.function = dummy_function  # type: typing.Callable
        # Global variables common for all tasks of this kind
        self.registered = False
        self.signature = ""
        # Saved from the initial task
        self.interactive = False
        self.module = types.ModuleType("None")  # type: types.ModuleType
        self.function_name = ""
        self.module_name = ""
        self.function_type = -1
        self.class_name = ""
        self.code_strings = False
