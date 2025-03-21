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
PyCOMPSs API - commons - error messages.

This file defines the public PyCOMPSs error messages displayed by the API.
"""


def not_in_pycompss(decorator_name: str) -> str:
    """Retrieve the "not in PyCOMPSs scope" error message.

    :param decorator_name: Decorator name which requires the message.
    :return: Not in PyCOMPSs error message.
    """
    return (
        f"The {decorator_name} decorator only works within PyCOMPSs framework."
    )


def cast_env_to_int_error(what: str) -> str:
    """Retrieve the "can not cast from env. variable to integer" error message.

    :param what: Environment variable name.
    :return: Can not cast from environment variable to integer.
    """
    return f"ERROR: {what} value cannot be cast from ENV variable to int"


def cast_string_to_int_error(what: str) -> str:
    """Retrieve the "can not cast from string to integer" error message.

    :param what: Environment variable name.
    :return: Can not cast from string to integer.
    """
    return f"ERROR: {what} value cannot be cast from string to int"


def wrong_value(value_name: str, decorator_name: str) -> str:
    """Retrieve the "wrong value at decorator" error message.

    :param value_name: Wrong value's name
    :param decorator_name: Decorator name which requires the message.
    :return: Wrong value at decorator message.
    """
    return f"ERROR: Wrong {value_name} value at {decorator_name} decorator."
