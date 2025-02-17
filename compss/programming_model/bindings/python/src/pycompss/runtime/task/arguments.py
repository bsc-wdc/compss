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
PyCOMPSs runtime - Task - Arguments.

This file contains the classes needed for the argument identification.
"""


def is_vararg(param_name: str) -> bool:
    """Determine if a parameter is named as a (internal) vararg.

    :param param_name: String with a parameter name.
    :returns: True if the name has the form of an internal vararg name.
    """
    return param_name.startswith("*")


def get_name_from_vararg(full_name: str) -> str:
    """Extract the vararg name from the name given with full_name.

    Part before "*".

    :param full_name: Complete vararg name.
    :return: The vararg name.
    """
    return full_name.split("*")[1]


def is_kwarg(param_name: str) -> bool:
    """Determine if a parameter is named as a (internal) kwargs.

    :param param_name: String with a parameter name.
    :return: True if the name has the form of an internal kwarg name.
    """
    return param_name.startswith("#kwarg")


def is_return(param_name: str) -> bool:
    """Determine if a parameter is named as a (internal) return.

    :param param_name: String with a parameter name.
    :returns: True if the name has the form of an internal return name.
    """
    return param_name.startswith("$return")


def get_vararg_name(varargs_name: str, i: int) -> str:
    """Given some integer i, return the name of the ith vararg.

    Note that the given internal names to these parameters are
    impossible to be assigned by the user because they are invalid
    Python variable names, as they start with a star

    :param varargs_name: Vararg names.
    :param i: A non negative integer.
    :return: The name of the ith vararg according to our internal naming
             convention.
    """
    return f"*{varargs_name}*_{i}"


def get_kwarg_name(var: str) -> str:
    """Given some variable name, get the kwarg identifier.

    :param var: A string with a variable name.
    :return: The name of the kwarg according to our internal naming convention.
    """
    return f"#kwarg_{var}"


def get_name_from_kwarg(var: str) -> str:
    """Given some kwarg name, return the original variable name.

    :param var: A string with a (internal) kwarg name.
    :return: The original variable name.
    """
    return var.replace("#kwarg_", "")


def get_return_name(i: int) -> str:
    """Given some integer i, return the name of the ith return.

    :param i: A non-negative integer.
    :return: The name of the return identifier according to our internal
             naming.
    """
    return f"$return_{i}"
