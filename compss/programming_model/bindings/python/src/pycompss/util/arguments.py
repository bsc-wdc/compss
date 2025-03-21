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
PyCOMPSs Util - Arguments.

This file contains the common methods to do any argument (used in a decorator)
check or management.
"""

from __future__ import print_function

import re
import sys

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing


def check_arguments(
    mandatory_arguments: typing.Set[str],
    deprecated_arguments: typing.Set[str],
    supported_arguments: typing.Set[str],
    argument_names: typing.List[str],
    decorator: str,
) -> None:
    """Perform all needed checks to the decorator definition.

    The checks are:
        1.- Checks that the mandatory arguments are present (otherwise, raises
            an exception).
        2.- Looks for unexpected arguments (displays a warning through stderr).

    :param mandatory_arguments: Set of mandatory argument names.
    :param deprecated_arguments: Set of deprecated argument names.
    :param supported_arguments: Set of supported argument names.
    :param argument_names: List of argument names to check.
    :param decorator: String - Decorator name.
    :return: None.
    """
    decorator_str = f"{decorator} decorator"
    # Look for mandatory arguments
    check_mandatory_arguments(
        mandatory_arguments, argument_names, decorator_str
    )
    # Look for deprecated arguments
    __check_deprecated_arguments(
        deprecated_arguments, argument_names, decorator_str
    )
    # Look for unexpected arguments
    __check_unexpected_arguments(
        supported_arguments, argument_names, decorator_str
    )


def check_mandatory_arguments(
    mandatory_arguments: typing.Set[str],
    argument_names: typing.List[str],
    where: str,
) -> None:
    """Check that all mandatory arguments are in arguments.

    :param mandatory_arguments: Set of supported arguments.
    :param argument_names: List of arguments to check.
    :param where: Location of the argument.
    :return: None.
    """
    for argument in mandatory_arguments:
        if "_" in argument:
            if (
                argument not in argument_names
                and __to_camel_case(argument) not in argument_names
            ):
                # The mandatory argument or it converted to camel case is
                # not in the arguments
                __error_mandatory_argument(where, argument)
        else:
            if argument not in argument_names:
                # The mandatory argument is not in the arguments
                __error_mandatory_argument(where, argument)


def __to_camel_case(argument: str) -> str:
    """Convert the given argument to camel case.

    :param argument: String to convert to camel case.
    :return: Camel case string.
    """
    components = argument.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def __error_mandatory_argument(decorator: str, argument: str) -> None:
    """Raise an exception when the argument is mandatory in the decorator.

    :param argument: Argument name.
    :param decorator: Decorator name.
    :return: None.
    :raise PyCOMPSsException: With the decorator and argument that produced
                              the error.
    """
    raise PyCOMPSsException(
        f"The argument {str(argument)} is mandatory "
        f"in the {str(decorator)} decorator."
    )


def __check_deprecated_arguments(
    deprecated_arguments: typing.Set[str],
    argument_names: typing.List[str],
    where: str,
) -> None:
    """Look for deprecated arguments and displays a warning if found.

    :param deprecated_arguments: Set of deprecated arguments.
    :param argument_names: List of arguments to check.
    :param where: Location of the argument.
    :return: None.
    :raise PyCOMPSsException: With the unsupported argument.
    """
    for argument in argument_names:
        if argument == "isModifier":
            message = (
                f"ERROR: Unsupported argument: isModifier Found "
                f"in {str(where)}.\n"
                "       Please, use: target_direction"
            )
            print(message, file=sys.stderr)  # also show the warn in stderr
            raise PyCOMPSsException(f"Unsupported argument: {str(argument)}")

        if argument in deprecated_arguments:
            current_argument = re.sub("([A-Z]+)", r"_\1", argument).lower()
            message = (
                f"WARNING: Deprecated argument: {str(argument)} Found "
                f"in {str(where)}.\n"
                f"         Please, use: {current_argument}"
            )

            # The print through stdout is disabled to prevent the message to
            # appear twice in the console. So the warning message will only
            # appear in STDERR.
            # print(message)                 # show the warn through stdout
            print(message, file=sys.stderr)  # also show the warn in stderr


def __check_unexpected_arguments(
    supported_arguments: typing.Set[str],
    argument_names: typing.List[str],
    where: str,
) -> None:
    """Look for unexpected arguments and displays a warning if found.

    :param supported_arguments: Set of supported arguments.
    :param argument_names: List of arguments to check.
    :param where: Location of the argument.
    :return: None.
    """
    for argument in argument_names:
        if argument not in supported_arguments:
            message = (
                f"WARNING: Unexpected argument: {str(argument)} Found "
                f"in {str(where)}."
            )
            # The print through stdout is disabled to prevent the message to
            # appear twice in the console. So the warning message will only
            # appear in STDERR.
            # print(message)                 # show the warn through stdout
            print(message, file=sys.stderr)  # also show the warn in stderr
