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
PyCOMPSs Utils - arguments
==========================
    This file contains the common methods to do any argument check
    or management.
"""

from __future__ import print_function
import sys
import re


def check_arguments(mandatory_arguments, deprecated_arguments,
                    supported_arguments, argument_names, decorator):
    """
    Performs all needed checks to the decorator definition:
        1.- Checks that the mandatory arguments are present (otherwise, raises
            an exception).
        2.- Looks for unexpected arguments (displays a warning through stderr).

    :param mandatory_arguments: Set of mandatory argument names
    :param deprecated_arguments: Set of deprecated argument names
    :param supported_arguments: Seto of supported argument names
    :param argument_names: List of argument names to check
    :param decorator: String - Decorator name
    :return: None
    """
    # Look for mandatory arguments
    check_mandatory_arguments(mandatory_arguments,
                              argument_names,
                              decorator + " decorator")
    # Look for deprecated arguments
    check_deprecated_arguments(deprecated_arguments,
                               argument_names,
                               decorator + " decorator")
    # Look for unexpected arguments
    check_unexpected_arguments(supported_arguments,
                               argument_names,
                               decorator + " decorator")


def check_mandatory_arguments(mandatory_arguments, arguments, where):
    """
    This method checks that all mandatory arguments are in arguments.

    :param mandatory_arguments: Set of supported arguments
    :param arguments: List of arguments to check
    :param where: Location of the argument
    :return: None
    """
    for argument in mandatory_arguments:
        if '_' in argument:
            if argument not in arguments and \
                    _to_camel_case(argument) not in arguments:
                # The mandatory argument or it converted to camel case is
                # not in the arguments
                error_mandatory_argument(where, argument)
        else:
            if argument not in arguments:
                # The mandatory argument is not in the arguments
                error_mandatory_argument(where, argument)


def _to_camel_case(argument):
    components = argument.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def error_mandatory_argument(argument, decorator):
    """
    Raises an exception when the argument is mandatory in the decorator

    :param argument: Argument name
    :param decorator: Decorator name
    :return: None
    :raise Exception: With the decorator and argument that produced the error
    """
    raise Exception("The argument " + str(argument) +
                    " is mandatory in the " + str(decorator) + " decorator.")


def check_deprecated_arguments(deprecated_arguments, arguments, where):
    """
    This method looks for deprecated arguments and displays a warning
    if found.

    :param deprecated_arguments: Set of deprecated arguments
    :param arguments: List of arguments to check
    :param where: Location of the argument
    :return: None
    """
    for argument in arguments:
        if argument in deprecated_arguments:
            current_argument = re.sub('([A-Z]+)', r'_\1', argument).lower()
            message = "WARNING: Deprecated argument: " + str(argument) + \
                      " Found in " + str(where) + ".\n" + \
                      "         Please, use: " + current_argument

            # The print through stdout is disabled to prevent the message to
            # appear twice in the console. So the warning message will only
            # appear in STDERR.
            # print(message)                 # show the warn through stdout
            print(message, file=sys.stderr)  # also show the warn through stderr


def check_unexpected_arguments(supported_arguments, arguments, where):
    """
    This method looks for unexpected arguments and displays a warning
    if found.

    :param supported_arguments: Set of supported arguments
    :param arguments: List of arguments to check
    :param where: Location of the argument
    :return: None
    """
    for argument in arguments:
        if argument not in supported_arguments:
            message = "WARNING: Unexpected argument: " + str(argument) + \
                      " Found in " + str(where) + "."
            # The print through stdout is disabled to prevent the message to
            # appear twice in the console. So the warning message will only
            # appear in STDERR.
            # print(message)                 # show the warn through stdout
            print(message, file=sys.stderr)  # also show the warn through stderr
