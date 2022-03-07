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
PyCOMPSs API - COMMONS - ERROR MESSAGES
=======================================
    This file defines the public PyCOMPSs error messages displayed by the api.
"""


def not_in_pycompss(decorator_name):
    # type: (str) -> str
    """
    Retrieves the "not in PyCOMPSs scope" error message.

    :param decorator_name: Decorator name which requires the message.
    :return: Not in PyCOMPSs error message.
    """
    return "The %s decorator only works within PyCOMPSs framework." % decorator_name


def cast_env_to_int_error(what):
    # type: (str) -> str
    """
    Retrieves the "can not cast from environment variable to integer" error
    message.

    :param what: Environment variable name.
    :return: Can not cast from environment variable to integer.
    """
    return "ERROR: %s value cannot be cast from ENV variable to int" % what


def cast_string_to_int_error(what):
    # type: (str) -> str
    """
    Retrieves the "can not cast from string to integer" error message.

    :param what: Environment variable name.
    :return: Can not cast from string to integer.
    """
    return "ERROR: %s value cannot be cast from string to int" % what


def wrong_value(value_name, decorator_name):
    # type: (str, str) -> str
    """
    Retrieves the "wrong value at decorator" error message.

    :param value_name: Wrong value's name
    :param decorator_name: Decorator name which requires the message.
    :return: Wrong value at decorator message.
    """
    return "ERROR: Wrong %s value at %s decorator." % (value_name, decorator_name)
