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
PyCOMPSs Util - Location.

This file defines the internal PyCOMPSs location functions.
"""

import inspect
import os


def _get_module_path() -> str:
    """Find the current file path when __file__ is not available.

    The '__file__' variable is not available if the code is compiled
    with mypy.
    :return: The current module path.
    """
    # Get the current call stack
    stack = inspect.stack()
    # Get the filename of the current file
    file_name = stack[0].filename
    # Get the directory path of the current file
    dir_name = os.path.dirname(file_name)
    return dir_name


def _get_current_path() -> str:
    """Get the current path.

    Checks if __file__ fails (with mypy) and retrieves it as an alternative
    way.
    :return: The current module path.
    """
    try:
        return os.path.dirname(os.path.realpath(__file__))
    except KeyError:
        # Using mypy __file__ does not exist.
        return _get_module_path()


def get_binding_location() -> str:
    """Get the binding main path.

    Removes the "util" folder from the last path:
    /path/to/pycompss/util -> /path/to/pycompss

    :return: The PyCOMPSs binding main path.
    """
    return os.path.dirname(_get_current_path())
