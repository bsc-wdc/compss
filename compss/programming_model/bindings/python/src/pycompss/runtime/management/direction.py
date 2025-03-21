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
PyCOMPSs Binding - Management - Direction.

This file contains the Direction management functions.
"""

from pycompss.api.parameter import DIRECTION


def get_compss_direction(access_mode: str) -> int:
    """Get the COMPSs direction of the given access_mode string.

    :param access_mode: String to parse and return the direction.
    :return: Direction object (IN/INOUT/OUT).
    """
    if access_mode.startswith("w"):
        return DIRECTION.OUT
    if access_mode.startswith("r+") or access_mode.startswith("a"):
        return DIRECTION.INOUT
    if access_mode.startswith("cv"):
        return DIRECTION.COMMUTATIVE
    if access_mode.startswith("c"):
        return DIRECTION.CONCURRENT
    return DIRECTION.IN
