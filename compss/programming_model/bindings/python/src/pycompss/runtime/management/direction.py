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
PyCOMPSs Binding - Management - Direction
=========================================
    This file contains the Direction management functions.
"""

from pycompss.api.parameter import DIRECTION


def get_compss_direction(pymode):
    # type: (str) -> int
    """ Get the COMPSs direction of the given pymode string.

    :param pymode: String to parse and return the direction.
    :return: Direction object (IN/INOUT/OUT).
    """
    if pymode.startswith('w'):
        return DIRECTION.OUT
    elif pymode.startswith('r+') or pymode.startswith('a'):
        return DIRECTION.INOUT
    elif pymode.startswith('cv'):
        return DIRECTION.COMMUTATIVE
    elif pymode.startswith('c'):
        return DIRECTION.CONCURRENT
    else:
        return DIRECTION.IN
