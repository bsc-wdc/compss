#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Functions: Map
=======================
    This file defines the common map functions.
"""

from pycompss.runtime.commons import IS_PYTHON3


def map(*args):
    """
    Apply function to every item of iterable and return a list of the results.
    If additional iterable arguments are passed, function must take that many
    arguments and is applied to the items from all iterables in parallel.
    If one iterable is shorter than another it is assumed to be extended with
    None items. If function is None, the identity function is assumed; if
    there are multiple arguments, map() returns a list consisting of tuples
    containing the corresponding items from all iterables (a kind of transpose
    operation). The iterable arguments may be a sequence or any iterable
    object. The result is always a list.

    :param args: items to apply function
    :return: list result
    """

    try:
        if IS_PYTHON3:
            import builtins
            return list(builtins.map(*args))
        else:
            import __builtin__
            return __builtin__.map(*args)
    except Exception as e:
        raise e
