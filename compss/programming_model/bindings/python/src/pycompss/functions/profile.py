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
PyCOMPSs Functions: Profiling decorator
=======================================
    This file defines the time it decorator to be used below the task decorator.
"""

import typing
from decorator import decorator  # noqa
from memory_profiler import profile as mem_profile


@decorator  # Mandatory in order to preserver the argspec
def profile(func, *a, **k):
    # type: (typing.Any, tuple, dict) -> typing.Any
    """ Memory profiler decorator.

    :param func: Function to be profiled (can be a decorated function, usually
                 with @task decorator).
    :param a: args
    :param k: kwargs
    :return: the function result.
    """
    result = mem_profile(func)(*a, **k)
    return result
