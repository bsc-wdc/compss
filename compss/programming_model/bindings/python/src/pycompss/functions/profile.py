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

from pycompss.util.typing_helper import typing
from functools import wraps
from memory_profiler import profile as mem_profile


class Profile(object):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f: typing.Any) -> typing.Any:
        """Memory profiler decorator.

        :param f: Function to be profiled (can be a decorated function, usually
                  with @task decorator).
        :return: the decorator wrapper.
        """

        @wraps(f)
        def wrapped_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            """Memory profiler decorator.

            :param args: args
            :param kwargs: kwargs
            :return: a list with [the function result, The elapsed time]
            """
            result = mem_profile(f)(*args, **kwargs)
            return result

        return wrapped_f


# For lower-case usage:
profile = Profile
