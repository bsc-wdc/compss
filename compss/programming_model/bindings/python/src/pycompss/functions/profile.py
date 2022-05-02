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
PyCOMPSs Functions: Profiling decorator.

This file defines the profiling decorator to be used below the task decorator.
"""

from functools import wraps

from memory_profiler import profile as mem_profile
from pycompss.util.typing_helper import typing


class Profile:
    """Profile decorator class."""

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        self.args = args
        self.kwargs = kwargs

    def __call__(self, function: typing.Any) -> typing.Any:
        """Memory profiler decorator.

        :param function: Function to be profiled (can be a decorated function, usually
                  with @task decorator).
        :return: the decorator wrapper.
        """

        @wraps(function)
        def wrapped_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            """Memory profiler decorator.

            :param args: args
            :param kwargs: kwargs
            :return: a list with [the function result, The elapsed time]
            """
            result = mem_profile(function)(*args, **kwargs)
            return result

        return wrapped_f

    def __str__(self):
        """Represent the profile decorator as string."""
        return "Profile decorator object"


# ########################################################################### #
# ################### Profile DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

profile = Profile  # pylint: disable=invalid-name
PROFILE = Profile
