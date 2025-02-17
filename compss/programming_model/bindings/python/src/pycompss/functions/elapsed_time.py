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
PyCOMPSs Functions: Elapsed time decorator.

This file defines the time it decorator to be used over the task decorator.
"""

import time
from functools import wraps

from pycompss.util.typing_helper import typing


class TimeIt:
    """TimeIT decorator class."""

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        self.args = args
        self.kwargs = kwargs

    def __call__(self, function: typing.Any) -> typing.Any:
        """Elapsed time decorator.

        :param f: Function to be time measured (can be a decorated function,
                  usually with @task decorator).
        :return: the decorator wrapper.
        """

        @wraps(function)
        def wrapped_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            """Elapsed time decorator body.

            :param args: args
            :param kwargs: kwargs
            :return: a list with [the function result, The elapsed time]
            """
            start_time = time.time()
            result = function(*args, **kwargs)
            end_time = time.time()
            return [result, (end_time - start_time)]

        return wrapped_f

    def __str__(self) -> str:
        """Represent the elapsed time decorator as string."""
        return "Elapsed time decorator object"


# ########################################################################### #
# ################### TimeIT DECORATOR ALTERNATIVE NAMES #################### #
# ########################################################################### #

timeit = TimeIt  # pylint: disable=invalid-name
TimeIT = TimeIt
TIMEIT = TimeIt
