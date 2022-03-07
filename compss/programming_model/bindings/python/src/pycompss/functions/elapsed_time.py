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
PyCOMPSs Functions: Elapsed time decorator
==========================================
    This file defines the time it decorator to be used over the task decorator.
"""

from pycompss.util.typing_helper import typing
import time
from functools import wraps


class TimeIt(object):
    def __init__(self, *args, **kwargs):
        # type: (*typing.Any, **typing.Any) -> None
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        # type: (typing.Any) -> typing.Any
        """Elapsed time decorator.

        :param f: Function to be time measured (can be a decorated function,
                  usually with @task decorator).
        :return: the decorator wrapper.
        """

        @wraps(f)
        def wrapped_f(*args, **kwargs):
            # type: (*typing.Any, **typing.Any) -> typing.Any
            """Elapsed time decorator.

            :param args: args
            :param kwargs: kwargs
            :return: a list with [the function result, The elapsed time]
            """
            ts = time.time()
            result = f(*args, **kwargs)
            te = time.time()
            return [result, (te - ts)]

        return wrapped_f


timeit = TimeIt
TimeIT = TimeIt
TIMEIT = TimeIt
