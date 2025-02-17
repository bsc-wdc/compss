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
PyCOMPSs API - dummy - decorator.

This file contains the dummy class task used as decorator.
"""

from pycompss.util.typing_helper import typing


class _Dummy:
    """Dummy task class (decorator style)."""

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Construct a dummy Task decorator.

        :param args: Task decorator arguments.
        :param kwargs: Task decorator keyword arguments.
        :returns: None
        """
        self.args = args
        self.kwargs = kwargs

    def __call__(self, function: typing.Any) -> typing.Any:
        """Invoke the dummy decorator.

        :param function: Decorated function.
        :returns: Result of executing the given function.
        """

        def wrapped_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            # returns may appear in @task decorator
            if "returns" in kwargs:
                kwargs.pop("returns")
            return function(*args, **kwargs)

        return wrapped_f

    def __repr__(self) -> str:
        attributes = f"(args: {repr(self.args)}, kwargs: {repr(self.kwargs)})"
        return f"Dummy {self.__class__.__name__} decorator {attributes}"
