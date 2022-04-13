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
PyCOMPSs API - dummy - constraint.

This file contains the dummy class constraint used as decorator.
"""

from pycompss.util.typing_helper import typing


class Constraint(object):
    """Dummy Constraint class (decorator style)."""

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Construct a dummy Constraint decorator.

        :param args: Task decorator arguments.
        :param kwargs: Task decorator keyword arguments.
        :returns: None
        """
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f: typing.Any) -> typing.Any:
        """Invoke the dummy Constraint decorator.

        :param f: Decorated function.
        :returns: Result of executing function f.
        """

        def wrapped_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return f(*args, **kwargs)

        return wrapped_f


constraint = Constraint
