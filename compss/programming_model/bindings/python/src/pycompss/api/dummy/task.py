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
PyCOMPSs Dummy API - task
=========================
    This file contains the dummy class task used as decorator.
"""

from pycompss.util.typing_helper import typing


class Task(object):
    """
    Dummy task class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        # type: (*typing.Any, **typing.Any) -> None
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        # type: (typing.Any) -> typing.Any
        def wrapped_f(*args, **kwargs):
            # type: (*typing.Any, **typing.Any) -> typing.Any
            if "returns" in kwargs:
                kwargs.pop("returns")
            return f(*args, **kwargs)

        return wrapped_f


task = Task
