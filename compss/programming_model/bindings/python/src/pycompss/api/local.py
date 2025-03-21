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
PyCOMPSs API - Local decorator.

This file contains the @local decorator, which is intended to be a decorator
for non-task functions that may receive future objects as parameters
(i.e: their inputs are pycompss task outputs). It also handles INOUTs
"""

import gc

from pycompss.util.context import CONTEXT
from pycompss.api.api import compss_wait_on
from pycompss.runtime.management.object_tracker import OT
from pycompss.util.objects.replace import replace
from pycompss.util.typing_helper import typing


def local(input_function: typing.Callable) -> typing.Callable:
    """Local decorator.

    :param input_function: Input function.
    :return: Wrapped function.
    """
    if not CONTEXT.in_pycompss():
        # Return dummy local decorator

        def wrapped_function(
            *args: typing.Any, **kwargs: typing.Any
        ) -> typing.Any:
            return input_function(*args, **kwargs)

    else:

        def sync_if_needed(obj: typing.Any) -> None:
            if OT.is_obj_pending_to_synchronize(obj):
                new_val = compss_wait_on(obj)
                replace(obj, new_val)

        def wrapped_function(
            *args: typing.Any, **kwargs: typing.Any
        ) -> typing.Any:
            gc.collect()
            _args = []
            _kwargs = {}
            for arg in args:
                sync_if_needed(arg)
                _args.append(arg)
            for key, value in kwargs.items():
                sync_if_needed(value)
                _kwargs[key] = value
            return input_function(*_args, **_kwargs)

    return wrapped_function


# ########################################################################### #
# #################### LOCAL DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

Local = local  # pylint: disable=invalid-name
